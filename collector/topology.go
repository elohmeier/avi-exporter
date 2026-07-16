package collector

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

// collectTopology builds Grafana node-graph-friendly metrics for one tenant:
// vsvip → vs → (pool | poolgroup → pool) → poolmember nodes plus edges. The
// "chain" label captures AKO metadata when present (namespace/host) so
// dashboards can group by app. VsVip nodes appear as a shared parent of
// one-or-more VS nodes, surfacing shared-listener / SNI patterns.
//
// Pool member nodes/edges are emitted by the pool_members module
// (collector/pool.go) because they require an extra per-pool API call.
// Pool group nodes/edges are emitted by the pool_group module.
func (e *Exporter) collectTopology(tenant string, vsItems []avi.VSInventoryItem, poolItems []avi.PoolInventoryItem, vsvipItems []avi.VsVipInventoryItem, ch chan<- prometheus.Metric) {
	// Pool inventory carries authoritative reverse VS references, including
	// policy-selected pools whose VS config has no direct pool_ref. Keep the VS
	// chain available while walking those references and deduplicate repeated
	// discoveries of the same edge.
	vsChainByUUID := make(map[string]string, len(vsItems))
	vsNeedsReversePoolEdges := make(map[string]bool, len(vsItems))
	for _, vs := range vsItems {
		mi := avi.ParseObjectMetadata(vs.Config.Markers, vs.Config.ServiceMetadata)
		vsChainByUUID[vs.Config.UUID] = chainFor(mi, vs.Config.Name)
		vsNeedsReversePoolEdges[vs.Config.UUID] = avi.RefUUID(vs.Config.PoolRef) == "" && avi.RefUUID(vs.Config.PoolGroupRef) == ""
	}
	emittedVSPoolEdges := make(map[string]struct{})
	emitVSPoolEdge := func(vsUUID, poolUUID, chain string) {
		if vsUUID == "" || poolUUID == "" {
			return
		}
		source := "vs:" + vsUUID
		target := "pool:" + poolUUID
		edgeID := source + "->" + target
		if _, exists := emittedVSPoolEdges[edgeID]; exists {
			return
		}
		emittedVSPoolEdges[edgeID] = struct{}{}
		edgeLabels := e.appendLabels(tenant, edgeID, source, target, chain, "")
		e.topologyEdge.WithLabelValues(edgeLabels...).Set(1)
	}

	// VsVip nodes + vsvip→vs edges.
	for _, vv := range vsvipItems {
		nodeID := "vsvip:" + vv.Config.UUID
		mi := avi.ParseObjectMetadata(vv.Config.Markers, vv.Config.ServiceMetadata)
		chain := chainFor(mi, vv.Config.Name)

		// Aggregate state across all VIPs in this VsVip.
		anyUp, allUp := false, true
		for _, rt := range vv.Runtime {
			if rt.OperStatus.State == "OPER_UP" {
				anyUp = true
			} else {
				allUp = false
			}
		}
		state, value, color := "DOWN", 0.0, "red"
		switch {
		case anyUp && allUp:
			state, value, color = "UP", 1, "green"
		case anyUp:
			state, value, color = "PARTIAL", 0.5, "orange"
		}

		// subtitle: list the primary IPs.
		var ips []string
		for _, v := range vv.Config.Vip {
			if ip := vipPrimaryIP(v); ip != "" {
				ips = append(ips, ip)
			}
		}
		subtitle := "VIPs: " + strings.Join(ips, ",")
		mainstat := strconv.Itoa(len(vv.Config.Vip))
		secondary := strconv.Itoa(len(vv.Config.VirtualServices)) + " VS"

		nodeLabels := e.appendLabels(tenant, nodeID, vv.Config.Name, subtitle, "vsvip", state, chain, mainstat, secondary, color)
		e.topologyNode.WithLabelValues(nodeLabels...).Set(value)

		statsLabels := e.appendLabels(tenant, nodeID, "vsvip", chain)
		e.topologyNodeState.WithLabelValues(statsLabels...).Set(value)

		for _, vsRef := range vv.Config.VirtualServices {
			vsNodeID := "vs:" + vsRef.UUID
			edgeID := nodeID + "->" + vsNodeID
			edgeLabels := e.appendLabels(tenant, edgeID, nodeID, vsNodeID, chain, "")
			e.topologyEdge.WithLabelValues(edgeLabels...).Set(1)
		}
	}

	stateColor := func(s string) (string, float64, string) {
		if s == "OPER_UP" {
			return "UP", 1, "green"
		}
		if s == "OPER_PARTIAL_UP" {
			return "PARTIAL", 0.5, "orange"
		}
		return "DOWN", 0, "red"
	}

	// VS nodes.
	for _, vs := range vsItems {
		nodeID := "vs:" + vs.Config.UUID
		mi := avi.ParseObjectMetadata(vs.Config.Markers, vs.Config.ServiceMetadata)
		chain := chainFor(mi, vs.Config.Name)

		state, value, color := stateColor(vs.Runtime.OperStatus.State)
		health := vs.HealthScore.HealthScore
		subtitle := fmt.Sprintf("Health: %.0f%%, SEs UP: %d%%", health, vs.Runtime.PercentSesUp)
		mainstat := fmt.Sprintf("%.0f", health)
		secondary := strconv.Itoa(vs.Runtime.PercentSesUp) + "%"

		nodeLabels := e.appendLabels(tenant, nodeID, vs.Config.Name, subtitle, "virtualservice", state, chain, mainstat, secondary, color)
		e.topologyNode.WithLabelValues(nodeLabels...).Set(value)

		statsLabels := e.appendLabels(tenant, nodeID, "virtualservice", chain)
		e.topologyNodeState.WithLabelValues(statsLabels...).Set(value)
		e.topologyNodeHealth.WithLabelValues(statsLabels...).Set(health)

		// VS → pool edge (direct).
		if poolUUID := avi.RefUUID(vs.Config.PoolRef); poolUUID != "" {
			emitVSPoolEdge(vs.Config.UUID, poolUUID, chain)
		}
		// VS → pool_group edge (the fan-out hole — pool group then contains N pools).
		if pgUUID := avi.RefUUID(vs.Config.PoolGroupRef); pgUUID != "" {
			pgNodeID := "poolgroup:" + pgUUID
			edgeID := nodeID + "->" + pgNodeID
			edgeLabels := e.appendLabels(tenant, edgeID, nodeID, pgNodeID, chain, "")
			e.topologyEdge.WithLabelValues(edgeLabels...).Set(1)
		}
	}

	// Pool nodes + pool → member edges.
	for _, p := range poolItems {
		nodeID := "pool:" + p.Config.UUID
		mi := avi.ParseObjectMetadata(p.Config.Markers, p.Config.ServiceMetadata)
		chain := chainFor(mi, p.Config.Name)

		state, value, color := stateColor(p.Runtime.OperStatus.State)
		health := p.HealthScore.HealthScore
		subtitle := fmt.Sprintf("Health: %.0f%%, Servers: %d/%d", health, p.Runtime.NumServersUp, p.Runtime.NumServers)
		mainstat := fmt.Sprintf("%d/%d", p.Runtime.NumServersUp, p.Runtime.NumServers)
		secondary := fmt.Sprintf("%.0f", health)

		nodeLabels := e.appendLabels(tenant, nodeID, p.Config.Name, subtitle, "pool", state, chain, mainstat, secondary, color)
		e.topologyNode.WithLabelValues(nodeLabels...).Set(value)

		statsLabels := e.appendLabels(tenant, nodeID, "pool", chain)
		e.topologyNodeState.WithLabelValues(statsLabels...).Set(value)
		e.topologyNodeHealth.WithLabelValues(statsLabels...).Set(health)

		// Pool inventory exposes reverse VS references for both direct and
		// policy-selected delivery paths. The latter cannot be reconstructed
		// from VS inventory alone because it omits l4_policies and the exporter
		// account may not be allowed to read L4 Policy Set configuration.
		for _, vsRef := range p.VirtualServices {
			vsUUID := vsRef.UUID
			if vsUUID == "" {
				vsUUID = avi.RefUUID(vsRef.Ref)
			}
			vsChain, exists := vsChainByUUID[vsUUID]
			if !exists {
				continue
			}
			if !vsNeedsReversePoolEdges[vsUUID] {
				continue
			}
			emitVSPoolEdge(vsUUID, p.Config.UUID, vsChain)
		}
		// Pool member topology nodes/edges are emitted by the pool_members module
		// (collector/pool.go:collectPoolMembers), which sources per-server runtime
		// from /api/pool/<uuid>/runtime/server/detail/ — the bulk pool-inventory
		// endpoint doesn't include per-server state.
	}
}

// chainFor returns a stable identifier grouping related nodes for dashboards.
// AKO objects use "ns/service" or host; non-AKO falls back to the object name.
func chainFor(mi avi.MarkerInfo, name string) string {
	if mi.Namespace != "" && mi.ServiceName != "" {
		return mi.Namespace + "/" + mi.ServiceName
	}
	if mi.Namespace != "" && mi.IngressName != "" {
		return mi.Namespace + "/" + mi.IngressName
	}
	if mi.Host != "" {
		return mi.Host
	}
	return name
}
