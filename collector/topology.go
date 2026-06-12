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
	// Map pool UUID → pool item for fast lookup when we walk VS→pool edges.
	poolByUUID := make(map[string]avi.PoolInventoryItem, len(poolItems))
	for _, p := range poolItems {
		poolByUUID[p.Config.UUID] = p
	}

	// VsVip nodes + vsvip→vs edges.
	for _, vv := range vsvipItems {
		nodeID := "vsvip:" + vv.Config.UUID
		mi := avi.ParseMarkers(vv.Config.Markers)
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
			poolNodeID := "pool:" + poolUUID
			edgeID := nodeID + "->" + poolNodeID
			edgeLabels := e.appendLabels(tenant, edgeID, nodeID, poolNodeID, chain, "")
			e.topologyEdge.WithLabelValues(edgeLabels...).Set(1)
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
		// Pool member topology nodes/edges are emitted by the pool_members module
		// (collector/pool.go:collectPoolMembers), which sources per-server runtime
		// from /api/pool/<uuid>/runtime/server/detail/ — the bulk pool-inventory
		// endpoint doesn't include per-server state.
	}

	_ = poolByUUID // reserved for future cross-checks
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
