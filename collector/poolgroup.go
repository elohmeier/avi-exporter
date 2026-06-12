package collector

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

// poolGroupLabelValues returns label values in pgLbl order:
// base..., tenant, poolgroup, poolgroup_uuid, namespace, service, ingress, host, ako.
func (e *Exporter) poolGroupLabelValues(tenant string, item avi.PoolGroupInventoryItem) []string {
	mi := avi.ParseMarkers(item.Config.Markers)
	ako := "false"
	if avi.IsAKOManaged(item.Config.CreatedBy) {
		ako = "true"
	}
	return e.appendLabels(tenant, item.Config.Name, item.Config.UUID,
		mi.Namespace, mi.ServiceName, mi.IngressName, mi.Host, ako)
}

func (e *Exporter) collectPoolGroupInventory(ctx context.Context, tenant string, items []avi.PoolGroupInventoryItem, ch chan<- prometheus.Metric) {
	for _, it := range items {
		labels := e.poolGroupLabelValues(tenant, it)
		e.poolGroupInfo.WithLabelValues(labels...).Set(1)
		e.poolGroupMemberCount.WithLabelValues(labels...).Set(float64(len(it.Config.Members)))
	}
}

func (e *Exporter) renderPoolGroupTopology(tenant string, items []avi.PoolGroupInventoryItem) {
	for _, it := range items {
		pgNodeID := "poolgroup:" + it.Config.UUID
		mi := avi.ParseMarkers(it.Config.Markers)
		chain := chainFor(mi, it.Config.Name)
		memberCount := strconv.Itoa(len(it.Config.Members))
		subtitle := "members: " + memberCount
		nodeLabels := e.appendLabels(tenant, pgNodeID, it.Config.Name, subtitle, "poolgroup", "UP", chain, memberCount, "", "blue")
		e.topologyNode.WithLabelValues(nodeLabels...).Set(1)

		statsLabels := e.appendLabels(tenant, pgNodeID, "poolgroup", chain)
		e.topologyNodeState.WithLabelValues(statsLabels...).Set(1)

		for _, m := range it.Config.Members {
			poolUUID := avi.RefUUID(m.PoolRef)
			if poolUUID == "" {
				continue
			}
			poolNodeID := "pool:" + poolUUID
			edgeID := pgNodeID + "->" + poolNodeID
			edgeLabels := e.appendLabels(tenant, edgeID, pgNodeID, poolNodeID, chain, "")
			e.topologyEdge.WithLabelValues(edgeLabels...).Set(1)
		}
	}
}
