package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

func (e *Exporter) collectCluster(ctx context.Context, ch chan<- prometheus.Metric) error {
	rt, err := e.client.GetClusterRuntime(ctx)
	if err != nil {
		e.logger.Error("get cluster runtime", "err", err)
		return err
	}

	clusterUp := 0.0
	if rt.ClusterState.State == "CLUSTER_UP_HA_ACTIVE" || rt.ClusterState.State == "CLUSTER_UP_NO_HA" {
		clusterUp = 1
	}
	base := e.buildBaseLabels()
	ch <- prometheus.MustNewConstMetric(e.clusterUp, prometheus.GaugeValue, clusterUp, base...)
	ch <- prometheus.MustNewConstMetric(e.clusterProgress, prometheus.GaugeValue, float64(rt.ClusterState.Progress), base...)

	e.emitInfo(e.clusterStateInfo, base, "state", rt.ClusterState.State)
	e.clusterStateInfo.Collect(ch)

	for _, n := range rt.NodeStates {
		labels := e.appendLabels(n.Name)
		up := 0.0
		if n.State == "CLUSTER_ACTIVE" {
			up = 1
		}
		e.clusterNodeUp.WithLabelValues(labels...).Set(up)

		leader := 0.0
		if n.Role == "CLUSTER_LEADER" {
			leader = 1
		}
		e.clusterNodeRole.WithLabelValues(labels...).Set(leader)
	}
	e.clusterNodeUp.Collect(ch)
	e.clusterNodeRole.Collect(ch)
	return nil
}
