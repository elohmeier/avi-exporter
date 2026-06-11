package collector

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

// vsLabelValues returns the label values for a VS, in the same order as vsLbl
// in exporter.go: base..., tenant, vs, vs_uuid, namespace, service, ingress, host, ako.
func (e *Exporter) vsLabelValues(tenant string, item avi.VSInventoryItem) []string {
	mi := avi.ParseMarkers(item.Config.Markers)
	ako := "false"
	if avi.IsAKOManaged(item.Config.CreatedBy) {
		ako = "true"
	}
	return e.appendLabels(tenant, item.Config.Name, item.Config.UUID,
		mi.Namespace, mi.ServiceName, mi.IngressName, mi.Host, ako)
}

func (e *Exporter) collectVSInventory(ctx context.Context, tenant string, items []avi.VSInventoryItem, ch chan<- prometheus.Metric) {
	for _, it := range items {
		labels := e.vsLabelValues(tenant, it)

		up := 0.0
		if it.Runtime.OperStatus.State == "OPER_UP" {
			up = 1
		}
		e.vsOperUp.WithLabelValues(labels...).Set(up)
		e.emitOperStatusInfo(e.vsOperStatusInfo, labels, it.Runtime.OperStatus.State)

		enabled := 0.0
		if it.Config.Enabled != nil && *it.Config.Enabled {
			enabled = 1
		}
		e.vsEnabled.WithLabelValues(labels...).Set(enabled)

		e.vsHealthScore.WithLabelValues(labels...).Set(it.HealthScore.HealthScore)
		e.vsPercentSesUp.WithLabelValues(labels...).Set(float64(it.Runtime.PercentSesUp))

		// VS type + alert info-metrics
		e.emitInfo(e.vsTypeInfo, labels, "type", it.Config.Type)
		e.emitAlertLevel(e.vsAlertLevel, labels, it.Alert.Level)

		// Per-VIP runtime from VS inventory's vip_summary[]. Same shape as
		// /api/vsvip-inventory runtime, but joined to the VS context — useful
		// even when the vsvip module is disabled or unsupported (pre-22.1).
		// Match by vip_id against config.vip[] to surface the IP (lets users
		// join these series to vip_* by ip).
		ipByVipID := make(map[string]string, len(it.Config.VIP))
		for _, v := range it.Config.VIP {
			if v.IPAddress != nil {
				ipByVipID[v.VipID] = v.IPAddress.Addr
			} else if v.IP6Address != nil {
				ipByVipID[v.VipID] = v.IP6Address.Addr
			}
		}
		for _, vrs := range it.Runtime.VipSummary {
			vipLbl := append(append([]string{}, labels...), vrs.VipID, ipByVipID[vrs.VipID])

			vipUp := 0.0
			if vrs.OperStatus.State == "OPER_UP" {
				vipUp = 1
			}
			e.vsVipOperUp.WithLabelValues(vipLbl...).Set(vipUp)
			e.emitOperStatusInfo(e.vsVipOperStatusInfo, vipLbl, vrs.OperStatus.State)
			e.vsVipPercentSesUp.WithLabelValues(vipLbl...).Set(float64(vrs.PercentSesUp))
			e.vsVipNumSeAssigned.WithLabelValues(vipLbl...).Set(float64(vrs.NumSeAssigned))
			e.vsVipNumSeRequested.WithLabelValues(vipLbl...).Set(float64(vrs.NumSeRequested))
		}
	}
}

// vsMetricIDs is the curated list pushed in the analytics POST per VS.
var vsMetricIDs = []string{
	"l4_client.avg_bandwidth",
	"l4_client.avg_complete_conns",
	"l4_client.avg_new_established_conns",
	"l4_client.max_open_conns",
	"l4_client.avg_connections_dropped",
	"l7_client.avg_total_requests",
	"l7_client.avg_complete_responses",
	"l7_client.avg_error_responses",
	"l7_client.avg_resp_2xx",
	"l7_client.avg_resp_4xx",
	"l7_client.avg_resp_5xx",
	"l7_client.apdexr",
	"l7_client.avg_client_rtt",
	"l7_server.avg_resp_latency",
}

func (e *Exporter) buildVSMetricQueries() []avi.MetricQuery {
	out := make([]avi.MetricQuery, 0, len(vsMetricIDs))
	for _, id := range vsMetricIDs {
		out = append(out, avi.MetricQuery{
			EntityUUID:   "*",
			MetricEntity: avi.EntityVS,
			MetricID:     id,
			Step:         e.cfg.MetricsStep,
			Limit:        e.cfg.MetricsLimit,
		})
	}
	return out
}

func (e *Exporter) collectVSAnalytics(ctx context.Context, tenant string, items []avi.VSInventoryItem, ch chan<- prometheus.Metric) error {
	req := avi.MetricsCollectionRequest{MetricRequests: e.buildVSMetricQueries()}
	resp, err := e.client.CollectMetrics(ctx, tenant, req)
	if err != nil {
		e.logger.Error("collect VS metrics", "tenant", tenant, "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	// Build a UUID → inventory lookup so we can attach the same labels analytics emits.
	byUUID := make(map[string]avi.VSInventoryItem, len(items))
	for _, it := range items {
		byUUID[it.Config.UUID] = it
	}

	for uuid, series := range resp.Series {
		it, ok := byUUID[uuid]
		if !ok {
			continue // metric for an entity we didn't inventory (e.g. deleted mid-scrape)
		}
		labels := e.vsLabelValues(tenant, it)
		for _, s := range series {
			v, ok := s.Last()
			if !ok {
				continue
			}
			gauge := e.vsGaugeFor(s.Header.Name)
			if gauge != nil {
				gauge.WithLabelValues(labels...).Set(v)
			}
		}
	}

	return nil
}

func (e *Exporter) vsGaugeFor(metricID string) *prometheus.GaugeVec {
	switch metricID {
	case "l4_client.avg_bandwidth":
		return e.vsAvgBandwidth
	case "l4_client.avg_complete_conns":
		return e.vsAvgCompleteConns
	case "l4_client.avg_new_established_conns":
		return e.vsAvgNewEstabConns
	case "l4_client.max_open_conns":
		return e.vsMaxOpenConns
	case "l4_client.avg_connections_dropped":
		return e.vsConnectionsDropped
	case "l7_client.avg_total_requests":
		return e.vsAvgTotalRequests
	case "l7_client.avg_complete_responses":
		return e.vsAvgCompleteResp
	case "l7_client.avg_error_responses":
		return e.vsAvgErrorResp
	case "l7_client.avg_resp_2xx":
		return e.vsAvgResp2xx
	case "l7_client.avg_resp_4xx":
		return e.vsAvgResp4xx
	case "l7_client.avg_resp_5xx":
		return e.vsAvgResp5xx
	case "l7_client.apdexr":
		return e.vsApdexR
	case "l7_client.avg_client_rtt":
		return e.vsAvgClientRTT
	case "l7_server.avg_resp_latency":
		return e.vsAvgRespLatency
	}
	return nil
}
