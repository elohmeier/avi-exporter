package collector

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

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
	"dns_client.avg_avi_errors",
	"dns_client.avg_complete_queries",
	"dns_client.avg_domain_lookup_failures",
	"dns_client.avg_tcp_queries",
	"dns_client.avg_udp_passthrough_resp_time",
	"dns_client.avg_udp_queries",
	"dns_client.avg_unsupported_queries",
	"dns_client.pct_errored_queries",
	"dns_server.avg_complete_queries",
	"dns_server.avg_errored_queries",
	"dns_server.avg_tcp_queries",
	"dns_server.avg_udp_queries",
	"l4_client.apdexc",
	"l4_client.avg_application_dos_attacks",
	"l4_client.avg_bandwidth",
	"l4_client.avg_complete_conns",
	"l4_client.avg_connections_dropped",
	"l4_client.avg_lossy_connections",
	"l4_client.avg_new_established_conns",
	"l4_client.avg_policy_drops",
	"l4_client.avg_rx_bytes",
	"l4_client.avg_rx_pkts",
	"l4_client.avg_total_rtt",
	"l4_client.avg_tx_bytes",
	"l4_client.avg_tx_pkts",
	"l4_client.max_open_conns",
	"l4_server.apdexc",
	"l4_server.avg_bandwidth",
	"l4_server.avg_errored_connections",
	"l4_server.avg_new_established_conns",
	"l4_server.avg_open_conns",
	"l4_server.avg_pool_complete_conns",
	"l4_server.avg_pool_open_conns",
	"l4_server.avg_rx_bytes",
	"l4_server.avg_rx_pkts",
	"l4_server.avg_total_rtt",
	"l4_server.avg_tx_bytes",
	"l4_server.avg_tx_pkts",
	"l4_server.max_open_conns",
	"l7_client.apdexr",
	"l7_client.avg_client_data_transfer_time",
	"l7_client.avg_client_rtt",
	"l7_client.avg_client_txn_latency",
	"l7_client.avg_complete_responses",
	"l7_client.avg_error_responses",
	"l7_client.avg_frustrated_responses",
	"l7_client.avg_http_headers_bytes",
	"l7_client.avg_http_headers_count",
	"l7_client.avg_http_params_count",
	"l7_client.avg_page_load_time",
	"l7_client.avg_post_bytes",
	"l7_client.avg_resp_2xx",
	"l7_client.avg_resp_4xx",
	"l7_client.avg_resp_4xx_avi_errors",
	"l7_client.avg_resp_5xx",
	"l7_client.avg_resp_5xx_avi_errors",
	"l7_client.avg_ssl_connections",
	"l7_client.avg_ssl_handshakes_new",
	"l7_client.avg_total_requests",
	"l7_client.avg_uri_length",
	"l7_client.avg_waf_attacks",
	"l7_client.avg_waf_disabled",
	"l7_client.avg_waf_evaluated",
	"l7_client.avg_waf_matched",
	"l7_client.avg_waf_rejected",
	"l7_client.pct_get_reqs",
	"l7_client.pct_post_reqs",
	"l7_client.pct_waf_attacks",
	"l7_client.pct_waf_disabled",
	"l7_client.sum_application_response_time",
	"l7_client.sum_get_reqs",
	"l7_client.sum_other_reqs",
	"l7_client.sum_post_reqs",
	"l7_client.sum_total_responses",
	"l7_server.apdexr",
	"l7_server.avg_application_response_time",
	"l7_server.avg_complete_responses",
	"l7_server.avg_frustrated_responses",
	"l7_server.avg_resp_latency",
	"l7_server.avg_total_requests",
	"l7_server.pct_response_errors",
	"healthscore.health_score_value",
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
	families, err := e.collectBuiltinPrometheusMetrics(ctx, "virtualservice", []string{tenant}, vsMetricIDs)
	if err != nil {
		e.logger.Error("collect VS metrics", "tenant", tenant, "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	// Build a UUID → inventory lookup so we can attach the same labels analytics emits.
	byUUID := make(map[string]avi.VSInventoryItem, len(items))
	for _, it := range items {
		byUUID[it.Config.UUID] = it
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	deleteTenantGaugeVecs(tenant, e.vsAnalyticsGaugeVecs()...)

	for familyName, family := range families {
		gauge := e.vsGaugeFor(familyName)
		if gauge == nil {
			continue
		}
		for _, metric := range family.GetMetric() {
			uuid := prometheusLabelValue(metric, "uuid")
			it, ok := byUUID[uuid]
			if !ok {
				continue // metric for an entity we didn't inventory (e.g. deleted mid-scrape)
			}
			value, ok := prometheusMetricValue(metric)
			if !ok {
				continue
			}
			gauge.WithLabelValues(e.vsLabelValues(tenant, it)...).Set(value)
		}
	}

	return nil
}

func (e *Exporter) collectRawVSAnalytics(ctx context.Context, tenants []string) error {
	families, err := e.collectBuiltinPrometheusMetrics(ctx, "virtualservice", tenants, vsMetricIDs)
	if err != nil {
		e.logger.Error("collect raw VS metrics", "tenants", tenants, "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	for _, tenant := range tenants {
		deleteTenantGaugeVecs(tenant, e.vsAnalyticsGaugeVecs()...)
	}
	fallbackTenant := ""
	if len(tenants) == 1 {
		fallbackTenant = tenants[0]
	}
	e.renderRawVSAnalyticsLocked(families, fallbackTenant)
	return nil
}

func (e *Exporter) renderRawVSAnalyticsLocked(families map[string]*dto.MetricFamily, fallbackTenant string) {
	for familyName, family := range families {
		gauge := e.vsGaugeFor(familyName)
		if gauge == nil {
			continue
		}
		for _, metric := range family.GetMetric() {
			value, ok := prometheusMetricValue(metric)
			if !ok {
				continue
			}
			uuid := prometheusLabelValue(metric, "uuid")
			name := prometheusLabelValue(metric, "name")
			if uuid == "" && name == "" {
				continue
			}
			if name == "" {
				name = uuid
			}
			tenant := prometheusLabelValue(metric, "tenant")
			if tenant == "" {
				tenant = fallbackTenant
			}
			gauge.WithLabelValues(e.rawVSAnalyticsLabelValues(
				tenant,
				name,
				uuid,
			)...).Set(value)
		}
	}
}

func (e *Exporter) rawVSAnalyticsLabelValues(tenant, name, uuid string) []string {
	return e.appendLabels(tenant, name, uuid, "", "", "", "", "false")
}

func (e *Exporter) vsGaugeFor(metricID string) *prometheus.GaugeVec {
	return e.vsAnalyticsGauges[metricID]
}
