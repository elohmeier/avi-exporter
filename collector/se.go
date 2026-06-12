package collector

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/elohmeier/avi-exporter/avi"
)

// seLabelValues returns label values in seLbl order: base..., se, se_uuid.
func (e *Exporter) seLabelValues(item avi.SEInventoryItem) []string {
	return e.appendLabels(item.Config.Name, item.Config.UUID)
}

func (e *Exporter) collectSEInventory(ctx context.Context, items []avi.SEInventoryItem, ch chan<- prometheus.Metric) {
	for _, it := range items {
		labels := e.seLabelValues(it)

		up := 0.0
		if it.Runtime.OperStatus.State == "OPER_UP" {
			up = 1
		}
		e.seOperUp.WithLabelValues(labels...).Set(up)
		e.emitOperStatusInfo(e.seOperStatusInfo, labels, it.Runtime.OperStatus.State)

		// "Enabled" derived from EnableState: anything that's not SE_STATE_ENABLED
		// counts as disabled/draining.
		enabled := 0.0
		if it.Config.EnableState == "SE_STATE_ENABLED" || it.Config.EnableState == "" {
			enabled = 1
		}
		e.seEnabled.WithLabelValues(labels...).Set(enabled)
		e.seHealthScore.WithLabelValues(labels...).Set(it.HealthScore.HealthScore)

		// Runtime booleans.
		e.seConnected.WithLabelValues(labels...).Set(boolToFloat(it.Runtime.SeConnected))
		e.seBgpPeersUp.WithLabelValues(labels...).Set(boolToFloat(it.Runtime.BgpPeersUp))
		e.seGatewayUp.WithLabelValues(labels...).Set(boolToFloat(it.Runtime.GatewayUp))
		e.seAtCurrVer.WithLabelValues(labels...).Set(boolToFloat(it.Runtime.AtCurrVer))
		e.seSufficientMem.WithLabelValues(labels...).Set(boolToFloat(it.Runtime.SufficientMem))
		e.seLicensedCores.WithLabelValues(labels...).Set(it.Runtime.LicensedCores)

		// Enum/string runtime fields as info-metrics.
		e.emitInfo(e.seLicenseState, labels, "license_state", it.Runtime.LicenseState)
		e.emitInfo(e.sePowerState, labels, "power_state", it.Runtime.PowerState)
		e.emitInfo(e.seMigrateState, labels, "migrate_state", it.Runtime.MigrateState)
		e.emitInfo(e.seVersionInfo, labels, "version", it.Runtime.Version)
		e.emitInfo(e.seEnableStateInfo, labels, "enable_state", it.Config.EnableState)
	}
}

var seMetricIDs = []string{
	"se_if.avg_bandwidth",
	"se_if.avg_connection_table_usage",
	"se_if.avg_rx_bytes",
	"se_if.avg_rx_pkts",
	"se_if.avg_rx_pkts_dropped_non_vs",
	"se_if.avg_tx_bytes",
	"se_if.avg_tx_pkts",
	"se_stats.avg_connection_mem_usage",
	"se_stats.avg_connections",
	"se_stats.avg_connections_dropped",
	"se_stats.avg_cpu_usage",
	"se_stats.avg_disk1_usage",
	"se_stats.avg_dynamic_mem_usage",
	"se_stats.avg_mem_usage",
	"se_stats.avg_packet_buffer_header_usage",
	"se_stats.avg_packet_buffer_large_usage",
	"se_stats.avg_packet_buffer_small_usage",
	"se_stats.avg_packet_buffer_usage",
	"se_stats.avg_persistent_table_usage",
	"se_stats.avg_rx_bandwidth",
	"se_stats.avg_ssl_session_cache_usage",
	"se_stats.max_se_bandwidth",
	"se_stats.pct_syn_cache_usage",
	"healthscore.health_score_value",
}

func (e *Exporter) collectSEAnalytics(ctx context.Context, items []avi.SEInventoryItem, ch chan<- prometheus.Metric) error {
	_ = items
	_ = ch

	families, err := e.collectBuiltinPrometheusMetrics(ctx, "serviceengine", e.seMetricTenants(), seMetricIDs)
	if err != nil {
		e.logger.Error("collect SE metrics", "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	resetGaugeVecs(e.seAnalyticsGaugeVecs()...)

	for familyName, family := range families {
		g := e.seGaugeFor(familyName)
		if g == nil {
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
			g.WithLabelValues(e.appendLabels(name, uuid)...).Set(value)
		}
	}

	return nil
}

func (e *Exporter) collectBuiltinPrometheusMetrics(ctx context.Context, resource string, tenants []string, metricIDs []string) (map[string]*dto.MetricFamily, error) {
	merged := make(map[string]*dto.MetricFamily)
	for _, batch := range builtinPrometheusMetricBatches(resource, metricIDs) {
		families, err := e.collectBuiltinPrometheusMetricBatch(ctx, resource, tenants, batch)
		if err != nil {
			return nil, fmt.Errorf("%s metrics %s: %w", resource, strings.Join(batch, ","), err)
		}
		mergeMetricFamilies(merged, families)
	}
	return merged, nil
}

func (e *Exporter) collectBuiltinPrometheusMetricBatch(ctx context.Context, resource string, tenants []string, metricIDs []string) (map[string]*dto.MetricFamily, error) {
	query := url.Values{}
	query.Set("tenant", strings.Join(tenants, ","))
	query.Set("metric_id", strings.Join(metricIDs, ","))

	raw, err := e.client.GetRaw(ctx, "/api/analytics/prometheus-metrics/"+resource, avi.RequestOptions{Query: query})
	if err != nil {
		return nil, err
	}

	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse %s prometheus metrics: %w", resource, err)
	}
	return families, nil
}

func builtinPrometheusMetricBatches(resource string, metricIDs []string) [][]string {
	if resource == "serviceengine" {
		out := make([][]string, 0, len(metricIDs))
		for _, id := range metricIDs {
			out = append(out, []string{id})
		}
		return out
	}
	return groupedMetricIDs(metricIDs)
}

func groupedMetricIDs(metricIDs []string) [][]string {
	out := make([][]string, 0)
	indexByGroup := make(map[string]int)
	for _, id := range metricIDs {
		group := metricIDGroup(id)
		idx, ok := indexByGroup[group]
		if !ok {
			idx = len(out)
			indexByGroup[group] = idx
			out = append(out, nil)
		}
		out[idx] = append(out[idx], id)
	}
	return out
}

func metricIDGroup(metricID string) string {
	if idx := strings.IndexByte(metricID, '.'); idx > 0 {
		return metricID[:idx]
	}
	return metricID
}

func mergeMetricFamilies(dst, src map[string]*dto.MetricFamily) {
	for name, family := range src {
		if existing := dst[name]; existing != nil {
			existing.Metric = append(existing.Metric, family.GetMetric()...)
			continue
		}
		dst[name] = family
	}
}

func (e *Exporter) seMetricTenants() []string {
	e.cacheMu.Lock()
	tenants := append([]string{}, e.tenants...)
	e.cacheMu.Unlock()
	if len(tenants) == 0 {
		static, _ := e.configuredTenants()
		tenants = static
	}

	out := make([]string, 0, len(tenants)+1)
	seen := make(map[string]bool, len(tenants)+1)
	for _, tenant := range append([]string{"admin"}, tenants...) {
		tenant = strings.TrimSpace(tenant)
		if tenant == "" || seen[tenant] {
			continue
		}
		seen[tenant] = true
		out = append(out, tenant)
	}
	if len(out) == 0 {
		return []string{"admin"}
	}
	return out
}

func prometheusLabelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

func prometheusMetricValue(metric *dto.Metric) (float64, bool) {
	if metric.GetGauge() != nil {
		return metric.GetGauge().GetValue(), true
	}
	if metric.GetUntyped() != nil {
		return metric.GetUntyped().GetValue(), true
	}
	if metric.GetCounter() != nil {
		return metric.GetCounter().GetValue(), true
	}
	return 0, false
}

func (e *Exporter) seGaugeFor(metricID string) *prometheus.GaugeVec {
	return e.seAnalyticsGauges[metricID]
}
