package collector

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

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
	"se_stats.avg_cpu_usage",
	"se_stats.avg_mem_usage",
	"se_stats.avg_disk1_usage",
	"se_stats.avg_connections",
	"se_stats.avg_connections_dropped",
	"se_stats.avg_connection_mem_usage",
	"se_stats.pct_connections_dropped",
	"se_stats.avg_packet_buffer_usage",
	"se_stats.avg_persistent_table_usage",
	"se_stats.avg_ssl_session_cache_usage",
	"se_if.avg_rx_bytes",
	"se_if.avg_tx_bytes",
	"se_if.avg_bandwidth",
}

func (e *Exporter) collectSEAnalytics(ctx context.Context, items []avi.SEInventoryItem, ch chan<- prometheus.Metric) error {
	queries := make([]avi.MetricQuery, 0, len(seMetricIDs))
	for _, id := range seMetricIDs {
		queries = append(queries, avi.MetricQuery{
			EntityUUID:   "*",
			MetricEntity: avi.EntitySE,
			MetricID:     id,
			Step:         e.cfg.MetricsStep,
			Limit:        e.cfg.MetricsLimit,
		})
	}

	resp, err := e.client.CollectMetrics(ctx, "admin", avi.MetricsCollectionRequest{MetricRequests: queries})
	if err != nil {
		e.logger.Error("collect SE metrics", "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	byUUID := make(map[string]avi.SEInventoryItem, len(items))
	for _, it := range items {
		byUUID[it.Config.UUID] = it
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	resetGaugeVecs(e.seAnalyticsGaugeVecs()...)

	for uuid, series := range resp.Series {
		it, ok := byUUID[uuid]
		if !ok {
			continue
		}
		labels := e.seLabelValues(it)
		for _, s := range series {
			v, ok := s.Last()
			if !ok {
				continue
			}
			if g := e.seGaugeFor(s.Header.Name); g != nil {
				g.WithLabelValues(labels...).Set(v)
			}
		}
	}

	return nil
}

func (e *Exporter) seGaugeFor(metricID string) *prometheus.GaugeVec {
	switch metricID {
	case "se_stats.avg_cpu_usage":
		return e.seAvgCPUUsage
	case "se_stats.avg_mem_usage":
		return e.seAvgMemUsage
	case "se_stats.avg_disk1_usage":
		return e.seAvgDiskUsage
	case "se_stats.avg_connections":
		return e.seAvgConnections
	case "se_stats.avg_connections_dropped":
		return e.seAvgConnDropped
	case "se_if.avg_rx_bytes":
		return e.seAvgRxBytes
	case "se_if.avg_tx_bytes":
		return e.seAvgTxBytes
	case "se_if.avg_bandwidth":
		return e.seAvgBandwidth
	case "se_stats.avg_connection_mem_usage":
		return e.seAvgConnMem
	case "se_stats.pct_connections_dropped":
		return e.sePctConnDropped
	case "se_stats.avg_packet_buffer_usage":
		return e.sePktBufUsage
	case "se_stats.avg_persistent_table_usage":
		return e.sePersistTblUsage
	case "se_stats.avg_ssl_session_cache_usage":
		return e.seSslSessCache
	}
	return nil
}
