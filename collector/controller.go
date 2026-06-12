package collector

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

var controllerMetricIDs = []string{
	"controller_stats.avg_cpu_usage",
	"controller_stats.avg_disk_read_bytes",
	"controller_stats.avg_disk_usage",
	"controller_stats.avg_disk_write_bytes",
	"controller_stats.avg_mem_usage",
	"controller_stats.avg_num_active_vs",
	"controller_stats.avg_num_backend_servers",
}

func (e *Exporter) collectControllerAnalytics(ctx context.Context) error {
	queries := make([]avi.MetricQuery, 0, len(controllerMetricIDs))
	for _, id := range controllerMetricIDs {
		queries = append(queries, avi.MetricQuery{
			EntityUUID:   "*",
			MetricEntity: avi.EntityController,
			MetricID:     id,
			Step:         e.cfg.MetricsStep,
			Limit:        e.cfg.MetricsLimit,
		})
	}

	resp, err := e.client.CollectMetrics(ctx, "admin", avi.MetricsCollectionRequest{MetricRequests: queries})
	if err != nil {
		e.logger.Error("collect controller metrics", "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	resetGaugeVecs(e.controllerMetricsGaugeVecs()...)

	for key, series := range resp.Series {
		for _, s := range series {
			v, ok := s.Last()
			if !ok {
				continue
			}
			if g := e.controllerGaugeFor(s.Header.Name); g != nil {
				controllerUUID := s.Header.EntityUUID
				if controllerUUID == "" {
					controllerUUID = key
				}
				g.WithLabelValues(e.appendLabels(controllerUUID)...).Set(v)
			}
		}
	}
	return nil
}

func (e *Exporter) controllerGaugeFor(metricID string) *prometheus.GaugeVec {
	switch metricID {
	case "controller_stats.avg_cpu_usage":
		return e.controllerAvgCPUUsage
	case "controller_stats.avg_mem_usage":
		return e.controllerAvgMemUsage
	case "controller_stats.avg_disk_usage":
		return e.controllerAvgDiskUsage
	case "controller_stats.avg_disk_read_bytes":
		return e.controllerAvgDiskReadBytes
	case "controller_stats.avg_disk_write_bytes":
		return e.controllerAvgDiskWriteBytes
	case "controller_stats.avg_num_active_vs":
		return e.controllerAvgNumActiveVS
	case "controller_stats.avg_num_backend_servers":
		return e.controllerAvgNumBackendServers
	}
	return nil
}
