package collector

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Collect is invoked by the Prometheus HTTP handler on each scrape.
// It intentionally never calls the Avi controller; all controller I/O happens
// in RefreshOnce, which is driven by the background scheduler.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()

	e.updateDynamicSelfMetricsLocked(time.Now())

	base := e.buildBaseLabels()
	if e.clusterCached {
		ch <- prometheus.MustNewConstMetric(e.clusterUp, prometheus.GaugeValue, e.clusterUpValue, base...)
		ch <- prometheus.MustNewConstMetric(e.clusterProgress, prometheus.GaugeValue, e.clusterProgressValue, base...)
	}

	for _, g := range e.allGaugeVecs() {
		g.Collect(ch)
	}

	e.up.Collect(ch)
	e.scrapeDuration.Collect(ch)
	e.scrapeErrorsTotal.Collect(ch)
	e.scrapeTotal.Collect(ch)
	e.moduleLastSuccess.Collect(ch)
	e.moduleLastAttempt.Collect(ch)
	e.moduleAge.Collect(ch)
	e.moduleStale.Collect(ch)
	e.moduleRefreshDuration.Collect(ch)
	e.moduleRefreshErrorsTotal.Collect(ch)
	e.moduleRefreshTotal.Collect(ch)
}

func resetGaugeVecs(gauges ...*prometheus.GaugeVec) {
	for _, g := range gauges {
		g.Reset()
	}
}

func deleteTenantGaugeVecs(tenant string, gauges ...*prometheus.GaugeVec) {
	match := prometheus.Labels{"tenant": tenant}
	for _, g := range gauges {
		g.DeletePartialMatch(match)
	}
}

func deleteTenantCounterVecs(tenant string, counters ...*prometheus.CounterVec) {
	match := prometheus.Labels{"tenant": tenant}
	for _, c := range counters {
		c.DeletePartialMatch(match)
	}
}
