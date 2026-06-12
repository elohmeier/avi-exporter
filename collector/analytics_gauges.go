package collector

import (
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

func buildAnalyticsGaugeMap(newGauge func(string, string, []string) *prometheus.GaugeVec, prefix string, labels []string, metricIDs []string, existing map[string]*prometheus.GaugeVec) map[string]*prometheus.GaugeVec {
	out := make(map[string]*prometheus.GaugeVec, len(metricIDs)*2)
	for _, id := range metricIDs {
		gv := existing[id]
		if gv == nil {
			gv = newGauge(analyticsGaugeName(prefix, id), analyticsGaugeHelp(prefix, id), labels)
		}
		out[id] = gv
		out[analyticsFamilyName(id)] = gv
	}
	return out
}

func analyticsGaugeName(prefix, metricID string) string {
	name := strings.NewReplacer(".", "_", "-", "_").Replace(metricID)
	if prefix == "" {
		return name
	}
	return prefix + "_" + name
}

func analyticsFamilyName(metricID string) string {
	return "avi_" + strings.NewReplacer(".", "_", "-", "_").Replace(metricID)
}

func analyticsGaugeHelp(prefix, metricID string) string {
	if prefix == "" {
		return "analytics metric " + metricID
	}
	return prefix + " analytics metric " + metricID
}

func sortedUniqueGaugeVecs(gauges map[string]*prometheus.GaugeVec) []*prometheus.GaugeVec {
	keys := make([]string, 0, len(gauges))
	for key := range gauges {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]*prometheus.GaugeVec, 0, len(gauges))
	seen := make(map[*prometheus.GaugeVec]bool, len(gauges))
	for _, key := range keys {
		gv := gauges[key]
		if gv == nil || seen[gv] {
			continue
		}
		seen[gv] = true
		out = append(out, gv)
	}
	return out
}
