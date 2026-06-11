package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

// gslbLabelValues returns label values in gslbLbl order:
// base..., tenant, gslbservice, gslbservice_uuid.
func (e *Exporter) gslbLabelValues(tenant string, item avi.GslbServiceInventoryItem) []string {
	return e.appendLabels(tenant, item.Config.Name, item.Config.UUID)
}

func (e *Exporter) collectGslbServices(ctx context.Context, tenant string, items []avi.GslbServiceInventoryItem, ch chan<- prometheus.Metric) {
	for _, it := range items {
		labels := e.gslbLabelValues(tenant, it)

		up := 0.0
		if it.Runtime.OperStatus.State == "OPER_UP" {
			up = 1
		}
		e.gslbServiceOperUp.WithLabelValues(labels...).Set(up)
		e.emitOperStatusInfo(e.gslbServiceOperStatusInfo, labels, it.Runtime.OperStatus.State)

		enabled := 0.0
		if it.Config.Enabled != nil && *it.Config.Enabled {
			enabled = 1
		}
		e.gslbServiceEnabled.WithLabelValues(labels...).Set(enabled)

		e.gslbServiceMemberCount.WithLabelValues(labels...).Set(float64(len(it.Config.Groups)))

		// Emit one info-metric per FQDN attached to the GSLB service.
		for _, fqdn := range it.Config.DomainNames {
			e.emitInfo(e.gslbServiceDomainsInfo, labels, "fqdn", fqdn)
		}
	}
	e.gslbServiceOperUp.Collect(ch)
	e.gslbServiceOperStatusInfo.Collect(ch)
	e.gslbServiceEnabled.Collect(ch)
	e.gslbServiceMemberCount.Collect(ch)
	e.gslbServiceDomainsInfo.Collect(ch)
}
