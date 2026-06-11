package collector

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

// vipLabelValues returns label values in vipLbl order:
// base..., tenant, vsvip, vsvip_uuid, vip_id, ip, namespace, service, ingress, host, ako.
func (e *Exporter) vipLabelValues(tenant string, item avi.VsVipInventoryItem, vipID, ip string) []string {
	mi := avi.ParseMarkers(item.Config.Markers)
	ako := "false"
	if avi.IsAKOManaged(item.Config.CreatedBy) {
		ako = "true"
	}
	return e.appendLabels(tenant, item.Config.Name, item.Config.UUID, vipID, ip,
		mi.Namespace, mi.ServiceName, mi.IngressName, mi.Host, ako)
}

// vipPlacementLabelValues mirrors vipPlacementLbl.
func (e *Exporter) vipPlacementLabelValues(tenant string, item avi.VsVipInventoryItem, vipID, seName, seUUID string, primary bool) []string {
	mi := avi.ParseMarkers(item.Config.Markers)
	ako := "false"
	if avi.IsAKOManaged(item.Config.CreatedBy) {
		ako = "true"
	}
	primaryStr := "false"
	if primary {
		primaryStr = "true"
	}
	return e.appendLabels(tenant, item.Config.Name, item.Config.UUID, vipID, seName, seUUID, primaryStr,
		mi.Namespace, mi.ServiceName, mi.IngressName, mi.Host, ako)
}

// vipDNSLabelValues mirrors vipDNSLbl.
func (e *Exporter) vipDNSLabelValues(tenant string, item avi.VsVipInventoryItem, fqdn, recType, ttl string) []string {
	mi := avi.ParseMarkers(item.Config.Markers)
	ako := "false"
	if avi.IsAKOManaged(item.Config.CreatedBy) {
		ako = "true"
	}
	return e.appendLabels(tenant, item.Config.Name, item.Config.UUID, fqdn, recType, ttl,
		mi.Namespace, mi.ServiceName, mi.IngressName, mi.Host, ako)
}

// vipPrimaryIP returns the first non-empty IP for a Vip entry, preferring v4.
func vipPrimaryIP(v avi.Vip) string {
	if v.IPAddress != nil && v.IPAddress.Addr != "" {
		return v.IPAddress.Addr
	}
	if v.IP6Address != nil && v.IP6Address.Addr != "" {
		return v.IP6Address.Addr
	}
	return ""
}

func (e *Exporter) collectVsVipInventory(ctx context.Context, tenant string, items []avi.VsVipInventoryItem, ch chan<- prometheus.Metric) {
	for _, it := range items {
		// Index runtime by vip_id so per-Vip entries can be joined to per-vip_id runtime.
		runtimeByVipID := make(map[string]avi.VsVipRuntime, len(it.Runtime))
		for _, rt := range it.Runtime {
			runtimeByVipID[rt.VipID] = rt
		}

		sharedCount := float64(len(it.Config.VirtualServices))

		for _, v := range it.Config.Vip {
			ip := vipPrimaryIP(v)
			labels := e.vipLabelValues(tenant, it, v.VipID, ip)

			e.vipEnabled.WithLabelValues(labels...).Set(boolToFloat(v.Enabled))

			fipPresent := 0.0
			if (v.FloatingIP != nil && v.FloatingIP.Addr != "") || (v.FloatingIP6 != nil && v.FloatingIP6.Addr != "") {
				fipPresent = 1
			}
			e.vipFloatingIP.WithLabelValues(labels...).Set(fipPresent)

			auto := 0.0
			if (v.AviAllocatedVIP != nil && *v.AviAllocatedVIP) || (v.AutoAllocateIP != nil && *v.AutoAllocateIP) {
				auto = 1
			}
			e.vipAutoAllocated.WithLabelValues(labels...).Set(auto)

			e.vipSharedByVsCount.WithLabelValues(labels...).Set(sharedCount)

			rt, hasRT := runtimeByVipID[v.VipID]
			if hasRT {
				up := 0.0
				if rt.OperStatus.State == "OPER_UP" {
					up = 1
				}
				e.vipOperUp.WithLabelValues(labels...).Set(up)
				e.emitOperStatusInfo(e.vipOperStatusInfo, labels, rt.OperStatus.State)
				e.vipPercentSesUp.WithLabelValues(labels...).Set(float64(rt.PercentSesUp))
				e.vipNumSeAssigned.WithLabelValues(labels...).Set(float64(rt.NumSeAssigned))
				e.vipNumSeRequested.WithLabelValues(labels...).Set(float64(rt.NumSeRequested))

				for _, se := range rt.ServiceEngine {
					primary := se.Primary != nil && *se.Primary
					active := 0.0
					if se.ActiveOnSe != nil && *se.ActiveOnSe {
						active = 1
					}
					placement := e.vipPlacementLabelValues(tenant, it, v.VipID, se.Name, avi.RefUUID(se.URL), primary)
					e.vipActiveOnSe.WithLabelValues(placement...).Set(active)
				}
			}
		}

		for _, d := range it.Config.DNSInfo {
			ttl := strconv.Itoa(d.TTL)
			labels := e.vipDNSLabelValues(tenant, it, d.FQDN, d.Type, ttl)
			e.vipDNSRecord.WithLabelValues(labels...).Set(1)
		}
	}

	e.vipOperUp.Collect(ch)
	e.vipOperStatusInfo.Collect(ch)
	e.vipEnabled.Collect(ch)
	e.vipPercentSesUp.Collect(ch)
	e.vipNumSeAssigned.Collect(ch)
	e.vipNumSeRequested.Collect(ch)
	e.vipActiveOnSe.Collect(ch)
	e.vipSharedByVsCount.Collect(ch)
	e.vipFloatingIP.Collect(ch)
	e.vipAutoAllocated.Collect(ch)
	e.vipDNSRecord.Collect(ch)

	_ = ctx
}
