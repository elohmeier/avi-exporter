package collector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

// poolLabelValues returns the label values for a pool, in poolLbl order.
func (e *Exporter) poolLabelValues(tenant string, item avi.PoolInventoryItem) []string {
	mi := avi.ParseMarkers(item.Config.Markers)
	ako := "false"
	if avi.IsAKOManaged(item.Config.CreatedBy) {
		ako = "true"
	}
	return e.appendLabels(tenant, item.Config.Name, item.Config.UUID,
		mi.Namespace, mi.ServiceName, mi.IngressName, mi.Host, ako)
}

func (e *Exporter) collectPoolInventory(ctx context.Context, tenant string, items []avi.PoolInventoryItem, ch chan<- prometheus.Metric) {
	for _, it := range items {
		labels := e.poolLabelValues(tenant, it)

		up := 0.0
		if it.Runtime.OperStatus.State == "OPER_UP" {
			up = 1
		}
		e.poolOperUp.WithLabelValues(labels...).Set(up)
		e.emitOperStatusInfo(e.poolOperStatusInfo, labels, it.Runtime.OperStatus.State)

		enabled := 0.0
		if it.Config.Enabled != nil && *it.Config.Enabled {
			enabled = 1
		}
		e.poolEnabled.WithLabelValues(labels...).Set(enabled)

		e.poolHealthScore.WithLabelValues(labels...).Set(it.HealthScore.HealthScore)
		e.poolNumServers.WithLabelValues(labels...).Set(float64(it.Runtime.NumServers))
		e.poolNumServersUp.WithLabelValues(labels...).Set(float64(it.Runtime.NumServersUp))
		e.poolNumServersEnabled.WithLabelValues(labels...).Set(float64(it.Runtime.NumServersEnabled))
		e.poolPercentServersUpEnabled.WithLabelValues(labels...).Set(float64(it.Runtime.PercentServersUpEnabled))
		e.poolPercentServersUpTotal.WithLabelValues(labels...).Set(float64(it.Runtime.PercentServersUpTotal))

		e.emitAlertLevel(e.poolAlertLevel, labels, it.Alert.Level)
		e.emitInfo(e.poolAppProfileType, labels, "app_profile_type", it.AppProfileType)
	}
	e.poolOperUp.Collect(ch)
	e.poolOperStatusInfo.Collect(ch)
	e.poolEnabled.Collect(ch)
	e.poolHealthScore.Collect(ch)
	e.poolNumServers.Collect(ch)
	e.poolNumServersUp.Collect(ch)
	e.poolNumServersEnabled.Collect(ch)
	e.poolPercentServersUpEnabled.Collect(ch)
	e.poolPercentServersUpTotal.Collect(ch)
	e.poolAlertLevel.Collect(ch)
	e.poolAppProfileType.Collect(ch)
}

// collectPoolMembers runs an N+1 follow-up against /api/pool/<uuid>/runtime/server/detail/
// for every pool in items, in parallel up to e.parallelism. The bulk
// pool-inventory endpoint does NOT return per-server data; this is the
// canonical way to get it.
//
// Per-pool failures are counted and surfaced as a synthetic module error
// (with a sample first error) so runModule increments scrape_errors_total.
func (e *Exporter) collectPoolMembers(ctx context.Context, tenant string, items []avi.PoolInventoryItem, ch chan<- prometheus.Metric) error {
	sem := make(chan struct{}, e.parallelism)
	var wg sync.WaitGroup
	var failed atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error

	for _, it := range items {
		it := it
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			servers, err := e.client.GetPoolRuntimeDetail(ctx, tenant, it.Config.UUID)
			if err != nil {
				e.logger.Debug("pool runtime detail", "tenant", tenant, "pool", it.Config.Name, "err", err)
				failed.Add(1)
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("pool %s: %w", it.Config.Name, err)
				}
				firstErrMu.Unlock()
				return
			}

			poolBase := e.poolLabelValues(tenant, it)
			poolNodeID := "pool:" + it.Config.UUID
			mi := avi.ParseMarkers(it.Config.Markers)
			chain := chainFor(mi, it.Config.Name)

			for _, s := range servers {
				memberLbl := e.appendPoolMemberLabels(poolBase, s.IPAddr.Addr, strconv.Itoa(s.Port))
				up := 0.0
				if s.OperStatus.State == "OPER_UP" {
					up = 1
				}
				e.poolMemberOperUp.WithLabelValues(memberLbl...).Set(up)
				e.emitOperStatusInfo(e.poolMemberOperStatusInfo, memberLbl, s.OperStatus.State)

				// Topology poolmember node + pool→poolmember edge.
				if !e.cfg.IsModuleDisabled("topology") {
					memberID := "poolmember:" + s.IPAddr.Addr + ":" + strconv.Itoa(s.Port)
					memberTitle := s.IPAddr.Addr + ":" + strconv.Itoa(s.Port)
					memberState, memberValue, memberColor := "DOWN", 0.0, "red"
					if s.OperStatus.State == "OPER_UP" {
						memberState, memberValue, memberColor = "UP", 1, "green"
					}
					nodeLabels := e.appendLabels(tenant, memberID, memberTitle, "", "poolmember", memberState, chain, "", "", memberColor)
					e.topologyNode.WithLabelValues(nodeLabels...).Set(memberValue)

					statsLabels := e.appendLabels(tenant, memberID, "poolmember", chain)
					e.topologyNodeState.WithLabelValues(statsLabels...).Set(memberValue)

					edgeID := poolNodeID + "->" + memberID
					edgeLabels := e.appendLabels(tenant, edgeID, poolNodeID, memberID, chain, "")
					e.topologyEdge.WithLabelValues(edgeLabels...).Set(1)
				}
			}
		}()
	}
	wg.Wait()

	e.poolMemberOperUp.Collect(ch)
	e.poolMemberOperStatusInfo.Collect(ch)

	if n := failed.Load(); n > 0 {
		return fmt.Errorf("%d/%d pool detail calls failed; first error: %w", n, len(items), firstErr)
	}
	return nil
}

// errAnalyticsFailed wraps an analytics POST failure so callers can detect it
// for self-metric bookkeeping.
var errAnalyticsFailed = errors.New("analytics collection failed")

// appendPoolMemberLabels rebuilds the poolMemberLbl ordering from a
// pool-label slice. See poolMemberLbl wiring in exporter.go.
func (e *Exporter) appendPoolMemberLabels(poolBase []string, ip, port string) []string {
	// poolBase layout: [user-labels..., tenant, pool, pool_uuid, namespace, service, ingress, host, ako]
	// poolMember adds ip,port immediately after pool_uuid.
	tail := len(e.labelKeys) // index where tenant starts
	tenantPlus3 := tail + 3  // after tenant, pool, pool_uuid
	out := make([]string, 0, len(poolBase)+2)
	out = append(out, poolBase[:tenantPlus3]...)
	out = append(out, ip, port)
	out = append(out, poolBase[tenantPlus3:]...)
	return out
}

var poolMetricIDs = []string{
	"l4_server.avg_bandwidth",
	"l4_server.avg_complete_conns",
	"l4_server.avg_open_conns",
	"l4_server.avg_total_rtt",
	"l7_server.avg_resp_latency",
	"l7_server.avg_error_responses",
	"l4_server.avg_health_status",
	"l4_server.avg_uptime",
}

func (e *Exporter) collectPoolAnalytics(ctx context.Context, tenant string, items []avi.PoolInventoryItem, ch chan<- prometheus.Metric) error {
	// Pool metrics ride on the VSERVER_METRICS_ENTITY namespace, scoped by
	// pool_uuid. The current shape passes the pool UUID as entity_uuid AND
	// as pool_uuid. Different controllers key the response map differently
	// (by pool UUID per the request, or by VS UUID for the VS that holds
	// the pool); the join below tolerates both via header.EntityUUID lookup.
	queries := make([]avi.MetricQuery, 0, len(poolMetricIDs)*len(items))
	for _, it := range items {
		for _, id := range poolMetricIDs {
			queries = append(queries, avi.MetricQuery{
				EntityUUID:   it.Config.UUID,
				MetricEntity: avi.EntityVS,
				PoolUUID:     it.Config.UUID,
				MetricID:     id,
				Step:         e.cfg.MetricsStep,
				Limit:        e.cfg.MetricsLimit,
			})
		}
	}
	if len(queries) == 0 {
		return nil
	}

	resp, err := e.client.CollectMetrics(ctx, tenant, avi.MetricsCollectionRequest{MetricRequests: queries})
	if err != nil {
		e.logger.Error("collect pool metrics", "tenant", tenant, "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	byUUID := make(map[string]avi.PoolInventoryItem, len(items))
	for _, it := range items {
		byUUID[it.Config.UUID] = it
	}

	// Join by response-map key first, falling back to header.entity_uuid
	// so controllers that key by VS UUID still match through the
	// pool_uuid-bearing header (when present).
	totalSeries := 0
	matched := 0
	for key, series := range resp.Series {
		for _, s := range series {
			totalSeries++
			it, ok := byUUID[key]
			if !ok {
				it, ok = byUUID[s.Header.EntityUUID]
			}
			if !ok {
				continue
			}
			matched++
			v, ok := s.Last()
			if !ok {
				continue
			}
			if g := e.poolGaugeFor(s.Header.Name); g != nil {
				g.WithLabelValues(e.poolLabelValues(tenant, it)...).Set(v)
			}
		}
	}
	if totalSeries == 0 {
		e.logger.Debug("pool analytics returned zero series — controller may reject this query shape",
			"tenant", tenant, "queries", len(queries))
	} else if matched == 0 {
		e.logger.Debug("pool analytics returned series but none matched known pool UUIDs",
			"tenant", tenant, "series", totalSeries)
	}

	e.poolAvgBandwidth.Collect(ch)
	e.poolAvgCompleteConns.Collect(ch)
	e.poolAvgOpenConns.Collect(ch)
	e.poolAvgTotalRTT.Collect(ch)
	e.poolAvgRespLatency.Collect(ch)
	e.poolAvgErrorResp.Collect(ch)
	e.poolAvgHealthStatus.Collect(ch)
	e.poolAvgUptime.Collect(ch)
	return nil
}

func (e *Exporter) poolGaugeFor(metricID string) *prometheus.GaugeVec {
	switch metricID {
	case "l4_server.avg_bandwidth":
		return e.poolAvgBandwidth
	case "l4_server.avg_complete_conns":
		return e.poolAvgCompleteConns
	case "l4_server.avg_open_conns":
		return e.poolAvgOpenConns
	case "l4_server.avg_total_rtt":
		return e.poolAvgTotalRTT
	case "l7_server.avg_resp_latency":
		return e.poolAvgRespLatency
	case "l7_server.avg_error_responses":
		return e.poolAvgErrorResp
	case "l4_server.avg_health_status":
		return e.poolAvgHealthStatus
	case "l4_server.avg_uptime":
		return e.poolAvgUptime
	}
	return nil
}
