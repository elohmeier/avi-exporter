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

type poolMemberSnapshot struct {
	Pool   avi.PoolInventoryItem
	Server avi.ServerRuntimeDetail
}

// poolLabelValues returns the label values for a pool, in poolLbl order.
func (e *Exporter) poolLabelValues(tenant string, item avi.PoolInventoryItem) []string {
	mi := avi.ParseObjectMetadata(item.Config.Markers, item.Config.ServiceMetadata)
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
}

// collectPoolMembers runs an N+1 follow-up against /api/pool/<uuid>/runtime/server/detail/
// for every pool in items, in parallel up to e.parallelism. The bulk
// pool-inventory endpoint does NOT return per-server data; this is the
// canonical way to get it.
//
// Per-pool failures are counted and surfaced as a synthetic module error
// (with a sample first error) so runModule increments scrape_errors_total.
func (e *Exporter) collectPoolMembers(ctx context.Context, tenant string, items []avi.PoolInventoryItem, ch chan<- prometheus.Metric) error {
	members, err := e.collectPoolMemberDetails(ctx, tenant, items)
	if err != nil {
		return err
	}
	e.renderPoolMembers(tenant, members)
	if !e.cfg.IsModuleDisabled("topology") {
		e.renderPoolMemberTopology(tenant, members)
	}
	return nil
}

func (e *Exporter) collectPoolMemberDetails(ctx context.Context, tenant string, items []avi.PoolInventoryItem) ([]poolMemberSnapshot, error) {
	sem := make(chan struct{}, e.parallelism)
	var wg sync.WaitGroup
	var failed atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error
	var membersMu sync.Mutex
	members := make([]poolMemberSnapshot, 0)

	for _, it := range items {
		it := it
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
			}
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

			membersMu.Lock()
			for _, s := range servers {
				members = append(members, poolMemberSnapshot{Pool: it, Server: s})
			}
			membersMu.Unlock()
		}()
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return members, err
	}
	if n := failed.Load(); n > 0 {
		return members, fmt.Errorf("%d/%d pool detail calls failed; first error: %w", n, len(items), firstErr)
	}
	return members, nil
}

func (e *Exporter) renderPoolMembers(tenant string, members []poolMemberSnapshot) {
	for _, member := range members {
		poolBase := e.poolLabelValues(tenant, member.Pool)
		server := member.Server
		memberLbl := e.appendPoolMemberLabels(poolBase, server.IPAddr.Addr, strconv.Itoa(server.Port))
		up := 0.0
		if server.OperStatus.State == "OPER_UP" {
			up = 1
		}
		e.poolMemberOperUp.WithLabelValues(memberLbl...).Set(up)
		e.emitOperStatusInfo(e.poolMemberOperStatusInfo, memberLbl, server.OperStatus.State)
	}
}

func (e *Exporter) renderPoolMemberTopology(tenant string, members []poolMemberSnapshot) {
	for _, member := range members {
		pool := member.Pool
		server := member.Server
		poolNodeID := "pool:" + pool.Config.UUID
		mi := avi.ParseObjectMetadata(pool.Config.Markers, pool.Config.ServiceMetadata)
		chain := chainFor(mi, pool.Config.Name)

		memberID := "poolmember:" + server.IPAddr.Addr + ":" + strconv.Itoa(server.Port)
		memberTitle := server.IPAddr.Addr + ":" + strconv.Itoa(server.Port)
		memberState, memberValue, memberColor := "DOWN", 0.0, "red"
		if server.OperStatus.State == "OPER_UP" {
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
	"l4_server.apdexc",
	"l4_server.avg_bandwidth",
	"l4_server.avg_complete_conns",
	"l4_server.avg_errored_connections",
	"l4_server.avg_health_status",
	"l4_server.avg_new_established_conns",
	"l4_server.avg_open_conns",
	"l4_server.avg_pool_complete_conns",
	"l4_server.avg_pool_open_conns",
	"l4_server.avg_rx_bytes",
	"l4_server.avg_rx_pkts",
	"l4_server.avg_total_rtt",
	"l4_server.avg_tx_bytes",
	"l4_server.avg_tx_pkts",
	"l4_server.avg_uptime",
	"l4_server.max_open_conns",
	"l7_server.apdexr",
	"l7_server.avg_application_response_time",
	"l7_server.avg_complete_responses",
	"l7_server.avg_error_responses",
	"l7_server.avg_frustrated_responses",
	"l7_server.avg_resp_4xx",
	"l7_server.avg_resp_5xx",
	"l7_server.avg_resp_latency",
	"l7_server.avg_total_requests",
	"l7_server.pct_response_errors",
	"healthscore.health_score_value",
}

func (e *Exporter) collectPoolAnalytics(ctx context.Context, tenant string, items []avi.PoolInventoryItem, ch chan<- prometheus.Metric) error {
	if len(items) == 0 {
		e.cacheMu.Lock()
		deleteTenantGaugeVecs(tenant, e.poolAnalyticsGaugeVecs()...)
		e.cacheMu.Unlock()
		return nil
	}

	families, err := e.collectBuiltinPrometheusMetrics(ctx, "pool", []string{tenant}, poolMetricIDs)
	if err != nil {
		e.logger.Error("collect pool metrics", "tenant", tenant, "err", err)
		return fmt.Errorf("%w: %v", errAnalyticsFailed, err)
	}

	byUUID := make(map[string]avi.PoolInventoryItem, len(items))
	for _, it := range items {
		byUUID[it.Config.UUID] = it
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	deleteTenantGaugeVecs(tenant, e.poolAnalyticsGaugeVecs()...)

	totalSeries := 0
	matched := 0
	for familyName, family := range families {
		g := e.poolGaugeFor(familyName)
		if g == nil {
			continue
		}
		for _, metric := range family.GetMetric() {
			totalSeries++
			it, ok := byUUID[prometheusLabelValue(metric, "uuid")]
			if !ok {
				continue
			}
			matched++
			value, ok := prometheusMetricValue(metric)
			if !ok {
				continue
			}
			g.WithLabelValues(e.poolLabelValues(tenant, it)...).Set(value)
		}
	}
	if totalSeries == 0 {
		e.logger.Debug("pool analytics returned zero series — controller may reject this query shape",
			"tenant", tenant, "metrics", len(poolMetricIDs))
	} else if matched == 0 {
		e.logger.Debug("pool analytics returned series but none matched known pool UUIDs",
			"tenant", tenant, "series", totalSeries)
	}

	return nil
}

func (e *Exporter) poolGaugeFor(metricID string) *prometheus.GaugeVec {
	return e.poolAnalyticsGauges[metricID]
}
