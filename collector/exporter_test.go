package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/elohmeier/avi-exporter/config"
)

func TestNewExporterRejectsInvalidParallelism(t *testing.T) {
	cfg := testConfig([]string{"admin"}, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, parallelism := range []int{0, -1} {
		t.Run(fmt.Sprintf("parallelism=%d", parallelism), func(t *testing.T) {
			_, err := NewExporter(cfg, "https://controller.example", "user", "pass", true, "", parallelism, logger)
			if err == nil {
				t.Fatalf("NewExporter accepted parallelism %d", parallelism)
			}
		})
	}
}

func TestParallelTenantScrapeDoesNotEmitDuplicateSeries(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/virtualservice-inventory" {
			http.NotFound(w, r)
			return
		}
		tenant := r.Header.Get("X-Avi-Tenant")
		writeJSON(t, w, map[string]any{
			"count": 1,
			"results": []map[string]any{
				{
					"config": map[string]any{
						"uuid":    "virtualservice-" + tenant,
						"name":    "vs-" + tenant,
						"enabled": true,
						"type":    "VS_TYPE_NORMAL",
					},
					"runtime": map[string]any{
						"oper_status":    map[string]any{"state": "OPER_UP"},
						"percent_ses_up": 100,
					},
					"health_score": map[string]any{"health_score": 100},
				},
			},
		})
	})
	defer controller.Close()

	cfg := testConfig([]string{"tenant-a", "tenant-b"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})

	mfs := gatherExporter(t, cfg, controller.URL, 2)
	vsOperUp := metricFamily(t, mfs, "avi_vs_oper_up")
	if got := len(vsOperUp.Metric); got != 2 {
		t.Fatalf("avi_vs_oper_up emitted %d metrics, want 2", got)
	}
}

func TestTenantUpReflectsPoolGroupFailure(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/poolgroup-inventory" {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "pool group unavailable", http.StatusInternalServerError)
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "gslb", "topology",
	})

	mfs := gatherExporter(t, cfg, controller.URL, 1)
	up := metricFamily(t, mfs, "avi_up")
	if got := metricValueForLabel(t, up, "tenant", "admin"); got != 0 {
		t.Fatalf("avi_up{tenant=%q} = %v, want 0", "admin", got)
	}
}

func TestControllerMetricsRenderFromAnalytics(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/analytics/metrics/collection" {
			http.NotFound(w, r)
			return
		}
		writeJSON(t, w, map[string]any{
			"series": map[string]any{
				"controller": []map[string]any{
					{
						"header": map[string]any{"name": "controller_stats.avg_cpu_usage"},
						"data":   []map[string]any{{"timestamp": "2026-01-01T00:00:00Z", "value": 42.5}},
					},
				},
			},
		})
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster",
		"se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})

	mfs := gatherExporter(t, cfg, controller.URL, 1)
	cpu := metricFamily(t, mfs, "avi_controller_avg_cpu_usage")
	if got := firstGaugeValue(t, cpu); got != 42.5 {
		t.Fatalf("avi_controller_avg_cpu_usage = %v, want 42.5", got)
	}
}

func TestControllerMetricsKeepPerNodeSeries(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/analytics/metrics/collection" {
			http.NotFound(w, r)
			return
		}
		writeJSON(t, w, map[string]any{
			"series": map[string]any{
				"controller-node-a": []map[string]any{
					{
						"header": map[string]any{
							"name":        "controller_stats.avg_cpu_usage",
							"entity_uuid": "controller-node-a",
						},
						"data": []map[string]any{{"timestamp": "2026-01-01T00:00:00Z", "value": 31.0}},
					},
				},
				"controller-node-b": []map[string]any{
					{
						"header": map[string]any{
							"name":        "controller_stats.avg_cpu_usage",
							"entity_uuid": "controller-node-b",
						},
						"data": []map[string]any{{"timestamp": "2026-01-01T00:00:00Z", "value": 72.0}},
					},
				},
			},
		})
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster",
		"se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})

	mfs := gatherExporter(t, cfg, controller.URL, 1)
	cpu := metricFamily(t, mfs, "avi_controller_avg_cpu_usage")
	if got := len(cpu.Metric); got != 2 {
		t.Fatalf("avi_controller_avg_cpu_usage emitted %d metrics, want 2 per-node series", got)
	}
	if got := metricValueForLabel(t, cpu, "controller_uuid", "controller-node-a"); got != 31.0 {
		t.Fatalf("controller-node-a cpu = %v, want 31", got)
	}
	if got := metricValueForLabel(t, cpu, "controller_uuid", "controller-node-b"); got != 72.0 {
		t.Fatalf("controller-node-b cpu = %v, want 72", got)
	}
}

func TestPrometheusGatherDoesNotCallAviAfterRefresh(t *testing.T) {
	var apiCalls atomic.Int64
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		apiCalls.Add(1)
		if r.URL.Path != "/api/virtualservice-inventory" {
			http.NotFound(w, r)
			return
		}
		writeVSInventory(t, w, "admin", "OPER_UP")
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}
	apiCalls.Store(0)

	reg := prometheus.NewRegistry()
	reg.MustRegister(exp)
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if got := apiCalls.Load(); got != 0 {
		t.Fatalf("Gather made %d Avi API calls, want 0", got)
	}
}

func TestFailedRefreshKeepsLastGoodData(t *testing.T) {
	var fail atomic.Bool
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/virtualservice-inventory" {
			http.NotFound(w, r)
			return
		}
		if fail.Load() {
			http.Error(w, "inventory unavailable", http.StatusInternalServerError)
			return
		}
		writeVSInventory(t, w, "admin", "OPER_UP")
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("first RefreshOnce: %v", err)
	}
	fail.Store(true)
	if err := exp.RefreshOnce(context.Background()); err == nil {
		t.Fatalf("second RefreshOnce succeeded, want error")
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(exp)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	vsOperUp := metricFamily(t, mfs, "avi_vs_oper_up")
	if got := metricValueForLabel(t, vsOperUp, "tenant", "admin"); got != 1 {
		t.Fatalf("cached avi_vs_oper_up = %v, want last-good value 1", got)
	}
	up := metricFamily(t, mfs, "avi_up")
	if got := metricValueForLabel(t, up, "tenant", "admin"); got != 0 {
		t.Fatalf("avi_up{tenant=%q} = %v, want 0 after failed refresh", "admin", got)
	}
}

func TestReadyFailsWhenRequiredModuleIsStale(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/virtualservice-inventory" {
			http.NotFound(w, r)
			return
		}
		writeVSInventory(t, w, "admin", "OPER_UP")
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}
	if !exp.Ready() {
		t.Fatalf("Ready() = false after successful refresh")
	}

	exp.cacheMu.Lock()
	for _, st := range exp.moduleStates {
		st.LastSuccess = time.Now().Add(-1 * time.Hour)
	}
	exp.cacheMu.Unlock()

	if exp.Ready() {
		t.Fatalf("Ready() = true with stale required module data")
	}
	rr := httptest.NewRecorder()
	exp.ReadyHandler(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz returned %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
}

func TestReadyFailsWhenTenantModuleDataIsMissing(t *testing.T) {
	cfg := testConfig([]string{"tenant-a"}, []string{
		"controller_metrics",
		"se_inventory", "se_metrics",
		"vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, "https://controller.example", "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}

	exp.cacheMu.Lock()
	exp.setTenantsLocked([]string{"tenant-a"})
	st := exp.ensureModuleStateLocked("cluster", "")
	st.LastAttempt = time.Now()
	st.LastSuccess = st.LastAttempt
	exp.cacheMu.Unlock()

	if exp.Ready() {
		t.Fatalf("Ready() = true with fresh cluster data but missing tenant module data")
	}
}

func TestFailedPoolMemberRefreshKeepsLastGoodMembers(t *testing.T) {
	var failPoolB atomic.Bool
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/pool-inventory":
			writePoolInventory(t, w, "pool-a", "pool-b")
		case r.URL.Path == "/api/pool/pool-a/runtime/server/detail/":
			writePoolRuntimeDetail(t, w, "10.0.0.1", 8080)
		case r.URL.Path == "/api/pool/pool-b/runtime/server/detail/":
			if failPoolB.Load() {
				http.Error(w, "pool detail unavailable", http.StatusInternalServerError)
				return
			}
			writePoolRuntimeDetail(t, w, "10.0.0.2", 8080)
		default:
			http.NotFound(w, r)
		}
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics",
		"pool_inventory", "pool_metrics",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 2, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("first RefreshOnce: %v", err)
	}

	failPoolB.Store(true)
	if err := exp.RefreshOnce(context.Background()); err == nil {
		t.Fatalf("second RefreshOnce succeeded, want pool member error")
	}

	mfs := gatherRegisteredExporter(t, exp)
	members := metricFamily(t, mfs, "avi_pool_member_oper_up")
	if got := countMetricsWithLabel(members, "ip", "10.0.0.1"); got != 1 {
		t.Fatalf("pool-a member count = %d, want 1", got)
	}
	if got := countMetricsWithLabel(members, "ip", "10.0.0.2"); got != 1 {
		t.Fatalf("pool-b member count after failed refresh = %d, want last-good member", got)
	}
}

func TestSuccessfulEmptyPoolAnalyticsClearsStaleSeries(t *testing.T) {
	var poolInventoryCalls atomic.Int64
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/pool-inventory":
			if poolInventoryCalls.Add(1) == 1 {
				writePoolInventory(t, w, "pool-a")
				return
			}
			writePoolInventory(t, w)
		case "/api/analytics/metrics/collection":
			writeJSON(t, w, map[string]any{
				"series": map[string]any{
					"pool-a": []map[string]any{
						{
							"header": map[string]any{
								"name":        "l4_server.avg_bandwidth",
								"entity_uuid": "pool-a",
							},
							"data": []map[string]any{{"timestamp": "2026-01-01T00:00:00Z", "value": 123.0}},
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics",
		"pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("first RefreshOnce: %v", err)
	}

	bandwidth := metricFamily(t, gatherRegisteredExporter(t, exp), "avi_pool_l4_server_avg_bandwidth_bps")
	if got := countMetricsWithLabel(bandwidth, "pool_uuid", "pool-a"); got != 1 {
		t.Fatalf("pool-a bandwidth count after first refresh = %d, want 1", got)
	}

	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("second RefreshOnce: %v", err)
	}
	if got := countMetricsWithLabelOptional(gatherRegisteredExporter(t, exp), "avi_pool_l4_server_avg_bandwidth_bps", "pool_uuid", "pool-a"); got != 0 {
		t.Fatalf("pool-a bandwidth count after empty pool refresh = %d, want 0", got)
	}
}

func TestFailedTopologyDependencyKeepsLastGoodTopology(t *testing.T) {
	var failPoolGroup atomic.Bool
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/virtualservice-inventory":
			writeVSInventoryWithPoolGroup(t, w)
		case "/api/pool-inventory":
			writePoolInventory(t, w, "pool-a")
		case "/api/vsvip-inventory":
			writeVsVipInventory(t, w)
		case "/api/poolgroup-inventory":
			if failPoolGroup.Load() {
				http.Error(w, "pool group unavailable", http.StatusInternalServerError)
				return
			}
			writePoolGroupInventory(t, w)
		default:
			http.NotFound(w, r)
		}
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "gslb",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("first RefreshOnce: %v", err)
	}

	failPoolGroup.Store(true)
	if err := exp.RefreshOnce(context.Background()); err == nil {
		t.Fatalf("second RefreshOnce succeeded, want pool group error")
	}

	topology := metricFamily(t, gatherRegisteredExporter(t, exp), "avi_topology_node")
	if got := countMetricsWithLabel(topology, "id", "poolgroup:poolgroup-a"); got != 1 {
		t.Fatalf("poolgroup topology node count after failed dependency refresh = %d, want last-good node", got)
	}
}

func TestWildcardTenantRefreshRemovesDroppedTenantMetrics(t *testing.T) {
	var tenantRefreshes atomic.Int64
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tenant":
			if tenantRefreshes.Add(1) == 1 {
				writeTenants(t, w, "tenant-a", "tenant-b")
			} else {
				writeTenants(t, w, "tenant-b")
			}
		case "/api/virtualservice-inventory":
			writeVSInventory(t, w, r.Header.Get("X-Avi-Tenant"), "OPER_UP")
		default:
			http.NotFound(w, r)
		}
	})
	defer controller.Close()

	cfg := testConfig([]string{"*"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("first RefreshOnce: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("second RefreshOnce: %v", err)
	}

	vsOperUp := metricFamily(t, gatherRegisteredExporter(t, exp), "avi_vs_oper_up")
	if got := countMetricsWithLabel(vsOperUp, "tenant", "tenant-a"); got != 0 {
		t.Fatalf("removed tenant-a metric count = %d, want 0", got)
	}
	if got := countMetricsWithLabel(vsOperUp, "tenant", "tenant-b"); got != 1 {
		t.Fatalf("tenant-b metric count = %d, want 1", got)
	}
	if got := countMetricsWithLabel(vsOperUp, "tenant", "admin"); got != 0 {
		t.Fatalf("unexpected implicit admin metric count = %d, want 0", got)
	}
	for _, module := range exp.CacheStatus().Modules {
		if module.Tenant == "tenant-a" {
			t.Fatalf("removed tenant-a still present in cache module status: %+v", module)
		}
		if module.Tenant == "admin" {
			t.Fatalf("implicit admin still present in cache module status: %+v", module)
		}
	}
}

func TestTenantRefreshHonorsParallelism(t *testing.T) {
	var current atomic.Int64
	var maxConcurrent atomic.Int64
	var releaseOnce sync.Once
	release := make(chan struct{})

	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/virtualservice-inventory" {
			http.NotFound(w, r)
			return
		}
		now := current.Add(1)
		for {
			max := maxConcurrent.Load()
			if now <= max || maxConcurrent.CompareAndSwap(max, now) {
				break
			}
		}
		if now == 2 {
			releaseOnce.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-time.After(200 * time.Millisecond):
			releaseOnce.Do(func() { close(release) })
		}
		defer current.Add(-1)
		writeVSInventory(t, w, r.Header.Get("X-Avi-Tenant"), "OPER_UP")
	})
	defer controller.Close()

	cfg := testConfig([]string{"tenant-a", "tenant-b"}, []string{
		"cluster", "controller_metrics",
		"se_inventory", "se_metrics",
		"vs_metrics",
		"pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 2, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}
	if got := maxConcurrent.Load(); got < 2 {
		t.Fatalf("max concurrent tenant refreshes = %d, want at least 2", got)
	}
}

func testConfig(tenants, disabled []string) *config.Config {
	return &config.Config{
		Labels:          map[string]string{},
		DisabledModules: disabled,
		Tenants:         tenants,
		APIVersion:      "30.2.1",
		MetricsStep:     300,
		MetricsLimit:    1,
	}
}

func writeTenants(t *testing.T, w http.ResponseWriter, names ...string) {
	t.Helper()
	results := make([]map[string]any, 0, len(names))
	for _, name := range names {
		results = append(results, map[string]any{"name": name, "uuid": name + "-uuid"})
	}
	writeJSON(t, w, map[string]any{"count": len(results), "results": results})
}

func writeVSInventory(t *testing.T, w http.ResponseWriter, tenant, state string) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"count": 1,
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid":    "virtualservice-" + tenant,
					"name":    "vs-" + tenant,
					"enabled": true,
					"type":    "VS_TYPE_NORMAL",
				},
				"runtime": map[string]any{
					"oper_status":    map[string]any{"state": state},
					"percent_ses_up": 100,
				},
				"health_score": map[string]any{"health_score": 100},
			},
		},
	})
}

func writeVSInventoryWithPoolGroup(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"count": 1,
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid":           "virtualservice-a",
					"name":           "vs-a",
					"enabled":        true,
					"type":           "VS_TYPE_NORMAL",
					"pool_group_ref": "poolgroup-a",
				},
				"runtime": map[string]any{
					"oper_status":    map[string]any{"state": "OPER_UP"},
					"percent_ses_up": 100,
				},
				"health_score": map[string]any{"health_score": 100},
			},
		},
	})
}

func writePoolInventory(t *testing.T, w http.ResponseWriter, uuids ...string) {
	t.Helper()
	results := make([]map[string]any, 0, len(uuids))
	for _, uuid := range uuids {
		results = append(results, map[string]any{
			"config": map[string]any{
				"uuid":    uuid,
				"name":    strings.ReplaceAll(uuid, "-", " "),
				"enabled": true,
			},
			"runtime": map[string]any{
				"oper_status":                map[string]any{"state": "OPER_UP"},
				"num_servers":                1,
				"num_servers_up":             1,
				"num_servers_enabled":        1,
				"percent_servers_up_enabled": 100,
				"percent_servers_up_total":   100,
			},
			"health_score": map[string]any{"health_score": 100},
		})
	}
	writeJSON(t, w, map[string]any{"count": len(results), "results": results})
}

func writePoolRuntimeDetail(t *testing.T, w http.ResponseWriter, ip string, port int) {
	t.Helper()
	writeJSON(t, w, []map[string]any{
		{
			"ip_addr":     map[string]any{"addr": ip, "type": "V4"},
			"port":        port,
			"oper_status": map[string]any{"state": "OPER_UP"},
		},
	})
}

func writePoolGroupInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"count": 1,
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid": "poolgroup-a",
					"name": "poolgroup a",
					"members": []map[string]any{
						{"pool_ref": "pool-a"},
					},
				},
			},
		},
	})
}

func writeVsVipInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"count": 1,
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid": "vsvip-a",
					"name": "vsvip a",
					"vip": []map[string]any{
						{
							"vip_id":     "1",
							"enabled":    true,
							"ip_address": map[string]any{"addr": "192.0.2.10", "type": "V4"},
						},
					},
				},
				"runtime": []map[string]any{
					{
						"vip_id":           "1",
						"oper_status":      map[string]any{"state": "OPER_UP"},
						"percent_ses_up":   100,
						"num_se_assigned":  1,
						"num_se_requested": 1,
					},
				},
			},
		},
	})
}

func newFakeController(t *testing.T, api http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			http.SetCookie(w, &http.Cookie{Name: "csrftoken", Value: "csrf-token"})
			http.SetCookie(w, &http.Cookie{Name: "sessionid", Value: "session-id"})
			writeJSON(t, w, map[string]string{"ok": "true"})
			return
		}
		if r.Header.Get("X-Avi-Version") == "" {
			t.Errorf("%s missing X-Avi-Version", r.URL.Path)
		}
		api(w, r)
	}))
}

func gatherExporter(t *testing.T, cfg *config.Config, url string, parallelism int) []*dto.MetricFamily {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, url, "user", "pass", true, "", parallelism, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Logf("RefreshOnce completed with errors: %v", err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(exp)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	return mfs
}

func gatherRegisteredExporter(t *testing.T, exp *Exporter) []*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(exp)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	return mfs
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func metricFamily(t *testing.T, mfs []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func metricValueForLabel(t *testing.T, mf *dto.MetricFamily, labelName, labelValue string) float64 {
	t.Helper()
	for _, m := range mf.Metric {
		for _, l := range m.Label {
			if l.GetName() == labelName && l.GetValue() == labelValue {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("metric %q with %s=%q not found", mf.GetName(), labelName, labelValue)
	return 0
}

func countMetricsWithLabel(mf *dto.MetricFamily, labelName, labelValue string) int {
	count := 0
	for _, m := range mf.Metric {
		for _, l := range m.Label {
			if l.GetName() == labelName && l.GetValue() == labelValue {
				count++
				break
			}
		}
	}
	return count
}

func countMetricsWithLabelOptional(mfs []*dto.MetricFamily, metricName, labelName, labelValue string) int {
	for _, mf := range mfs {
		if mf.GetName() == metricName {
			return countMetricsWithLabel(mf, labelName, labelValue)
		}
	}
	return 0
}

func firstGaugeValue(t *testing.T, mf *dto.MetricFamily) float64 {
	t.Helper()
	if len(mf.Metric) == 0 || mf.Metric[0].Gauge == nil {
		t.Fatalf("metric family %q has no gauge samples", mf.GetName())
	}
	return mf.Metric[0].GetGauge().GetValue()
}
