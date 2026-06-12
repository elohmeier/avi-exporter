package collector

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/elohmeier/avi-exporter/avi"
	"github.com/elohmeier/avi-exporter/config"
)

var allTestModules = []string{
	"cluster", "controller_metrics",
	"se_inventory", "se_metrics",
	"vs_inventory", "vs_metrics",
	"pool_inventory", "pool_metrics", "pool_members",
	"vsvip", "pool_group", "gslb", "topology",
}

func newEdgeExporter(t *testing.T, cfg *config.Config, api http.HandlerFunc) (*Exporter, string) {
	t.Helper()
	controller := newFakeController(t, api)
	t.Cleanup(controller.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	return exp, controller.URL
}

func TestSchedulerAndRunRefreshWithLog(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	exp.refreshInterval = time.Millisecond
	exp.Start(nil)
	time.Sleep(5 * time.Millisecond)
	exp.Stop()

	failing, _ := newEdgeExporter(t, testConfig([]string{"admin"}, []string{
		"controller_metrics", "se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics", "pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	}), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "cluster unavailable", http.StatusInternalServerError)
	})
	failing.runRefreshWithLog(context.Background())
}

func TestCacheAndTenantHelperBranches(t *testing.T) {
	badCA := t.TempDir() + "/bad-ca.pem"
	if err := os.WriteFile(badCA, []byte("not a cert"), 0o600); err != nil {
		t.Fatalf("write bad CA: %v", err)
	}
	if _, err := NewExporter(testConfig([]string{"admin"}, nil), "https://controller.example", "user", "pass", false, badCA, 1, nil); err == nil {
		t.Fatalf("NewExporter accepted invalid CA")
	}

	cfg := testConfig(nil, allTestModules)
	cfg.Labels = map[string]string{"zone": "eu", "env": "prod"}
	exp, _ := newEdgeExporter(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	if got, want := exp.buildBaseLabels(), []string{"prod", "eu"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("base labels = %#v, want %#v", got, want)
	}
	if tenants, wildcard := exp.configuredTenants(); wildcard || !reflect.DeepEqual(tenants, []string{"admin"}) {
		t.Fatalf("configuredTenants() = %#v, %v; want admin,false", tenants, wildcard)
	}
	if got := normalizeTenants(nil); !reflect.DeepEqual(got, []string{"admin"}) {
		t.Fatalf("normalizeTenants(nil) = %#v", got)
	}
	if got := normalizeTenants([]string{"", ""}); !reflect.DeepEqual(got, []string{"admin"}) {
		t.Fatalf("normalizeTenants(empty names) = %#v", got)
	}
	if got := normalizeTenants([]string{"b", "a", "b", ""}); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("normalizeTenants(dedupe) = %#v", got)
	}

	exp.cacheMu.Lock()
	if got := exp.expectedTenantsLocked(); !reflect.DeepEqual(got, []string{"admin"}) {
		t.Fatalf("expectedTenantsLocked static = %#v", got)
	}
	exp.cfg.Tenants = []string{"*"}
	if got := exp.expectedTenantsLocked(); got != nil {
		t.Fatalf("expectedTenantsLocked wildcard = %#v, want nil", got)
	}
	exp.cacheMu.Unlock()

	if !(&moduleState{}).isStale(time.Now()) {
		t.Fatalf("zero module state should be stale")
	}
	old := &moduleState{LastSuccess: time.Now().Add(-11 * time.Minute), MaxStale: 0}
	if !old.isStale(time.Now()) {
		t.Fatalf("old module state with default max stale should be stale")
	}

	noExpected, _ := newEdgeExporter(t, testConfig([]string{"admin"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	if noExpected.Ready() {
		t.Fatalf("Ready() = true with no expected modules")
	}
	status := noExpected.CacheStatus()
	if status.Ready {
		t.Fatalf("CacheStatus.Ready = true with no expected modules")
	}

	exp.cacheMu.Lock()
	exp.moduleStates[moduleKey{Module: "same", Tenant: "b"}] = &moduleState{Module: "same", Tenant: "b", Required: true, LastSuccess: time.Now()}
	exp.moduleStates[moduleKey{Module: "same", Tenant: "a"}] = &moduleState{Module: "same", Tenant: "a", Required: true}
	exp.cacheMu.Unlock()
	status = exp.CacheStatus()
	if len(status.Modules) < 2 || status.Modules[0].Tenant != "a" {
		t.Fatalf("CacheStatus modules not sorted by tenant: %#v", status.Modules)
	}
	if status.Ready {
		t.Fatalf("CacheStatus.Ready = true with stale required module")
	}

	readyWithExtraStale, _ := newEdgeExporter(t, testConfig([]string{"admin"}, []string{
		"controller_metrics", "se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics", "pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	}), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	readyWithExtraStale.cacheMu.Lock()
	cluster := readyWithExtraStale.ensureModuleStateLocked("cluster", "")
	cluster.LastSuccess = time.Now()
	extra := readyWithExtraStale.ensureModuleStateLocked("extra", "")
	extra.Required = true
	extra.LastSuccess = time.Now().Add(-time.Hour)
	readyWithExtraStale.cacheMu.Unlock()
	if readyWithExtraStale.Ready() {
		t.Fatalf("Ready() = true with stale extra required module")
	}
}

func TestRefreshFailureBranches(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, []string{
		"vs_inventory", "vs_metrics", "pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	}), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusInternalServerError)
	})
	if err := exp.RefreshOnce(nil); err == nil {
		t.Fatalf("RefreshOnce(nil) succeeded with failing modules")
	}

	wildcard, _ := newEdgeExporter(t, testConfig([]string{"*"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tenant" {
			http.Error(w, "tenant discovery unavailable", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	})
	if tenants, err := wildcard.refreshTenants(context.Background()); err == nil || len(tenants) != 0 {
		t.Fatalf("refreshTenants fallback = %#v, %v; want no implicit tenant plus error", tenants, err)
	}
	if err := wildcard.RefreshOnce(context.Background()); err == nil {
		t.Fatalf("wildcard RefreshOnce succeeded with failed discovery")
	}

	disabledSE, _ := newEdgeExporter(t, testConfig([]string{"admin"}, []string{"se_inventory", "se_metrics"}), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	if err := disabledSE.refreshServiceEngines(context.Background()); err != nil {
		t.Fatalf("disabled refreshServiceEngines: %v", err)
	}

	failingSEMetrics, _ := newEdgeExporter(t, testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics",
		"vs_inventory", "vs_metrics", "pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	}), func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/serviceengine-inventory":
			writeFullSEInventory(t, w)
		case "/api/analytics/metrics/collection":
			http.Error(w, "analytics unavailable", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	})
	if err := failingSEMetrics.refreshServiceEngines(context.Background()); err == nil {
		t.Fatalf("refreshServiceEngines succeeded with failed SE analytics")
	}

	emptyTenants, _ := newEdgeExporter(t, testConfig([]string{"admin"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	if err := emptyTenants.refreshTenantSet(context.Background(), nil); err != nil {
		t.Fatalf("empty refreshTenantSet: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := emptyTenants.refreshTenantSet(ctx, []string{"admin"}); err == nil {
		t.Fatalf("canceled refreshTenantSet succeeded")
	}
}

func TestRefreshTenantDependencyFailures(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, []string{"cluster", "controller_metrics", "se_inventory", "se_metrics"}), func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/virtualservice-inventory", "/api/pool-inventory", "/api/vsvip-inventory", "/api/gslbservice-inventory":
			http.Error(w, "unavailable", http.StatusInternalServerError)
		case "/api/poolgroup-inventory":
			writeJSON(t, w, map[string]any{"results": []map[string]any{}})
		default:
			http.NotFound(w, r)
		}
	})
	if err := exp.refreshTenant(context.Background(), "admin"); err == nil {
		t.Fatalf("refreshTenant succeeded with failed dependencies")
	}

	missingMembers, _ := newEdgeExporter(t, testConfig([]string{"admin"}, []string{
		"cluster", "controller_metrics", "se_inventory", "se_metrics",
		"vs_metrics", "pool_metrics", "pool_group", "gslb",
	}), func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/virtualservice-inventory":
			writeVSInventory(t, w, "admin", "OPER_UP")
		case "/api/pool-inventory":
			writePoolInventory(t, w, "pool-a")
		case "/api/vsvip-inventory":
			writeVsVipInventory(t, w)
		case "/api/pool/pool-a/runtime/server/detail/":
			http.Error(w, "pool detail unavailable", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	})
	if err := missingMembers.refreshTenant(context.Background(), "admin"); err == nil {
		t.Fatalf("refreshTenant succeeded with missing pool member dependency")
	}
}

func TestPoolMemberWrapperAndAnalyticsEdges(t *testing.T) {
	pool := avi.PoolInventoryItem{
		Config:  avi.PoolConfig{UUID: "pool-a", Name: "pool-a"},
		Runtime: avi.PoolRuntime{OperStatus: avi.OperStatus{State: "OPER_UP"}},
	}
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, nil), func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/pool/pool-a/runtime/server/detail/":
			writePoolRuntimeDetail(t, w, "10.0.0.1", 80)
		case "/api/analytics/prometheus-metrics/pool":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("# Successfully gathered 0 metrics for pool\n"))
		default:
			http.NotFound(w, r)
		}
	})
	if err := exp.collectPoolMembers(context.Background(), "admin", []avi.PoolInventoryItem{pool}, nil); err != nil {
		t.Fatalf("collectPoolMembers: %v", err)
	}
	if err := exp.collectPoolAnalytics(context.Background(), "admin", []avi.PoolInventoryItem{pool}, nil); err != nil {
		t.Fatalf("collectPoolAnalytics empty series: %v", err)
	}

	matchedZero, _ := newEdgeExporter(t, testConfig([]string{"admin"}, nil), func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/analytics/prometheus-metrics/pool" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(`avi_l4_server_avg_bandwidth{uuid="unknown",type="pool",tenant="admin",name="unknown"} 1` + "\n"))
			return
		}
		http.NotFound(w, r)
	})
	if err := matchedZero.collectPoolAnalytics(context.Background(), "admin", []avi.PoolInventoryItem{pool}, nil); err != nil {
		t.Fatalf("collectPoolAnalytics unmatched series: %v", err)
	}
	if got := matchedZero.poolGaugeFor("unknown.metric"); got != nil {
		t.Fatalf("poolGaugeFor unknown = %#v, want nil", got)
	}

	emptyData, _ := newEdgeExporter(t, testConfig([]string{"admin"}, nil), func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/analytics/prometheus-metrics/pool" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("# Successfully gathered 0 metrics for pool\n"))
			return
		}
		http.NotFound(w, r)
	})
	if err := emptyData.collectPoolAnalytics(context.Background(), "admin", []avi.PoolInventoryItem{pool}, nil); err != nil {
		t.Fatalf("collectPoolAnalytics empty matching data: %v", err)
	}

	failing, _ := newEdgeExporter(t, testConfig([]string{"admin"}, nil), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusInternalServerError)
	})
	if err := failing.collectPoolMembers(context.Background(), "admin", []avi.PoolInventoryItem{pool}, nil); err == nil {
		t.Fatalf("collectPoolMembers succeeded with pool detail error")
	}
	if err := failing.collectPoolAnalytics(context.Background(), "admin", []avi.PoolInventoryItem{pool}, nil); err == nil {
		t.Fatalf("collectPoolAnalytics succeeded with analytics error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := failing.collectPoolMemberDetails(ctx, "admin", nil); err == nil {
		t.Fatalf("collectPoolMemberDetails succeeded with canceled context")
	}
	if _, err := failing.collectPoolMemberDetails(ctx, "admin", []avi.PoolInventoryItem{pool}); err == nil {
		t.Fatalf("collectPoolMemberDetails with items succeeded with canceled context")
	}
}

func TestFanoutCancellationWhileWaitingForSemaphore(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	exp.parallelism = 0
	ctx, cancel := context.WithCancel(context.Background())
	tenantDone := make(chan error, 1)
	go func() {
		tenantDone <- exp.refreshTenantSet(ctx, []string{"admin"})
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	if err := <-tenantDone; err == nil {
		t.Fatalf("refreshTenantSet succeeded after semaphore wait cancellation")
	}

	pool := avi.PoolInventoryItem{Config: avi.PoolConfig{UUID: "pool-a", Name: "pool-a"}}
	ctx, cancel = context.WithCancel(context.Background())
	poolDone := make(chan error, 1)
	go func() {
		_, err := exp.collectPoolMemberDetails(ctx, "admin", []avi.PoolInventoryItem{pool})
		poolDone <- err
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	if err := <-poolDone; err == nil {
		t.Fatalf("collectPoolMemberDetails succeeded after semaphore wait cancellation")
	}
}

func TestAnalyticsAndClusterErrorBranches(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, nil), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusInternalServerError)
	})
	if err := exp.collectControllerAnalytics(context.Background()); err == nil {
		t.Fatalf("collectControllerAnalytics succeeded with server error")
	}
	if err := exp.collectVSAnalytics(context.Background(), "admin", []avi.VSInventoryItem{{Config: avi.VSConfig{UUID: "vs-a", Name: "vs-a"}}}, nil); err == nil {
		t.Fatalf("collectVSAnalytics succeeded with server error")
	}
	if err := exp.collectSEAnalytics(context.Background(), []avi.SEInventoryItem{{Config: avi.SEConfig{UUID: "se-a", Name: "se-a"}}}, nil); err == nil {
		t.Fatalf("collectSEAnalytics succeeded with server error")
	}
	if err := exp.collectCluster(context.Background(), nil); err == nil {
		t.Fatalf("collectCluster succeeded with server error")
	}
}

func TestTopologyAndHelperEdges(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	exp.emitOperStatusInfo(exp.seOperStatusInfo, exp.appendLabels("se", "se-a"), "")

	exp.collectTopology("admin",
		[]avi.VSInventoryItem{{
			Config:      avi.VSConfig{UUID: "vs-partial", Name: "vs-partial", PoolRef: "pool-partial", Markers: []avi.Marker{{Key: "Namespace", Values: []string{"ns"}}, {Key: "IngressName", Values: []string{"ing"}}}},
			Runtime:     avi.VSRuntime{OperStatus: avi.OperStatus{State: "OPER_PARTIAL_UP"}, PercentSesUp: 50},
			HealthScore: avi.HealthScore{HealthScore: 50},
		}},
		[]avi.PoolInventoryItem{{
			Config:      avi.PoolConfig{UUID: "pool-partial", Name: "pool-partial"},
			Runtime:     avi.PoolRuntime{OperStatus: avi.OperStatus{State: "OPER_PARTIAL_UP"}, NumServers: 2, NumServersUp: 1},
			HealthScore: avi.HealthScore{HealthScore: 50},
		}},
		[]avi.VsVipInventoryItem{{
			Config: avi.VsVipConfig{
				UUID:            "vip-partial",
				Name:            "vip-partial",
				Vip:             []avi.Vip{{VipID: "1", IPAddress: &avi.IPAddr{Addr: "192.0.2.1"}}},
				VirtualServices: []avi.VsRef{{UUID: "vs-partial"}},
			},
			Runtime: []avi.VsVipRuntime{
				{VipID: "1", OperStatus: avi.OperStatus{State: "OPER_UP"}},
				{VipID: "2", OperStatus: avi.OperStatus{State: "OPER_DOWN"}},
			},
		}},
		nil,
	)
	if got := chainFor(avi.MarkerInfo{Namespace: "ns", IngressName: "ing"}, "fallback"); got != "ns/ing" {
		t.Fatalf("chainFor ingress = %q", got)
	}
}
