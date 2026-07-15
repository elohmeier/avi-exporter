package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

func TestFullRefreshCoversAllModules(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/cluster/runtime":
			writeFullClusterRuntime(t, w)
		case "/api/cluster":
			writeJSON(t, w, map[string]any{
				"uuid": "cluster-a", "name": "avi-a",
				"nodes": []map[string]any{{
					"name": "node-a", "ip": map[string]any{"addr": "192.0.2.10", "type": "V4"},
					"vm_hostname": "controller-a", "vm_name": "avi-controller-a", "vm_uuid": "vm-a",
				}},
			})
		case "/api/analytics/metrics/collection":
			writeFullAnalytics(t, w, r)
		case "/api/analytics/prometheus-metrics/serviceengine":
			writeFullSEPrometheusMetrics(t, w)
		case "/api/analytics/prometheus-metrics/virtualservice":
			writeFullVSPrometheusMetrics(t, w)
		case "/api/analytics/prometheus-metrics/pool":
			writeFullPoolPrometheusMetrics(t, w)
		case "/api/serviceengine-inventory":
			writeFullSEInventory(t, w)
		case "/api/serviceengine":
			writeJSON(t, w, map[string]any{
				"count": 1,
				"results": []map[string]any{{
					"uuid": "se-a", "name": "se-a", "controller_ip": "192.0.2.10",
					"cloud_ref":    "https://controller/api/cloud/cloud-a#cloud-a",
					"tenant_ref":   "https://controller/api/tenant/admin-uuid#admin",
					"se_group_ref": "https://controller/api/serviceenginegroup/seg-a#seg-a",
					"mgmt_vnic": map[string]any{
						"if_name": "Management", "network_ref": "https://controller/api/network/net-mgmt#management",
						"vnic_networks": []map[string]any{{"ip": map[string]any{"ip_addr": map[string]any{"addr": "192.0.2.20", "type": "V4"}, "mask": 24}}},
					},
				}},
			})
		case "/api/virtualservice-inventory":
			writeFullVSInventory(t, w)
		case "/api/pool-inventory":
			writeFullPoolInventory(t, w)
		case "/api/pool/pool-a/runtime/server/detail/":
			writeJSON(t, w, []map[string]any{
				{"ip_addr": map[string]any{"addr": "10.0.0.10", "type": "V4"}, "port": 8080, "oper_status": map[string]any{"state": "OPER_UP"}},
				{"ip_addr": map[string]any{"addr": "10.0.0.11", "type": "V4"}, "port": 8081, "oper_status": map[string]any{"state": "OPER_DOWN"}},
			})
		case "/api/pool/pool-b/runtime/server/detail/":
			writeJSON(t, w, []map[string]any{})
		case "/api/vsvip-inventory":
			writeFullVsVipInventory(t, w)
		case "/api/poolgroup-inventory":
			writeFullPoolGroupInventory(t, w)
		case "/api/gslbservice-inventory":
			writeFullGslbInventory(t, w)
		default:
			http.NotFound(w, r)
		}
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 2, logger)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	mfs := gatherRegisteredExporter(t, exp)
	if got := firstGaugeValue(t, metricFamily(t, mfs, "avi_cluster_up")); got != 1 {
		t.Fatalf("avi_cluster_up = %v, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_cluster_node_info"), "vm_uuid", "vm-a"); got != 1 {
		t.Fatalf("cluster identity metric count = %d, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_se_address_info"), "ip", "192.0.2.20"); got != 1 {
		t.Fatalf("SE address metric count = %d, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_se_oper_up"), "se_uuid", "se-a"); got != 1 {
		t.Fatalf("SE metrics for se-a = %d, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_se_avg_cpu_usage"), "se_uuid", "se-a"); got != 1 {
		t.Fatalf("SE analytics for se-a = %d, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_vs_l4_client_avg_bandwidth_bps"), "vs_uuid", "vs-a"); got != 1 {
		t.Fatalf("VS analytics for vs-a = %d, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_pool_member_oper_up"), "ip", "10.0.0.10"); got != 1 {
		t.Fatalf("pool member metric count = %d, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_vip_dns_record_info"), "fqdn", "app.example.com"); got != 1 {
		t.Fatalf("VIP DNS metric count = %d, want 1", got)
	}
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_gslb_service_domain_info"), "fqdn", "global.example.com"); got != 1 {
		t.Fatalf("GSLB domain metric count = %d, want 1", got)
	}
	if !exp.Ready() {
		t.Fatalf("Ready() = false after full successful refresh")
	}
	ready := httptest.NewRecorder()
	exp.ReadyHandler(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ReadyHandler code = %d, want 200", ready.Code)
	}
	debug := httptest.NewRecorder()
	exp.DebugCacheHandler(debug, httptest.NewRequest(http.MethodGet, "/debug/cache", nil))
	if debug.Code != http.StatusOK {
		t.Fatalf("DebugCacheHandler code = %d, want 200", debug.Code)
	}
	if got := debug.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("DebugCacheHandler content type = %q", got)
	}
}

func TestCollectClusterLegacyPath(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/cluster/runtime" {
			http.NotFound(w, r)
			return
		}
		writeFullClusterRuntime(t, w)
	})
	defer controller.Close()

	exp, err := NewExporter(testConfig([]string{"admin"}, nil), controller.URL, "user", "pass", true, "", 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	ch := make(chan prometheus.Metric, 8)
	if err := exp.collectCluster(context.Background(), ch); err != nil {
		t.Fatalf("collectCluster: %v", err)
	}
	close(ch)
	if got := len(ch); got == 0 {
		t.Fatalf("collectCluster emitted no metrics")
	}
}

func writeFullClusterRuntime(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"cluster_state": map[string]any{"state": "CLUSTER_UP_HA_ACTIVE", "progress": 99},
		"node_states": []map[string]any{
			{"name": "node-a", "state": "CLUSTER_ACTIVE", "role": "CLUSTER_LEADER"},
			{"name": "node-b", "state": "CLUSTER_INACTIVE", "role": "CLUSTER_FOLLOWER"},
		},
	})
}

func writeFullAnalytics(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	var req avi.MetricsCollectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Fatalf("decode analytics request: %v", err)
	}

	series := map[string][]map[string]any{}
	add := func(key, entityUUID, name string, values ...float64) {
		data := make([]map[string]any, 0, len(values))
		for _, v := range values {
			data = append(data, map[string]any{"timestamp": "2026-01-01T00:00:00Z", "value": v})
		}
		series[key] = append(series[key], map[string]any{
			"header": map[string]any{"name": name, "entity_uuid": entityUUID},
			"data":   data,
		})
	}

	for i, q := range req.MetricRequests {
		value := float64(i + 1)
		switch q.MetricEntity {
		case avi.EntityController:
			add("controller-a", "controller-a", q.MetricID, value)
		case avi.EntitySE:
			add("se-a", "se-a", q.MetricID, value)
		case avi.EntityVS:
			if q.PoolUUID != "" {
				add(q.EntityUUID, q.EntityUUID, q.MetricID, value)
			} else {
				add("vs-a", "vs-a", q.MetricID, value)
			}
		default:
			add("unknown", "unknown", q.MetricID, value)
		}
	}

	add("controller-a", "controller-a", "controller_stats.unknown", 1)
	add("controller-a", "controller-a", "controller_stats.avg_cpu_usage")
	add("se-a", "se-a", "se_stats.unknown", 1)
	add("se-a", "se-a", "se_stats.avg_cpu_usage")
	add("unknown-se", "unknown-se", "se_stats.avg_cpu_usage", 1)
	add("vs-a", "vs-a", "vs_stats.unknown", 1)
	add("vs-a", "vs-a", "l4_client.avg_bandwidth")
	add("unknown-vs", "unknown-vs", "l4_client.avg_bandwidth", 1)
	add("pool-a-keyed-by-vs", "pool-a", "l4_server.avg_complete_conns", 1)
	add("unknown-pool", "unknown-pool", "l4_server.avg_bandwidth", 1)

	writeJSON(t, w, map[string]any{"series": series})
}

func writeFullSEInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"results": []map[string]any{
			{
				"config": map[string]any{"uuid": "se-a", "name": "se-a", "enable_state": "SE_STATE_ENABLED"},
				"runtime": map[string]any{
					"oper_status":            map[string]any{"state": "OPER_UP"},
					"se_connected":           true,
					"bgp_peers_up":           true,
					"gateway_up":             true,
					"at_curr_ver":            true,
					"sufficient_memory":      true,
					"licensed_service_cores": 4,
					"license_state":          "LICENSE_VALID",
					"power_state":            "POWERED_ON",
					"migrate_state":          "MIGRATE_NONE",
					"version":                "30.2.1",
				},
				"health_score": map[string]any{"health_score": 98},
			},
			{
				"config":       map[string]any{"uuid": "se-b", "name": "se-b", "enable_state": "SE_STATE_DISABLED"},
				"runtime":      map[string]any{"oper_status": map[string]any{"state": "OPER_DOWN"}},
				"health_score": map[string]any{"health_score": 20},
			},
		},
	})
}

func writeFullSEPrometheusMetrics(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprintln(w, "# Successfully gathered 2 metrics for serviceengine")
	_, _ = fmt.Fprintln(w, `avi_se_stats_avg_cpu_usage{uuid="se-a",type="serviceengine",tenant="admin",name="se-a"} 12`)
	_, _ = fmt.Fprintln(w, `avi_se_stats_avg_mem_usage{uuid="se-a",type="serviceengine",tenant="admin",name="se-a"} 34`)
}

func writeFullVSPrometheusMetrics(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprintln(w, "# Successfully gathered 2 metrics for virtualservice")
	_, _ = fmt.Fprintln(w, `avi_l4_client_avg_bandwidth{uuid="vs-a",type="virtualservice",tenant="admin",name="vs-a"} 123`)
	_, _ = fmt.Fprintln(w, `avi_dns_client_avg_complete_queries{uuid="vs-a",type="virtualservice",tenant="admin",name="vs-a"} 456`)
}

func writeFullPoolPrometheusMetrics(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprintln(w, "# Successfully gathered 2 metrics for pool")
	_, _ = fmt.Fprintln(w, `avi_l4_server_avg_bandwidth{uuid="pool-a",type="pool",tenant="admin",name="pool-a"} 789`)
	_, _ = fmt.Fprintln(w, `avi_l7_server_avg_resp_5xx{uuid="pool-a",type="pool",tenant="admin",name="pool-a"} 3`)
}

func writeFullVSInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid":           "vs-a",
					"name":           "vs-a",
					"enabled":        true,
					"type":           "VS_TYPE_NORMAL",
					"created_by":     "ako-cluster",
					"markers":        akoMarkers(),
					"pool_ref":       "https://controller/api/pool/pool-a#pool-a",
					"pool_group_ref": "https://controller/api/poolgroup/pg-a#pg-a",
					"vip": []map[string]any{
						{"vip_id": "1", "ip_address": map[string]any{"addr": "192.0.2.10", "type": "V4"}},
					},
				},
				"runtime": map[string]any{
					"oper_status":    map[string]any{"state": "OPER_UP"},
					"percent_ses_up": 100,
					"vip_summary": []map[string]any{
						{"vip_id": "1", "oper_status": map[string]any{"state": "OPER_UP"}, "percent_ses_up": 100, "num_se_assigned": 1, "num_se_requested": 1},
					},
				},
				"health_score": map[string]any{"health_score": 97},
				"alert":        map[string]any{"level": "HIGH"},
			},
			{
				"config": map[string]any{
					"uuid":    "vs-b",
					"name":    "vs-b",
					"type":    "VS_TYPE_VH_CHILD",
					"vip":     []map[string]any{{"vip_id": "2", "ip6_address": map[string]any{"addr": "2001:db8::10", "type": "V6"}}},
					"markers": []map[string]any{{"key": "Host", "values": []string{"host.example.com"}}},
				},
				"runtime": map[string]any{
					"oper_status":    map[string]any{"state": "OPER_PARTIAL_UP"},
					"percent_ses_up": 50,
					"vip_summary":    []map[string]any{{"vip_id": "2", "oper_status": map[string]any{"state": "OPER_DOWN"}}},
				},
				"health_score": map[string]any{"health_score": 50},
			},
		},
	})
}

func writeFullPoolInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"results": []map[string]any{
			{
				"config": map[string]any{"uuid": "pool-a", "name": "pool-a", "enabled": true, "created_by": "ako-cluster", "markers": akoMarkers()},
				"runtime": map[string]any{
					"oper_status":                map[string]any{"state": "OPER_UP"},
					"num_servers":                2,
					"num_servers_up":             1,
					"num_servers_enabled":        2,
					"percent_servers_up_enabled": 50,
					"percent_servers_up_total":   50,
				},
				"health_score":      map[string]any{"health_score": 88},
				"alert":             map[string]any{"level": "LOW"},
				"app_profile_type":  "APPLICATION_PROFILE_TYPE_HTTP",
				"virtualservices":   []map[string]any{{"uuid": "vs-a"}},
				"tenant_ref":        "admin",
				"cloud_ref":         "cloud-a",
				"gslb_sp_enabled":   true,
				"vrf_ref":           "vrf-a",
				"created_by":        "ako-cluster",
				"health_score_uuid": "ignored",
			},
			{
				"config":       map[string]any{"uuid": "pool-b", "name": "pool-b"},
				"runtime":      map[string]any{"oper_status": map[string]any{"state": "OPER_DOWN"}},
				"health_score": map[string]any{"health_score": 10},
			},
		},
	})
}

func writeFullVsVipInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid":       "vsvip-a",
					"name":       "vsvip-a",
					"created_by": "ako-cluster",
					"markers":    akoMarkers(),
					"vip": []map[string]any{
						{
							"vip_id":                    "1",
							"enabled":                   true,
							"ip_address":                map[string]any{"addr": "192.0.2.10", "type": "V4"},
							"floating_ip":               map[string]any{"addr": "198.51.100.10", "type": "V4"},
							"avi_allocated_vip":         true,
							"auto_allocate_floating_ip": true,
						},
						{
							"vip_id":           "2",
							"enabled":          false,
							"ip6_address":      map[string]any{"addr": "2001:db8::20", "type": "V6"},
							"floating_ip6":     map[string]any{"addr": "2001:db8::30", "type": "V6"},
							"auto_allocate_ip": true,
						},
					},
					"virtualservices": []map[string]any{{"uuid": "vs-a"}, {"uuid": "vs-b"}},
					"dns_info":        []map[string]any{{"fqdn": "app.example.com", "type": "DNS_RECORD_A", "ttl": 60}},
				},
				"runtime": []map[string]any{
					{
						"vip_id":           "1",
						"oper_status":      map[string]any{"state": "OPER_UP"},
						"percent_ses_up":   100,
						"num_se_assigned":  1,
						"num_se_requested": 1,
						"service_engine": []map[string]any{
							{"name": "se-a", "url": "https://controller/api/serviceengine/se-a#se-a", "primary": true, "active_on_se": true},
							{"name": "se-b", "url": "https://controller/api/serviceengine/se-b#se-b", "primary": false, "active_on_se": false},
						},
					},
				},
			},
			{
				"config":  map[string]any{"uuid": "vsvip-b", "name": "vsvip-b", "vip": []map[string]any{{"vip_id": "1"}}},
				"runtime": []map[string]any{{"vip_id": "1", "oper_status": map[string]any{"state": "OPER_DOWN"}}},
			},
		},
	})
}

func writeFullPoolGroupInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid":       "pg-a",
					"name":       "pg-a",
					"created_by": "ako-cluster",
					"markers":    akoMarkers(),
					"members": []map[string]any{
						{"pool_ref": "https://controller/api/pool/pool-a#pool-a"},
						{"pool_ref": ""},
					},
				},
			},
		},
	})
}

func writeFullGslbInventory(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	writeJSON(t, w, map[string]any{
		"results": []map[string]any{
			{
				"config": map[string]any{
					"uuid":         "gslb-a",
					"name":         "gslb-a",
					"enabled":      true,
					"domain_names": []string{"global.example.com"},
					"groups":       []map[string]any{{"name": "group-a", "enabled": true}},
				},
				"runtime": map[string]any{"oper_status": map[string]any{"state": "OPER_UP"}},
			},
			{
				"config":  map[string]any{"uuid": "gslb-b", "name": "gslb-b"},
				"runtime": map[string]any{"oper_status": map[string]any{"state": "OPER_DOWN"}},
			},
		},
	})
}

func akoMarkers() []map[string]any {
	return []map[string]any{
		{"key": "Namespace", "values": []string{"ns"}},
		{"key": "ServiceName", "values": []string{"svc"}},
		{"key": "IngressName", "values": []string{"ing"}},
		{"key": "Host", "values": []string{"app.example.com"}},
	}
}
