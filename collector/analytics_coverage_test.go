package collector

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestAnalyticsMetricIDsCoverLegacyVectorScrape(t *testing.T) {
	legacy := map[string][]string{
		"controller": {
			"controller_stats.avg_cpu_usage",
			"controller_stats.avg_disk_read_bytes",
			"controller_stats.avg_disk_usage",
			"controller_stats.avg_disk_write_bytes",
			"controller_stats.avg_mem_usage",
			"controller_stats.avg_num_active_vs",
			"controller_stats.avg_num_backend_servers",
		},
		"serviceengine": {
			"se_if.avg_bandwidth",
			"se_if.avg_connection_table_usage",
			"se_if.avg_rx_bytes",
			"se_if.avg_rx_pkts",
			"se_if.avg_rx_pkts_dropped_non_vs",
			"se_if.avg_tx_bytes",
			"se_if.avg_tx_pkts",
			"se_stats.avg_connection_mem_usage",
			"se_stats.avg_connections",
			"se_stats.avg_connections_dropped",
			"se_stats.avg_cpu_usage",
			"se_stats.avg_disk1_usage",
			"se_stats.avg_dynamic_mem_usage",
			"se_stats.avg_mem_usage",
			"se_stats.avg_packet_buffer_header_usage",
			"se_stats.avg_packet_buffer_large_usage",
			"se_stats.avg_packet_buffer_small_usage",
			"se_stats.avg_packet_buffer_usage",
			"se_stats.avg_persistent_table_usage",
			"se_stats.avg_rx_bandwidth",
			"se_stats.avg_ssl_session_cache_usage",
			"se_stats.max_se_bandwidth",
			"se_stats.pct_syn_cache_usage",
			"healthscore.health_score_value",
		},
		"pool": {
			"l4_server.apdexc",
			"l4_server.avg_bandwidth",
			"l4_server.avg_errored_connections",
			"l4_server.avg_new_established_conns",
			"l4_server.avg_open_conns",
			"l4_server.avg_pool_complete_conns",
			"l4_server.avg_pool_open_conns",
			"l4_server.avg_rx_bytes",
			"l4_server.avg_rx_pkts",
			"l4_server.avg_total_rtt",
			"l4_server.avg_tx_bytes",
			"l4_server.avg_tx_pkts",
			"l4_server.max_open_conns",
			"l7_server.apdexr",
			"l7_server.avg_application_response_time",
			"l7_server.avg_complete_responses",
			"l7_server.avg_frustrated_responses",
			"l7_server.avg_resp_4xx",
			"l7_server.avg_resp_5xx",
			"l7_server.avg_resp_latency",
			"l7_server.avg_total_requests",
			"l7_server.pct_response_errors",
			"healthscore.health_score_value",
		},
		"virtualservice": {
			"dns_client.avg_avi_errors",
			"dns_client.avg_complete_queries",
			"dns_client.avg_domain_lookup_failures",
			"dns_client.avg_tcp_queries",
			"dns_client.avg_udp_passthrough_resp_time",
			"dns_client.avg_udp_queries",
			"dns_client.avg_unsupported_queries",
			"dns_client.pct_errored_queries",
			"dns_server.avg_complete_queries",
			"dns_server.avg_errored_queries",
			"dns_server.avg_tcp_queries",
			"dns_server.avg_udp_queries",
			"l4_client.apdexc",
			"l4_client.avg_application_dos_attacks",
			"l4_client.avg_bandwidth",
			"l4_client.avg_complete_conns",
			"l4_client.avg_connections_dropped",
			"l4_client.avg_lossy_connections",
			"l4_client.avg_new_established_conns",
			"l4_client.avg_policy_drops",
			"l4_client.avg_rx_bytes",
			"l4_client.avg_rx_pkts",
			"l4_client.avg_total_rtt",
			"l4_client.avg_tx_bytes",
			"l4_client.avg_tx_pkts",
			"l4_client.max_open_conns",
			"l4_server.apdexc",
			"l4_server.avg_bandwidth",
			"l4_server.avg_errored_connections",
			"l4_server.avg_new_established_conns",
			"l4_server.avg_open_conns",
			"l4_server.avg_pool_complete_conns",
			"l4_server.avg_pool_open_conns",
			"l4_server.avg_rx_bytes",
			"l4_server.avg_rx_pkts",
			"l4_server.avg_total_rtt",
			"l4_server.avg_tx_bytes",
			"l4_server.avg_tx_pkts",
			"l4_server.max_open_conns",
			"l7_client.apdexr",
			"l7_client.avg_client_data_transfer_time",
			"l7_client.avg_client_txn_latency",
			"l7_client.avg_complete_responses",
			"l7_client.avg_frustrated_responses",
			"l7_client.avg_http_headers_bytes",
			"l7_client.avg_http_headers_count",
			"l7_client.avg_http_params_count",
			"l7_client.avg_page_load_time",
			"l7_client.avg_post_bytes",
			"l7_client.avg_resp_2xx",
			"l7_client.avg_resp_4xx",
			"l7_client.avg_resp_4xx_avi_errors",
			"l7_client.avg_resp_5xx",
			"l7_client.avg_resp_5xx_avi_errors",
			"l7_client.avg_ssl_connections",
			"l7_client.avg_ssl_handshakes_new",
			"l7_client.avg_uri_length",
			"l7_client.avg_waf_attacks",
			"l7_client.avg_waf_disabled",
			"l7_client.avg_waf_evaluated",
			"l7_client.avg_waf_matched",
			"l7_client.avg_waf_rejected",
			"l7_client.pct_get_reqs",
			"l7_client.pct_post_reqs",
			"l7_client.pct_waf_attacks",
			"l7_client.pct_waf_disabled",
			"l7_client.sum_application_response_time",
			"l7_client.sum_get_reqs",
			"l7_client.sum_other_reqs",
			"l7_client.sum_post_reqs",
			"l7_client.sum_total_responses",
			"l7_server.apdexr",
			"l7_server.avg_application_response_time",
			"l7_server.avg_complete_responses",
			"l7_server.avg_frustrated_responses",
			"l7_server.avg_resp_latency",
			"l7_server.avg_total_requests",
			"l7_server.pct_response_errors",
			"healthscore.health_score_value",
		},
	}

	assertMetricIDSubset(t, "controller", legacy["controller"], controllerMetricIDs)
	assertMetricIDSubset(t, "serviceengine", legacy["serviceengine"], seMetricIDs)
	assertMetricIDSubset(t, "pool", legacy["pool"], poolMetricIDs)
	assertMetricIDSubset(t, "virtualservice", legacy["virtualservice"], vsMetricIDs)
}

func TestLegacyAnalyticsMetricIDsHaveGauges(t *testing.T) {
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	defer controller.Close()

	exp, err := NewExporter(testConfig([]string{"admin"}, nil), controller.URL, "user", "pass", true, "", 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}

	for _, tc := range []struct {
		name   string
		id     string
		family string
		gauge  func(string) *prometheus.GaugeVec
	}{
		{name: "VS DNS", id: "dns_client.avg_complete_queries", family: "avi_dns_client_avg_complete_queries", gauge: func(s string) *prometheus.GaugeVec { return exp.vsGaugeFor(s) }},
		{name: "pool response code", id: "l7_server.avg_resp_5xx", family: "avi_l7_server_avg_resp_5xx", gauge: func(s string) *prometheus.GaugeVec { return exp.poolGaugeFor(s) }},
		{name: "SE packet buffer", id: "se_stats.avg_packet_buffer_large_usage", family: "avi_se_stats_avg_packet_buffer_large_usage", gauge: func(s string) *prometheus.GaugeVec { return exp.seGaugeFor(s) }},
	} {
		if tc.gauge(tc.id) == nil {
			t.Fatalf("%s metric ID %q has no gauge", tc.name, tc.id)
		}
		if tc.gauge(tc.family) == nil {
			t.Fatalf("%s prometheus family %q has no gauge", tc.name, tc.family)
		}
	}
}

func assertMetricIDSubset(t *testing.T, name string, want, got []string) {
	t.Helper()
	have := make(map[string]bool, len(got))
	for _, id := range got {
		have[id] = true
	}
	for _, id := range want {
		if !have[id] {
			t.Fatalf("%s metric IDs missing legacy metric %q", name, id)
		}
	}
}
