package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
	"github.com/elohmeier/avi-exporter/config"
)

const namespace = "avi"

// Exporter implements prometheus.Collector for an Avi controller.
type Exporter struct {
	refreshMu       sync.Mutex
	cacheMu         sync.Mutex
	startOnce       sync.Once
	schedulerCancel context.CancelFunc
	cfg             *config.Config
	url             string
	parallelism     int
	labelKeys       []string
	baseLabels      []string // cached values for labelKeys (immutable across the process)
	logger          *slog.Logger
	client          *avi.Client

	tenants              []string
	moduleStates         map[moduleKey]*moduleState
	clusterUpValue       float64
	clusterProgressValue float64
	clusterCached        bool
	refreshInterval      time.Duration
	refreshTimeout       time.Duration

	// --- Self-metrics ---
	up                *prometheus.GaugeVec
	scrapeDuration    *prometheus.GaugeVec
	scrapeErrorsTotal *prometheus.CounterVec
	scrapeTotal       *prometheus.CounterVec

	moduleLastSuccess        *prometheus.GaugeVec
	moduleLastAttempt        *prometheus.GaugeVec
	moduleAge                *prometheus.GaugeVec
	moduleStale              *prometheus.GaugeVec
	moduleRefreshDuration    *prometheus.GaugeVec
	moduleRefreshErrorsTotal *prometheus.CounterVec
	moduleRefreshTotal       *prometheus.CounterVec

	// --- Cluster (admin tenant) ---
	clusterUp        *prometheus.Desc
	clusterProgress  *prometheus.Desc
	clusterStateInfo *prometheus.GaugeVec
	clusterNodeUp    *prometheus.GaugeVec
	clusterNodeRole  *prometheus.GaugeVec

	// --- Controller analytics ---
	controllerAvgCPUUsage          *prometheus.GaugeVec
	controllerAvgMemUsage          *prometheus.GaugeVec
	controllerAvgDiskUsage         *prometheus.GaugeVec
	controllerAvgDiskReadBytes     *prometheus.GaugeVec
	controllerAvgDiskWriteBytes    *prometheus.GaugeVec
	controllerAvgNumActiveVS       *prometheus.GaugeVec
	controllerAvgNumBackendServers *prometheus.GaugeVec

	// --- Virtual Service inventory ---
	vsOperUp         *prometheus.GaugeVec
	vsOperStatusInfo *prometheus.GaugeVec
	vsEnabled        *prometheus.GaugeVec
	vsHealthScore    *prometheus.GaugeVec
	vsPercentSesUp   *prometheus.GaugeVec
	vsTypeInfo       *prometheus.GaugeVec
	vsAlertLevel     *prometheus.GaugeVec

	// VS vip_summary[] — per-VIP runtime from VS inventory (separate from /vsvip-inventory)
	vsVipOperUp         *prometheus.GaugeVec
	vsVipPercentSesUp   *prometheus.GaugeVec
	vsVipNumSeAssigned  *prometheus.GaugeVec
	vsVipNumSeRequested *prometheus.GaugeVec
	vsVipOperStatusInfo *prometheus.GaugeVec

	// --- VS analytics ---
	vsAvgBandwidth       *prometheus.GaugeVec
	vsAvgCompleteConns   *prometheus.GaugeVec
	vsAvgNewEstabConns   *prometheus.GaugeVec
	vsMaxOpenConns       *prometheus.GaugeVec
	vsConnectionsDropped *prometheus.GaugeVec
	vsAvgTotalRequests   *prometheus.GaugeVec
	vsAvgCompleteResp    *prometheus.GaugeVec
	vsAvgErrorResp       *prometheus.GaugeVec
	vsAvgResp2xx         *prometheus.GaugeVec
	vsAvgResp4xx         *prometheus.GaugeVec
	vsAvgResp5xx         *prometheus.GaugeVec
	vsApdexR             *prometheus.GaugeVec
	vsAvgClientRTT       *prometheus.GaugeVec
	vsAvgRespLatency     *prometheus.GaugeVec
	vsAnalyticsGauges    map[string]*prometheus.GaugeVec

	// --- Pool inventory ---
	poolOperUp                  *prometheus.GaugeVec
	poolOperStatusInfo          *prometheus.GaugeVec
	poolEnabled                 *prometheus.GaugeVec
	poolHealthScore             *prometheus.GaugeVec
	poolNumServers              *prometheus.GaugeVec
	poolNumServersUp            *prometheus.GaugeVec
	poolNumServersEnabled       *prometheus.GaugeVec
	poolPercentServersUpEnabled *prometheus.GaugeVec
	poolPercentServersUpTotal   *prometheus.GaugeVec
	poolAlertLevel              *prometheus.GaugeVec
	poolAppProfileType          *prometheus.GaugeVec

	// --- Pool members (via /api/pool/<uuid>/runtime/server/detail/) ---
	poolMemberOperUp         *prometheus.GaugeVec
	poolMemberOperStatusInfo *prometheus.GaugeVec

	// --- Pool analytics ---
	poolAvgBandwidth     *prometheus.GaugeVec
	poolAvgCompleteConns *prometheus.GaugeVec
	poolAvgOpenConns     *prometheus.GaugeVec
	poolAvgTotalRTT      *prometheus.GaugeVec
	poolAvgRespLatency   *prometheus.GaugeVec
	poolAvgErrorResp     *prometheus.GaugeVec
	poolAvgHealthStatus  *prometheus.GaugeVec
	poolAvgUptime        *prometheus.GaugeVec
	poolAnalyticsGauges  map[string]*prometheus.GaugeVec

	// --- Pool Group ---
	poolGroupInfo        *prometheus.GaugeVec
	poolGroupMemberCount *prometheus.GaugeVec

	// --- GSLB Service ---
	gslbServiceOperUp         *prometheus.GaugeVec
	gslbServiceOperStatusInfo *prometheus.GaugeVec
	gslbServiceEnabled        *prometheus.GaugeVec
	gslbServiceMemberCount    *prometheus.GaugeVec
	gslbServiceDomainsInfo    *prometheus.GaugeVec

	// --- Service Engine inventory ---
	seOperUp          *prometheus.GaugeVec
	seOperStatusInfo  *prometheus.GaugeVec
	seEnabled         *prometheus.GaugeVec
	seHealthScore     *prometheus.GaugeVec
	seConnected       *prometheus.GaugeVec
	seBgpPeersUp      *prometheus.GaugeVec
	seGatewayUp       *prometheus.GaugeVec
	seAtCurrVer       *prometheus.GaugeVec
	seSufficientMem   *prometheus.GaugeVec
	seLicensedCores   *prometheus.GaugeVec
	seLicenseState    *prometheus.GaugeVec
	sePowerState      *prometheus.GaugeVec
	seMigrateState    *prometheus.GaugeVec
	seVersionInfo     *prometheus.GaugeVec
	seEnableStateInfo *prometheus.GaugeVec

	// --- SE analytics ---
	seAvgCPUUsage     *prometheus.GaugeVec
	seAvgMemUsage     *prometheus.GaugeVec
	seAvgDiskUsage    *prometheus.GaugeVec
	seAvgConnections  *prometheus.GaugeVec
	seAvgConnDropped  *prometheus.GaugeVec
	seAvgRxBytes      *prometheus.GaugeVec
	seAvgTxBytes      *prometheus.GaugeVec
	seAvgBandwidth    *prometheus.GaugeVec
	seAvgConnMem      *prometheus.GaugeVec
	sePctConnDropped  *prometheus.GaugeVec
	sePktBufUsage     *prometheus.GaugeVec
	sePersistTblUsage *prometheus.GaugeVec
	seSslSessCache    *prometheus.GaugeVec
	seAnalyticsGauges map[string]*prometheus.GaugeVec

	// --- VIP / VsVip inventory ---
	vipOperUp          *prometheus.GaugeVec
	vipOperStatusInfo  *prometheus.GaugeVec
	vipEnabled         *prometheus.GaugeVec
	vipPercentSesUp    *prometheus.GaugeVec
	vipNumSeAssigned   *prometheus.GaugeVec
	vipNumSeRequested  *prometheus.GaugeVec
	vipActiveOnSe      *prometheus.GaugeVec
	vipSharedByVsCount *prometheus.GaugeVec
	vipFloatingIP      *prometheus.GaugeVec
	vipAutoAllocated   *prometheus.GaugeVec
	vipDNSRecord       *prometheus.GaugeVec

	// --- Topology (Grafana node-graph style) ---
	topologyNode              *prometheus.GaugeVec
	topologyEdge              *prometheus.GaugeVec
	topologyNodeState         *prometheus.GaugeVec
	topologyNodeHealth        *prometheus.GaugeVec
	topologyNodeRequestsTotal *prometheus.GaugeVec
	topologyNodeConnections   *prometheus.GaugeVec
}

// NewExporter wires descriptors and constructs the underlying AVI client.
func NewExporter(cfg *config.Config, url, username, password string, ignoreCert bool, caFile string, parallelism int, logger *slog.Logger) (*Exporter, error) {
	if parallelism < 1 {
		return nil, fmt.Errorf("parallelism must be at least 1")
	}

	client, err := avi.NewClient(url, username, password, cfg.APIVersion, ignoreCert, caFile, logger)
	if err != nil {
		return nil, err
	}

	base := cfg.LabelKeys()
	baseVals := make([]string, len(base))
	for i, k := range base {
		baseVals[i] = cfg.Labels[k]
	}

	// Label sets — see appendLabels() / xxxLabelValues() helpers in each file.
	clusterLbl := append(append([]string{}, base...), "node")
	controllerLbl := append(append([]string{}, base...), "controller_uuid")
	moduleLbl := append(append([]string{}, base...), "module", "tenant")
	tenantBase := append(append([]string{}, base...), "tenant")
	vsLbl := append(append([]string{}, tenantBase...), "vs", "vs_uuid", "namespace", "service", "ingress", "host", "ako")
	vsVipLbl := append(append([]string{}, vsLbl...), "vip_id", "ip")
	poolLbl := append(append([]string{}, tenantBase...), "pool", "pool_uuid", "namespace", "service", "ingress", "host", "ako")
	poolMemberLbl := append(append([]string{}, tenantBase...), "pool", "pool_uuid", "ip", "port", "namespace", "service", "ingress", "host", "ako")
	seLbl := append(append([]string{}, base...), "se", "se_uuid")
	seAnalyticsLbl := append(append([]string{}, tenantBase...), "se", "se_uuid")
	pgLbl := append(append([]string{}, tenantBase...), "poolgroup", "poolgroup_uuid", "namespace", "service", "ingress", "host", "ako")
	gslbLbl := append(append([]string{}, tenantBase...), "gslbservice", "gslbservice_uuid")
	vipLbl := append(append([]string{}, tenantBase...), "vsvip", "vsvip_uuid", "vip_id", "ip", "namespace", "service", "ingress", "host", "ako")
	vipPlacementLbl := append(append([]string{}, tenantBase...), "vsvip", "vsvip_uuid", "vip_id", "se", "se_uuid", "primary", "namespace", "service", "ingress", "host", "ako")
	vipDNSLbl := append(append([]string{}, tenantBase...), "vsvip", "vsvip_uuid", "fqdn", "type", "ttl", "namespace", "service", "ingress", "host", "ako")
	topoNodeLbl := append(append([]string{}, tenantBase...), "id", "title", "subtitle", "node_type", "state", "chain", "mainstat", "secondarystat", "color")
	topoEdgeLbl := append(append([]string{}, tenantBase...), "id", "source", "target", "chain", "mainstat")
	topoNodeStatsLbl := append(append([]string{}, tenantBase...), "id", "node_type", "chain")

	g := func(name, help string, labels []string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help}, labels)
	}
	c := func(name, help string, labels []string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, labels)
	}

	// oper_status_info / alert / info family helpers reuse the same label
	// set as the base metric, plus a final string-valued "state" / "level" / etc.
	withTrailing := func(lbl []string, extra string) []string {
		return append(append([]string{}, lbl...), extra)
	}

	e := &Exporter{
		cfg:             cfg,
		url:             url,
		parallelism:     parallelism,
		labelKeys:       base,
		baseLabels:      baseVals,
		logger:          logger,
		client:          client,
		moduleStates:    make(map[moduleKey]*moduleState),
		refreshInterval: defaultRefreshInterval,
		refreshTimeout:  defaultRefreshTimeout,

		// Self-metrics
		up:                       g("up", "1 if the last background refresh of this tenant succeeded", append(append([]string{}, base...), "tenant")),
		scrapeDuration:           g("scrape_duration_seconds", "Compatibility alias for avi_module_refresh_duration_seconds", moduleLbl),
		scrapeErrorsTotal:        c("scrape_errors_total", "Compatibility alias for avi_module_refresh_errors_total", moduleLbl),
		scrapeTotal:              c("scrape_total", "Compatibility alias for avi_module_refresh_total", moduleLbl),
		moduleLastSuccess:        g("module_last_success_timestamp_seconds", "Unix timestamp of the last successful module refresh", moduleLbl),
		moduleLastAttempt:        g("module_last_attempt_timestamp_seconds", "Unix timestamp of the last attempted module refresh", moduleLbl),
		moduleAge:                g("module_age_seconds", "Age of the cached data currently served by module", moduleLbl),
		moduleStale:              g("module_stale", "1 when the module cache is older than its stale threshold", moduleLbl),
		moduleRefreshDuration:    g("module_refresh_duration_seconds", "Wall-clock duration of the last module refresh", moduleLbl),
		moduleRefreshErrorsTotal: c("module_refresh_errors_total", "Cumulative module refresh failures", moduleLbl),
		moduleRefreshTotal:       c("module_refresh_total", "Cumulative module refresh attempts", moduleLbl),

		// Cluster
		clusterUp:        prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "cluster_up"), "1 if the controller cluster is up and HA active", base, nil),
		clusterProgress:  prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "cluster_progress"), "Cluster startup/recovery progress 0-100", base, nil),
		clusterStateInfo: g("cluster_state_info", "Cluster state label-carrying info metric (always 1)", withTrailing(base, "state")),
		clusterNodeUp:    g("cluster_node_up", "1 if the named controller node is up", clusterLbl),
		clusterNodeRole:  g("cluster_node_leader", "1 if the named node is the cluster leader", clusterLbl),

		// Controller analytics
		controllerAvgCPUUsage:          g("controller_avg_cpu_usage", "Controller average CPU usage percent", controllerLbl),
		controllerAvgMemUsage:          g("controller_avg_mem_usage", "Controller average memory usage percent", controllerLbl),
		controllerAvgDiskUsage:         g("controller_avg_disk_usage", "Controller average disk usage percent", controllerLbl),
		controllerAvgDiskReadBytes:     g("controller_avg_disk_read_bytes", "Controller average disk read bytes", controllerLbl),
		controllerAvgDiskWriteBytes:    g("controller_avg_disk_write_bytes", "Controller average disk write bytes", controllerLbl),
		controllerAvgNumActiveVS:       g("controller_avg_num_active_vs", "Controller average active virtual services", controllerLbl),
		controllerAvgNumBackendServers: g("controller_avg_num_backend_servers", "Controller average backend servers", controllerLbl),

		// VS inventory
		vsOperUp:         g("vs_oper_up", "1 if virtual service oper_status is OPER_UP", vsLbl),
		vsOperStatusInfo: g("vs_oper_status_info", "VS oper_status label-carrying info (always 1)", withTrailing(vsLbl, "state")),
		vsEnabled:        g("vs_enabled", "1 if virtual service config.enabled is true", vsLbl),
		vsHealthScore:    g("vs_health_score", "Virtual service health score (0-100)", vsLbl),
		vsPercentSesUp:   g("vs_percent_ses_up", "Percentage of placement SEs UP for this VS (0-100)", vsLbl),
		vsTypeInfo:       g("vs_type_info", "VS type (NORMAL/VH_PARENT/VH_CHILD) info metric", withTrailing(vsLbl, "type")),
		vsAlertLevel:     g("vs_alert_level_info", "Active alert level summary (LOW/MEDIUM/HIGH); absent if none", withTrailing(vsLbl, "level")),

		// VS vip_summary
		vsVipOperUp:         g("vs_vip_oper_up", "Per-VIP oper_up from VS runtime.vip_summary[]", vsVipLbl),
		vsVipPercentSesUp:   g("vs_vip_percent_ses_up", "Per-VIP percent SEs up from VS runtime.vip_summary[]", vsVipLbl),
		vsVipNumSeAssigned:  g("vs_vip_num_se_assigned", "Per-VIP num_se_assigned from VS runtime.vip_summary[]", vsVipLbl),
		vsVipNumSeRequested: g("vs_vip_num_se_requested", "Per-VIP num_se_requested from VS runtime.vip_summary[]", vsVipLbl),
		vsVipOperStatusInfo: g("vs_vip_oper_status_info", "Per-VIP oper_status label-carrying info (always 1)", withTrailing(vsVipLbl, "state")),

		// VS analytics
		vsAvgBandwidth:       g("vs_l4_client_avg_bandwidth_bps", "Client bandwidth bits/s", vsLbl),
		vsAvgCompleteConns:   g("vs_l4_client_avg_complete_conns", "Completed client connections", vsLbl),
		vsAvgNewEstabConns:   g("vs_l4_client_avg_new_established_conns", "New established connections", vsLbl),
		vsMaxOpenConns:       g("vs_l4_client_max_open_conns", "Max open connections in window", vsLbl),
		vsConnectionsDropped: g("vs_l4_client_avg_connections_dropped", "Connections dropped", vsLbl),
		vsAvgTotalRequests:   g("vs_l7_client_avg_total_requests", "Total HTTP requests", vsLbl),
		vsAvgCompleteResp:    g("vs_l7_client_avg_complete_responses", "Complete HTTP responses", vsLbl),
		vsAvgErrorResp:       g("vs_l7_client_avg_error_responses", "Error HTTP responses", vsLbl),
		vsAvgResp2xx:         g("vs_l7_client_avg_resp_2xx", "2xx HTTP responses", vsLbl),
		vsAvgResp4xx:         g("vs_l7_client_avg_resp_4xx", "4xx HTTP responses", vsLbl),
		vsAvgResp5xx:         g("vs_l7_client_avg_resp_5xx", "5xx HTTP responses", vsLbl),
		vsApdexR:             g("vs_l7_client_apdex_r", "Apdex score for response time (0-1)", vsLbl),
		vsAvgClientRTT:       g("vs_l7_client_avg_client_rtt_ms", "Average client RTT (ms)", vsLbl),
		vsAvgRespLatency:     g("vs_l7_server_avg_resp_latency_ms", "Average server response latency (ms)", vsLbl),

		// Pool inventory
		poolOperUp:                  g("pool_oper_up", "1 if pool oper_status is OPER_UP", poolLbl),
		poolOperStatusInfo:          g("pool_oper_status_info", "Pool oper_status info (always 1)", withTrailing(poolLbl, "state")),
		poolEnabled:                 g("pool_enabled", "1 if pool config.enabled is true", poolLbl),
		poolHealthScore:             g("pool_health_score", "Pool health score (0-100)", poolLbl),
		poolNumServers:              g("pool_num_servers", "Number of servers in the pool", poolLbl),
		poolNumServersUp:            g("pool_num_servers_up", "Number of servers UP", poolLbl),
		poolNumServersEnabled:       g("pool_num_servers_enabled", "Number of admin-enabled servers", poolLbl),
		poolPercentServersUpEnabled: g("pool_percent_servers_up_enabled", "Percent of admin-enabled servers UP", poolLbl),
		poolPercentServersUpTotal:   g("pool_percent_servers_up_total", "Percent of total servers UP", poolLbl),
		poolAlertLevel:              g("pool_alert_level_info", "Active alert level summary", withTrailing(poolLbl, "level")),
		poolAppProfileType:          g("pool_app_profile_type_info", "Pool app profile type (HTTP/L4/...)", withTrailing(poolLbl, "app_profile_type")),

		// Pool members
		poolMemberOperUp:         g("pool_member_oper_up", "1 if pool member oper_status is OPER_UP", poolMemberLbl),
		poolMemberOperStatusInfo: g("pool_member_oper_status_info", "Pool member oper_status info (always 1)", withTrailing(poolMemberLbl, "state")),

		// Pool analytics
		poolAvgBandwidth:     g("pool_l4_server_avg_bandwidth_bps", "Backend bandwidth bits/s", poolLbl),
		poolAvgCompleteConns: g("pool_l4_server_avg_complete_conns", "Completed backend connections", poolLbl),
		poolAvgOpenConns:     g("pool_l4_server_avg_open_conns", "Open backend connections", poolLbl),
		poolAvgTotalRTT:      g("pool_l4_server_avg_total_rtt_ms", "Average TCP RTT to backend (ms)", poolLbl),
		poolAvgRespLatency:   g("pool_l7_server_avg_resp_latency_ms", "Average backend response latency (ms)", poolLbl),
		poolAvgErrorResp:     g("pool_l7_server_avg_error_responses", "Backend error HTTP responses", poolLbl),
		poolAvgHealthStatus:  g("pool_l4_server_avg_health_status", "Backend health status score", poolLbl),
		poolAvgUptime:        g("pool_l4_server_avg_uptime", "Backend uptime ratio (0-100)", poolLbl),

		// Pool Group
		poolGroupInfo:        g("pool_group_info", "Pool group info metric (always 1)", pgLbl),
		poolGroupMemberCount: g("pool_group_member_count", "Number of pool members in the pool group", pgLbl),

		// GSLB
		gslbServiceOperUp:         g("gslb_service_oper_up", "1 if GSLB service oper_status is OPER_UP", gslbLbl),
		gslbServiceOperStatusInfo: g("gslb_service_oper_status_info", "GSLB service oper_status info", withTrailing(gslbLbl, "state")),
		gslbServiceEnabled:        g("gslb_service_enabled", "1 if GSLB service config.enabled is true", gslbLbl),
		gslbServiceMemberCount:    g("gslb_service_member_count", "Number of pool members in the GSLB service", gslbLbl),
		gslbServiceDomainsInfo:    g("gslb_service_domain_info", "GSLB service domain info (always 1)", withTrailing(gslbLbl, "fqdn")),

		// SE inventory
		seOperUp:          g("se_oper_up", "1 if service engine oper_status is OPER_UP", seLbl),
		seOperStatusInfo:  g("se_oper_status_info", "SE oper_status info (always 1)", withTrailing(seLbl, "state")),
		seEnabled:         g("se_enabled", "1 if SE enable_state is SE_STATE_ENABLED", seLbl),
		seHealthScore:     g("se_health_score", "Service engine health score (0-100)", seLbl),
		seConnected:       g("se_connected", "1 if SE has an active control-channel connection to the controller", seLbl),
		seBgpPeersUp:      g("se_bgp_peers_up", "1 if all advertise_vip BGP peers are up", seLbl),
		seGatewayUp:       g("se_gateway_up", "1 if SE default gateway is reachable", seLbl),
		seAtCurrVer:       g("se_at_curr_ver", "1 if SE is running the controller's current version", seLbl),
		seSufficientMem:   g("se_sufficient_memory", "1 if SE has sufficient memory", seLbl),
		seLicensedCores:   g("se_licensed_service_cores", "Number of service cores licensed to this SE", seLbl),
		seLicenseState:    g("se_license_state_info", "SE license_state info (always 1)", withTrailing(seLbl, "license_state")),
		sePowerState:      g("se_power_state_info", "SE power_state info (always 1)", withTrailing(seLbl, "power_state")),
		seMigrateState:    g("se_migrate_state_info", "SE migrate_state info (always 1)", withTrailing(seLbl, "migrate_state")),
		seVersionInfo:     g("se_version_info", "SE software version info (always 1)", withTrailing(seLbl, "version")),
		seEnableStateInfo: g("se_enable_state_info", "SE enable_state info (always 1)", withTrailing(seLbl, "enable_state")),

		// SE analytics
		seAvgCPUUsage:     g("se_avg_cpu_usage", "SE average CPU usage percent", seAnalyticsLbl),
		seAvgMemUsage:     g("se_avg_mem_usage", "SE average memory usage percent", seAnalyticsLbl),
		seAvgDiskUsage:    g("se_avg_disk_usage", "SE average disk usage percent", seAnalyticsLbl),
		seAvgConnections:  g("se_avg_connections", "SE average active connections", seAnalyticsLbl),
		seAvgConnDropped:  g("se_avg_connections_dropped", "SE average connections dropped", seAnalyticsLbl),
		seAvgRxBytes:      g("se_if_avg_rx_bytes", "SE average rx bytes", seAnalyticsLbl),
		seAvgTxBytes:      g("se_if_avg_tx_bytes", "SE average tx bytes", seAnalyticsLbl),
		seAvgBandwidth:    g("se_if_avg_bandwidth_bps", "SE average bandwidth bits/s", seAnalyticsLbl),
		seAvgConnMem:      g("se_avg_connection_mem_usage", "SE average connection-memory usage", seAnalyticsLbl),
		sePctConnDropped:  g("se_pct_connections_dropped", "SE percent of connections dropped", seAnalyticsLbl),
		sePktBufUsage:     g("se_avg_packet_buffer_usage", "SE average packet-buffer usage", seAnalyticsLbl),
		sePersistTblUsage: g("se_avg_persistent_table_usage", "SE average persistence-table usage", seAnalyticsLbl),
		seSslSessCache:    g("se_avg_ssl_session_cache_usage", "SE average SSL session-cache usage", seAnalyticsLbl),

		// VsVip
		vipOperUp:          g("vip_oper_up", "1 if VIP oper_status is OPER_UP", vipLbl),
		vipOperStatusInfo:  g("vip_oper_status_info", "VIP oper_status info (always 1)", withTrailing(vipLbl, "state")),
		vipEnabled:         g("vip_enabled", "1 if the VIP entry is enabled (vip[].enabled)", vipLbl),
		vipPercentSesUp:    g("vip_percent_ses_up", "Percentage of SEs UP for this VIP", vipLbl),
		vipNumSeAssigned:   g("vip_num_se_assigned", "Number of SEs currently hosting this VIP", vipLbl),
		vipNumSeRequested:  g("vip_num_se_requested", "Number of SEs requested to host this VIP", vipLbl),
		vipActiveOnSe:      g("vip_active_on_se", "1 if the VIP is active on the named SE", vipPlacementLbl),
		vipSharedByVsCount: g("vip_shared_by_vs_count", "Number of Virtual Services sharing this VsVip", vipLbl),
		vipFloatingIP:      g("vip_floating_ip", "1 when the VIP has a floating/elastic IP attached", vipLbl),
		vipAutoAllocated:   g("vip_auto_allocated", "1 when the VIP address was auto-allocated by Avi IPAM", vipLbl),
		vipDNSRecord:       g("vip_dns_record_info", "Info metric (always 1) for each DNS FQDN attached to a VsVip", vipDNSLbl),

		// Topology
		topologyNode:              g("topology_node", "Topology node (1 if up, 0 if down). Labels carry node-graph visualization fields.", topoNodeLbl),
		topologyEdge:              g("topology_edge", "Topology edge between two nodes (always 1).", topoEdgeLbl),
		topologyNodeState:         g("topology_node_state", "Topology node state (1=UP, 0=DOWN)", topoNodeStatsLbl),
		topologyNodeHealth:        g("topology_node_health", "Topology node health (0-100)", topoNodeStatsLbl),
		topologyNodeRequestsTotal: g("topology_node_requests_total", "Topology node total request rate", topoNodeStatsLbl),
		topologyNodeConnections:   g("topology_node_connections", "Topology node connection count", topoNodeStatsLbl),
	}

	e.vsAnalyticsGauges = buildAnalyticsGaugeMap(g, "vs", vsLbl, vsMetricIDs, map[string]*prometheus.GaugeVec{
		"l4_client.avg_bandwidth":             e.vsAvgBandwidth,
		"l4_client.avg_complete_conns":        e.vsAvgCompleteConns,
		"l4_client.avg_new_established_conns": e.vsAvgNewEstabConns,
		"l4_client.max_open_conns":            e.vsMaxOpenConns,
		"l4_client.avg_connections_dropped":   e.vsConnectionsDropped,
		"l7_client.avg_total_requests":        e.vsAvgTotalRequests,
		"l7_client.avg_complete_responses":    e.vsAvgCompleteResp,
		"l7_client.avg_error_responses":       e.vsAvgErrorResp,
		"l7_client.avg_resp_2xx":              e.vsAvgResp2xx,
		"l7_client.avg_resp_4xx":              e.vsAvgResp4xx,
		"l7_client.avg_resp_5xx":              e.vsAvgResp5xx,
		"l7_client.apdexr":                    e.vsApdexR,
		"l7_client.avg_client_rtt":            e.vsAvgClientRTT,
		"l7_server.avg_resp_latency":          e.vsAvgRespLatency,
	})
	e.poolAnalyticsGauges = buildAnalyticsGaugeMap(g, "pool", poolLbl, poolMetricIDs, map[string]*prometheus.GaugeVec{
		"l4_server.avg_bandwidth":       e.poolAvgBandwidth,
		"l4_server.avg_complete_conns":  e.poolAvgCompleteConns,
		"l4_server.avg_open_conns":      e.poolAvgOpenConns,
		"l4_server.avg_total_rtt":       e.poolAvgTotalRTT,
		"l7_server.avg_resp_latency":    e.poolAvgRespLatency,
		"l7_server.avg_error_responses": e.poolAvgErrorResp,
		"l4_server.avg_health_status":   e.poolAvgHealthStatus,
		"l4_server.avg_uptime":          e.poolAvgUptime,
	})
	e.seAnalyticsGauges = buildAnalyticsGaugeMap(g, "", seAnalyticsLbl, seMetricIDs, map[string]*prometheus.GaugeVec{
		"se_stats.avg_cpu_usage":               e.seAvgCPUUsage,
		"se_stats.avg_mem_usage":               e.seAvgMemUsage,
		"se_stats.avg_disk1_usage":             e.seAvgDiskUsage,
		"se_stats.avg_connections":             e.seAvgConnections,
		"se_stats.avg_connections_dropped":     e.seAvgConnDropped,
		"se_if.avg_rx_bytes":                   e.seAvgRxBytes,
		"se_if.avg_tx_bytes":                   e.seAvgTxBytes,
		"se_if.avg_bandwidth":                  e.seAvgBandwidth,
		"se_stats.avg_connection_mem_usage":    e.seAvgConnMem,
		"se_stats.pct_connections_dropped":     e.sePctConnDropped,
		"se_stats.avg_packet_buffer_usage":     e.sePktBufUsage,
		"se_stats.avg_persistent_table_usage":  e.sePersistTblUsage,
		"se_stats.avg_ssl_session_cache_usage": e.seSslSessCache,
	})

	return e, nil
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.clusterUp
	ch <- e.clusterProgress

	e.up.Describe(ch)
	e.scrapeDuration.Describe(ch)
	e.scrapeErrorsTotal.Describe(ch)
	e.scrapeTotal.Describe(ch)
	e.moduleLastSuccess.Describe(ch)
	e.moduleLastAttempt.Describe(ch)
	e.moduleAge.Describe(ch)
	e.moduleStale.Describe(ch)
	e.moduleRefreshDuration.Describe(ch)
	e.moduleRefreshErrorsTotal.Describe(ch)
	e.moduleRefreshTotal.Describe(ch)

	e.clusterStateInfo.Describe(ch)
	e.clusterNodeUp.Describe(ch)
	e.clusterNodeRole.Describe(ch)

	for _, m := range e.allGaugeVecs() {
		m.Describe(ch)
	}
}

// allGaugeVecs lists every GaugeVec the exporter owns. Used for Describe and
// Reset loops so adding a new metric doesn't require updating three lists.
func (e *Exporter) allGaugeVecs() []*prometheus.GaugeVec {
	out := []*prometheus.GaugeVec{
		e.clusterStateInfo, e.clusterNodeUp, e.clusterNodeRole,
		e.controllerAvgCPUUsage, e.controllerAvgMemUsage, e.controllerAvgDiskUsage,
		e.controllerAvgDiskReadBytes, e.controllerAvgDiskWriteBytes,
		e.controllerAvgNumActiveVS, e.controllerAvgNumBackendServers,
		e.vsOperUp, e.vsOperStatusInfo, e.vsEnabled, e.vsHealthScore, e.vsPercentSesUp, e.vsTypeInfo, e.vsAlertLevel,
		e.vsVipOperUp, e.vsVipPercentSesUp, e.vsVipNumSeAssigned, e.vsVipNumSeRequested, e.vsVipOperStatusInfo,
		e.poolOperUp, e.poolOperStatusInfo, e.poolEnabled, e.poolHealthScore,
		e.poolNumServers, e.poolNumServersUp, e.poolNumServersEnabled,
		e.poolPercentServersUpEnabled, e.poolPercentServersUpTotal,
		e.poolAlertLevel, e.poolAppProfileType,
		e.poolMemberOperUp, e.poolMemberOperStatusInfo,
		e.poolGroupInfo, e.poolGroupMemberCount,
		e.gslbServiceOperUp, e.gslbServiceOperStatusInfo, e.gslbServiceEnabled, e.gslbServiceMemberCount, e.gslbServiceDomainsInfo,
		e.seOperUp, e.seOperStatusInfo, e.seEnabled, e.seHealthScore,
		e.seConnected, e.seBgpPeersUp, e.seGatewayUp, e.seAtCurrVer, e.seSufficientMem, e.seLicensedCores,
		e.seLicenseState, e.sePowerState, e.seMigrateState, e.seVersionInfo, e.seEnableStateInfo,
		e.vipOperUp, e.vipOperStatusInfo, e.vipEnabled, e.vipPercentSesUp, e.vipNumSeAssigned, e.vipNumSeRequested,
		e.vipActiveOnSe, e.vipSharedByVsCount, e.vipFloatingIP, e.vipAutoAllocated, e.vipDNSRecord,
		e.topologyNode, e.topologyEdge, e.topologyNodeState, e.topologyNodeHealth,
		e.topologyNodeRequestsTotal, e.topologyNodeConnections,
	}
	out = append(out, sortedUniqueGaugeVecs(e.vsAnalyticsGauges)...)
	out = append(out, sortedUniqueGaugeVecs(e.poolAnalyticsGauges)...)
	out = append(out, sortedUniqueGaugeVecs(e.seAnalyticsGauges)...)
	return out
}

// buildBaseLabels returns the cached base label values.
func (e *Exporter) buildBaseLabels() []string {
	return e.baseLabels
}

// appendLabels combines base labels (sorted user-supplied) with additional values.
func (e *Exporter) appendLabels(extra ...string) []string {
	out := make([]string, 0, len(e.baseLabels)+len(extra))
	out = append(out, e.baseLabels...)
	out = append(out, extra...)
	return out
}

// --- Info-metric helpers ---------------------------------------------------

// emitOperStatusInfo sets {label..., state="OPER_*"} 1 for the given metric.
// Empty state is treated as OPER_UNKNOWN so dashboards don't get gaps.
func (e *Exporter) emitOperStatusInfo(gv *prometheus.GaugeVec, base []string, state string) {
	if state == "" {
		state = "OPER_UNKNOWN"
	}
	gv.WithLabelValues(append(append([]string{}, base...), state)...).Set(1)
}

// emitAlertLevel emits the alert info series only when a level is set.
func (e *Exporter) emitAlertLevel(gv *prometheus.GaugeVec, base []string, level string) {
	if level == "" {
		return
	}
	gv.WithLabelValues(append(append([]string{}, base...), level)...).Set(1)
}

// emitInfo emits a generic info metric with one extra label value.
// kind exists only to make caller intent clearer at the call site.
func (e *Exporter) emitInfo(gv *prometheus.GaugeVec, base []string, kind, value string) {
	_ = kind
	if value == "" {
		return
	}
	gv.WithLabelValues(append(append([]string{}, base...), value)...).Set(1)
}

// boolToFloat converts an optional bool pointer; nil becomes 0.
func boolToFloat(b *bool) float64 {
	if b != nil && *b {
		return 1
	}
	return 0
}
