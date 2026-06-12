package avi

import "encoding/json"

// Marker mirrors RoleFilterMatchLabel — the schema AKO uses to tag Avi objects
// with Kubernetes metadata (Namespace, ServiceName, IngressName, Host, ...).
type Marker struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// PageResp is the generic collection envelope (`{count,next,previous,results}`).
type PageResp[T any] struct {
	Count    int     `json:"count"`
	Next     *string `json:"next"`
	Previous *string `json:"previous"`
	Results  []T     `json:"results"`
}

// AlertSummary mirrors inventory `alert` blocks (LOW/MEDIUM/HIGH).
type AlertSummary struct {
	Level string `json:"level,omitempty"`
}

// --- Tenants --------------------------------------------------------------

type Tenant struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

// --- Cluster --------------------------------------------------------------

type ClusterNode struct {
	Name       string `json:"name"`
	VMHostname string `json:"vm_hostname,omitempty"`
	VMUUID     string `json:"vm_uuid,omitempty"`
}

type Cluster struct {
	UUID  string        `json:"uuid"`
	Name  string        `json:"name"`
	Nodes []ClusterNode `json:"nodes"`
}

type ClusterNodeState struct {
	Name    string `json:"name"`
	State   string `json:"state"` // e.g. CLUSTER_ACTIVE
	Role    string `json:"role"`  // e.g. CLUSTER_LEADER / CLUSTER_FOLLOWER
	UpSince string `json:"up_since,omitempty"`
}

type ClusterRuntime struct {
	NodeStates   []ClusterNodeState `json:"node_states"`
	ClusterState struct {
		State    string `json:"state"`              // CLUSTER_UP_HA_ACTIVE / CLUSTER_UP_NO_HA / ...
		Progress int    `json:"progress,omitempty"` // 0-100
		UpSince  string `json:"up_since,omitempty"`
	} `json:"cluster_state"`
}

// --- Shared inventory sub-types -------------------------------------------

// OperStatus is a common runtime sub-object on inventory entries.
type OperStatus struct {
	State            string          `json:"state"` // OPER_UP / OPER_DOWN / OPER_PARTITIONED / OPER_DISABLED / OPER_FAILED / ...
	LastChangedTime  json.RawMessage `json:"last_changed_time,omitempty"`
	ReasonCode       int             `json:"reason_code,omitempty"`
	ReasonCodeString string          `json:"reason_code_string,omitempty"`
}

// HealthScore is the analytics sub-object on inventory entries.
// Note: per the alb-sdk HealthScoreSummary, performance_score is a nested
// struct (HealthScorePerformanceData), not a scalar. Modeling it as a raw
// message keeps inventory parsing robust whether the controller returns a
// number, an object, or omits the field.
type HealthScore struct {
	HealthScore      float64         `json:"health_score"`
	AnomalyPenalty   json.RawMessage `json:"anomaly_penalty,omitempty"`
	PerformanceScore json.RawMessage `json:"performance_score,omitempty"`
	SecurityPenalty  json.RawMessage `json:"security_penalty,omitempty"`
	ResourcesPenalty json.RawMessage `json:"resources_penalty,omitempty"`
}

// IPAddr matches {type:"V4"|"V6", addr:"..."}.
type IPAddr struct {
	Type string `json:"type,omitempty"`
	Addr string `json:"addr,omitempty"`
}

// --- Virtual Service Inventory --------------------------------------------

// VSConfig is the per-VS config block (subset of fields we read).
type VSConfig struct {
	UUID         string    `json:"uuid"`
	Name         string    `json:"name"`
	URL          string    `json:"url,omitempty"`
	Enabled      *bool     `json:"enabled,omitempty"`
	Type         string    `json:"type,omitempty"` // VS_TYPE_NORMAL / VS_TYPE_VH_PARENT / VS_TYPE_VH_CHILD
	Fqdn         string    `json:"fqdn,omitempty"`
	CloudRef     string    `json:"cloud_ref,omitempty"`
	TenantRef    string    `json:"tenant_ref,omitempty"`
	VsvipRef     string    `json:"vsvip_ref,omitempty"`
	PoolRef      string    `json:"pool_ref,omitempty"`
	PoolGroupRef string    `json:"pool_group_ref,omitempty"`
	SeGroupRef   string    `json:"se_group_ref,omitempty"`
	CreatedBy    string    `json:"created_by,omitempty"`
	Markers      []Marker  `json:"markers,omitempty"`
	Services     []Service `json:"services,omitempty"`
	VIP          []Vip     `json:"vip,omitempty"`
	DNSInfo      []DNSInfo `json:"dns_info,omitempty"`
}

// Service is one (port, protocol) listener bound to a VS.
type Service struct {
	Port      int    `json:"port,omitempty"`
	PortRange int    `json:"port_range_end,omitempty"`
	Protocol  string `json:"override_application_profile_ref,omitempty"`
	EnableSSL *bool  `json:"enable_ssl,omitempty"`
}

// VipRuntimeSummary is one runtime entry per vip_id in a VS's runtime.vip_summary[].
// Source: VirtualServiceRuntimeSummary.VipSummary (alb-sdk).
type VipRuntimeSummary struct {
	VipID          string          `json:"vip_id"`
	OperStatus     OperStatus      `json:"oper_status"`
	PercentSesUp   int             `json:"percent_ses_up,omitempty"`
	NumSeAssigned  int             `json:"num_se_assigned,omitempty"`
	NumSeRequested int             `json:"num_se_requested,omitempty"`
	ServiceEngine  []VipSeAssigned `json:"service_engine,omitempty"`
	ScaleinInProg  *bool           `json:"scalein_in_progress,omitempty"`
	ScaleoutInProg *bool           `json:"scaleout_in_progress,omitempty"`
	MigrateInProg  *bool           `json:"migrate_in_progress,omitempty"`
}

// VSRuntime is the runtime summary attached to each VS inventory entry.
type VSRuntime struct {
	OperStatus   OperStatus          `json:"oper_status"`
	PercentSesUp int                 `json:"percent_ses_up,omitempty"`
	Type         string              `json:"type,omitempty"`
	VHChildVsRef []string            `json:"vh_child_vs_ref,omitempty"`
	VipSummary   []VipRuntimeSummary `json:"vip_summary,omitempty"`
}

type VSInventoryItem struct {
	Config      VSConfig     `json:"config"`
	Runtime     VSRuntime    `json:"runtime"`
	HealthScore HealthScore  `json:"health_score"`
	Alert       AlertSummary `json:"alert"`
	UUID        string       `json:"uuid"`
}

// --- Pool Inventory --------------------------------------------------------

// PoolConfig is the (summary) config block on /api/pool-inventory entries.
// It does NOT include `servers[]` — that requires /api/pool/{uuid}.
type PoolConfig struct {
	UUID          string   `json:"uuid"`
	Name          string   `json:"name"`
	URL           string   `json:"url,omitempty"`
	Enabled       *bool    `json:"enabled,omitempty"`
	TenantRef     string   `json:"tenant_ref,omitempty"`
	CloudRef      string   `json:"cloud_ref,omitempty"`
	VrfRef        string   `json:"vrf_ref,omitempty"`
	CreatedBy     string   `json:"created_by,omitempty"`
	GslbSpEnabled *bool    `json:"gslb_sp_enabled,omitempty"`
	Markers       []Marker `json:"markers,omitempty"`
}

// PoolRuntime mirrors PoolRuntimeSummary on the bulk inventory. The per-server
// list is NOT exposed here — for that, see PoolRuntimeDetail.
type PoolRuntime struct {
	OperStatus              OperStatus `json:"oper_status"`
	NumServers              int        `json:"num_servers,omitempty"`
	NumServersUp            int        `json:"num_servers_up,omitempty"`
	NumServersEnabled       int        `json:"num_servers_enabled,omitempty"`
	PercentServersUpEnabled int        `json:"percent_servers_up_enabled,omitempty"`
	PercentServersUpTotal   int        `json:"percent_servers_up_total,omitempty"`
}

type PoolInventoryItem struct {
	Config          PoolConfig   `json:"config"`
	Runtime         PoolRuntime  `json:"runtime"`
	HealthScore     HealthScore  `json:"health_score"`
	Alert           AlertSummary `json:"alert"`
	AppProfileType  string       `json:"app_profile_type,omitempty"`
	VirtualServices []VsRef      `json:"virtualservices,omitempty"`
	UUID            string       `json:"uuid"`
}

// ServerRuntimeDetail mirrors the per-server runtime returned by
// /api/pool/{uuid}/runtime/server/detail/ — used by the pool_members module.
type ServerRuntimeDetail struct {
	IPAddr     IPAddr     `json:"ip_addr"`
	Port       int        `json:"port"`
	OperStatus OperStatus `json:"oper_status"`
	SeUUID     string     `json:"se_uuid,omitempty"`
	Hostname   string     `json:"hostname,omitempty"`
}

// PoolRuntimeDetail is one entry in the runtime/server/detail array.
// Each pool can have multiple servers; some controller versions wrap the
// detail in {server:[...]}, others return a top-level array directly.
type PoolRuntimeDetail struct {
	Server []ServerRuntimeDetail `json:"server,omitempty"`
}

// --- Service Engine Inventory ---------------------------------------------

type SEConfig struct {
	UUID            string   `json:"uuid"`
	Name            string   `json:"name"`
	URL             string   `json:"url,omitempty"`
	EnableState     string   `json:"enable_state,omitempty"` // SE_STATE_ENABLED / SE_STATE_DISABLED / ...
	HostRef         string   `json:"host_ref,omitempty"`
	MgmtIPAddress   *IPAddr  `json:"mgmt_ip_address,omitempty"`
	TenantRef       string   `json:"tenant_ref,omitempty"`
	SeGroupRef      string   `json:"se_group_ref,omitempty"`
	VirtualServices []string `json:"virtualservice_refs,omitempty"`
}

type SERuntime struct {
	OperStatus    OperStatus `json:"oper_status"`
	SeConnected   *bool      `json:"se_connected,omitempty"`
	BgpPeersUp    *bool      `json:"bgp_peers_up,omitempty"`
	GatewayUp     *bool      `json:"gateway_up,omitempty"`
	LicenseState  string     `json:"license_state,omitempty"`
	PowerState    string     `json:"power_state,omitempty"`
	MigrateState  string     `json:"migrate_state,omitempty"`
	Version       string     `json:"version,omitempty"`
	AtCurrVer     *bool      `json:"at_curr_ver,omitempty"`
	SufficientMem *bool      `json:"sufficient_memory,omitempty"`
	OnlineSince   string     `json:"online_since,omitempty"`
	LicensedCores float64    `json:"licensed_service_cores,omitempty"`
}

type SEInventoryItem struct {
	Config      SEConfig    `json:"config"`
	Runtime     SERuntime   `json:"runtime"`
	HealthScore HealthScore `json:"health_score"`
	UUID        string      `json:"uuid"`
}

// --- VsVip Inventory -------------------------------------------------------

// Vip is one IP entry inside a VsVip. A single VsVip may carry several Vips
// (typically v4 + v6, or v4 + floating IP).
type Vip struct {
	VipID            string  `json:"vip_id"`
	Enabled          *bool   `json:"enabled,omitempty"`
	IPAddress        *IPAddr `json:"ip_address,omitempty"`
	IP6Address       *IPAddr `json:"ip6_address,omitempty"`
	FloatingIP       *IPAddr `json:"floating_ip,omitempty"`
	FloatingIP6      *IPAddr `json:"floating_ip6,omitempty"`
	AutoAllocateIP   *bool   `json:"auto_allocate_ip,omitempty"`
	AutoAllocateFIP  *bool   `json:"auto_allocate_floating_ip,omitempty"`
	AviAllocatedVIP  *bool   `json:"avi_allocated_vip,omitempty"`
	AviAllocatedFIP  *bool   `json:"avi_allocated_fip,omitempty"`
	AvailabilityZone string  `json:"availability_zone,omitempty"`
}

// DNSInfo is one FQDN record attached to a VsVip or VS.
type DNSInfo struct {
	FQDN string `json:"fqdn"`
	Type string `json:"type,omitempty"` // DNS_RECORD_A / _AAAA / _CNAME / ...
	TTL  int    `json:"ttl,omitempty"`
}

// VsRef is a back-reference to a VirtualService.
type VsRef struct {
	UUID string `json:"uuid"`
	Ref  string `json:"ref,omitempty"`
}

// UnmarshalJSON accepts both compact {uuid} references and documented {ref}
// references, including include_name refs suffixed with "#<name>".
func (v *VsRef) UnmarshalJSON(b []byte) error {
	var ref string
	if err := json.Unmarshal(b, &ref); err == nil {
		v.UUID = RefUUID(ref)
		v.Ref = ref
		return nil
	}

	type raw VsRef
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.UUID == "" {
		r.UUID = RefUUID(r.Ref)
	}
	*v = VsRef(r)
	return nil
}

// VsVipConfig is the config block from /api/vsvip-inventory.
type VsVipConfig struct {
	UUID              string    `json:"uuid"`
	Name              string    `json:"name"`
	URL               string    `json:"url,omitempty"`
	CloudRef          string    `json:"cloud_ref,omitempty"`
	TenantRef         string    `json:"tenant_ref,omitempty"`
	VrfContextRef     string    `json:"vrf_context_ref,omitempty"`
	CreatedBy         string    `json:"created_by,omitempty"`
	Markers           []Marker  `json:"markers,omitempty"`
	Vip               []Vip     `json:"vip,omitempty"`
	DNSInfo           []DNSInfo `json:"dns_info,omitempty"`
	VirtualServices   []VsRef   `json:"virtualservices,omitempty"`
	EastWestPlacement *bool     `json:"east_west_placement,omitempty"`
}

// VipSeAssigned is one SE→VIP placement entry shared by VsVipRuntime and
// VipRuntimeSummary on the VS side.
type VipSeAssigned struct {
	Name          string `json:"name,omitempty"`
	URL           string `json:"url,omitempty"`
	Ref           string `json:"ref,omitempty"`
	UUID          string `json:"uuid,omitempty"`
	Primary       *bool  `json:"primary,omitempty"`
	Standby       *bool  `json:"standby,omitempty"`
	Connected     *bool  `json:"connected,omitempty"`
	ActiveOnSe    *bool  `json:"active_on_se,omitempty"`
	ActiveOnCloud *bool  `json:"active_on_cloud,omitempty"`
}

// UnmarshalJSON accepts both older url/uuid forms and documented ref forms.
func (v *VipSeAssigned) UnmarshalJSON(b []byte) error {
	type raw VipSeAssigned
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.URL == "" {
		r.URL = r.Ref
	}
	if r.UUID == "" {
		r.UUID = RefUUID(r.URL)
	}
	*v = VipSeAssigned(r)
	return nil
}

// VsVipRuntime is the per-VIP runtime entry.
type VsVipRuntime struct {
	VipID          string          `json:"vip_id"`
	OperStatus     OperStatus      `json:"oper_status"`
	PercentSesUp   int             `json:"percent_ses_up,omitempty"`
	NumSeAssigned  int             `json:"num_se_assigned,omitempty"`
	NumSeRequested int             `json:"num_se_requested,omitempty"`
	ServiceEngine  []VipSeAssigned `json:"service_engine,omitempty"`
}

// VsVipInventoryItem mirrors one /api/vsvip-inventory entry. The controller
// may return `runtime` as either a single object (swagger model) or as an
// array per vip_id (observed on some versions). UnmarshalJSON accepts both.
type VsVipInventoryItem struct {
	Config  VsVipConfig    `json:"config"`
	Runtime []VsVipRuntime `json:"-"`
	UUID    string         `json:"uuid"`
}

// UnmarshalJSON handles both single-object and array shapes of `runtime`.
func (v *VsVipInventoryItem) UnmarshalJSON(b []byte) error {
	type raw struct {
		Config  VsVipConfig     `json:"config"`
		Runtime json.RawMessage `json:"runtime"`
		UUID    string          `json:"uuid"`
	}
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	v.Config = r.Config
	v.UUID = r.UUID

	if len(r.Runtime) == 0 || string(r.Runtime) == "null" {
		return nil
	}
	// Try array first, fall back to single object.
	if err := json.Unmarshal(r.Runtime, &v.Runtime); err == nil {
		return nil
	}
	var single VsVipRuntime
	if err := json.Unmarshal(r.Runtime, &single); err != nil {
		return err
	}
	v.Runtime = []VsVipRuntime{single}
	return nil
}

// --- Pool Group Inventory --------------------------------------------------

type PoolGroupMember struct {
	PoolRef       string `json:"pool_ref"`
	PriorityLabel string `json:"priority_label,omitempty"`
	Ratio         int    `json:"ratio,omitempty"`
}

type PoolGroupConfig struct {
	UUID       string            `json:"uuid"`
	Name       string            `json:"name"`
	URL        string            `json:"url,omitempty"`
	CloudRef   string            `json:"cloud_ref,omitempty"`
	TenantRef  string            `json:"tenant_ref,omitempty"`
	CreatedBy  string            `json:"created_by,omitempty"`
	Markers    []Marker          `json:"markers,omitempty"`
	Members    []PoolGroupMember `json:"members,omitempty"`
	MinServers int               `json:"min_servers,omitempty"`
}

// PoolGroupInventoryItem mirrors /api/poolgroup-inventory. There's no
// runtime section on PG inventory (state is derived from member pools).
type PoolGroupInventoryItem struct {
	Config PoolGroupConfig `json:"config"`
	UUID   string          `json:"uuid"`
}

// --- GSLB Service Inventory ------------------------------------------------

type GslbPool struct {
	Name     string `json:"name,omitempty"`
	Priority int    `json:"priority,omitempty"`
	Enabled  *bool  `json:"enabled,omitempty"`
}

type GslbServiceConfig struct {
	UUID          string     `json:"uuid"`
	Name          string     `json:"name"`
	URL           string     `json:"url,omitempty"`
	Enabled       *bool      `json:"enabled,omitempty"`
	IsFederated   *bool      `json:"is_federated,omitempty"`
	PoolAlgorithm string     `json:"pool_algorithm,omitempty"`
	MinMembers    int        `json:"min_members,omitempty"`
	DomainNames   []string   `json:"domain_names,omitempty"`
	Groups        []GslbPool `json:"groups,omitempty"`
	TenantRef     string     `json:"tenant_ref,omitempty"`
	CreatedBy     string     `json:"created_by,omitempty"`
	Markers       []Marker   `json:"markers,omitempty"`
}

type GslbServiceRuntime struct {
	Name          string     `json:"name,omitempty"`
	ObjUUID       string     `json:"obj_uuid,omitempty"`
	OperStatus    OperStatus `json:"oper_status"`
	ServicesState string     `json:"services_state,omitempty"`
	DomainNames   []string   `json:"domain_names,omitempty"`
}

type GslbServiceInventoryItem struct {
	Config  GslbServiceConfig  `json:"config"`
	Runtime GslbServiceRuntime `json:"runtime"`
	UUID    string             `json:"uuid"`
}
