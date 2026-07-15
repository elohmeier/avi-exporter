package collector

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"testing"

	"github.com/elohmeier/avi-exporter/avi"
)

func intPointer(value int) *int { return &value }

func TestServiceEngineAddressesFlattensAndDeduplicates(t *testing.T) {
	item := avi.SEConfig{
		MgmtIPAddress:  &avi.IPAddr{Addr: "192.0.2.10"},
		MgmtIP6Address: &avi.IPAddr{Addr: "2001:db8::5"},
		MgmtVNIC: &avi.VNIC{
			IfName:     "Management",
			MacAddress: "00:11:22:33:44:55",
			NetworkRef: "https://controller/api/network/net-mgmt#management",
			VrfRef:     "https://controller/api/vrfcontext/vrf-mgmt#management-vrf",
			VNICNetworks: []avi.VNICNetwork{{
				IP: avi.IPAddrPrefix{IPAddr: avi.IPAddr{Addr: "192.0.2.10"}, Mask: intPointer(24)},
			}},
		},
		DataVNICs: []avi.VNIC{
			{
				IfName:      "eth1",
				MacAddress:  "00:11:22:33:44:66",
				NetworkName: "backend",
				NetworkRef:  "https://controller/api/network/net-data#backend",
				VrfRef:      "https://controller/api/vrfcontext/vrf-data#backend-vrf",
				VNICNetworks: []avi.VNICNetwork{
					{IP: avi.IPAddrPrefix{IPAddr: avi.IPAddr{Addr: "198.51.100.20"}, Mask: intPointer(25)}},
					{IP: avi.IPAddrPrefix{IPAddr: avi.IPAddr{Addr: "not-an-ip"}, Mask: intPointer(24)}},
				},
				VLANInterfaces: []avi.VLANInterface{{
					VNICNetworks: []avi.VNICNetwork{{
						IP: avi.IPAddrPrefix{IPAddr: avi.IPAddr{Addr: "2001:db8:42::10"}, Mask: intPointer(64)},
					}},
				}},
			},
			{
				IfName:       "duplicate-data",
				VNICNetworks: []avi.VNICNetwork{{IP: avi.IPAddrPrefix{IPAddr: avi.IPAddr{Addr: "192.0.2.10"}}}},
			},
			{
				IfName:       "management-fallback",
				IsMgmt:       true,
				VNICNetworks: []avi.VNICNetwork{{IP: avi.IPAddrPrefix{IPAddr: avi.IPAddr{Addr: "203.0.113.9"}}}},
			},
		},
	}

	got := serviceEngineAddresses(item)
	want := []seAddressRecord{
		{IP: "192.0.2.10", Role: "management", Interface: "Management", Network: "management", NetworkUUID: "net-mgmt", VRF: "management-vrf", VRFUUID: "vrf-mgmt", CIDR: "192.0.2.0/24", MAC: "00:11:22:33:44:55"},
		{IP: "198.51.100.20", Role: "data", Interface: "eth1", Network: "backend", NetworkUUID: "net-data", VRF: "backend-vrf", VRFUUID: "vrf-data", CIDR: "198.51.100.0/25", MAC: "00:11:22:33:44:66"},
		{IP: "2001:db8:42::10", Role: "data", Interface: "eth1", Network: "backend", NetworkUUID: "net-data", VRF: "backend-vrf", VRFUUID: "vrf-data", CIDR: "2001:db8:42::/64", MAC: "00:11:22:33:44:66"},
		{IP: "2001:db8::5", Role: "management"},
		{IP: "203.0.113.9", Role: "management", Interface: "management-fallback"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("serviceEngineAddresses() = %#v, want %#v", got, want)
	}

	for _, test := range []struct {
		name string
		ip   string
		mask *int
		want string
	}{
		{name: "missing mask", ip: "192.0.2.1", want: ""},
		{name: "invalid IP", ip: "bad", mask: intPointer(24), want: ""},
		{name: "invalid IPv4 mask", ip: "192.0.2.1", mask: intPointer(33), want: ""},
		{name: "zero mask", ip: "192.0.2.1", mask: intPointer(0), want: "0.0.0.0/0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizedCIDR(test.ip, test.mask); got != test.want {
				t.Fatalf("normalizedCIDR(%q) = %q, want %q", test.ip, got, test.want)
			}
		})
	}

	preferred := preferAddress(
		seAddressRecord{IP: "192.0.2.30", Role: "data", Interface: "eth2"},
		seAddressRecord{IP: "192.0.2.30", Role: "data", Network: "frontend"},
	)
	if preferred.Interface != "eth2" || preferred.Network != "frontend" {
		t.Fatalf("preferAddress did not deterministically merge equal candidates: %#v", preferred)
	}
	filled := seAddressRecord{}
	fillAddressGaps(&filled, seAddressRecord{
		Interface: "eth3", Network: "network", NetworkUUID: "network-uuid",
		VRF: "vrf", VRFUUID: "vrf-uuid", CIDR: "192.0.2.0/24", MAC: "00:11:22:33:44:77",
	})
	if filled.Interface == "" || filled.Network == "" || filled.NetworkUUID == "" || filled.VRF == "" || filled.VRFUUID == "" || filled.CIDR == "" || filled.MAC == "" {
		t.Fatalf("fillAddressGaps left metadata empty: %#v", filled)
	}
}

func TestIdentityInventoryRefreshAndLastGoodCache(t *testing.T) {
	phase := 0
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/cluster":
			writeJSON(t, w, map[string]any{
				"uuid": "cluster-1", "name": "controller-cluster",
				"nodes": []map[string]any{{
					"name":              "node-a",
					"ip":                map[string]any{"addr": "192.0.2.5", "type": "V4"},
					"ip6":               map[string]any{"addr": "2001:db8::5", "type": "V6"},
					"public_ip_or_name": map[string]any{"addr": "avi.example.com"},
					"vm_hostname":       "controller-a", "vm_name": "avi-a", "vm_uuid": "vm-a",
				}},
			})
		case "/api/serviceengine":
			if phase == 2 {
				http.Error(w, "SE config unavailable", http.StatusServiceUnavailable)
				return
			}
			addresses := []map[string]any{{
				"ip": map[string]any{"ip_addr": map[string]any{"addr": "10.20.30.40", "type": "V4"}, "mask": 24},
			}}
			if phase == 0 {
				addresses = append(addresses, map[string]any{
					"ip": map[string]any{"ip_addr": map[string]any{"addr": "10.20.31.40", "type": "V4"}, "mask": 24},
				})
			}
			writeJSON(t, w, map[string]any{
				"count": 1,
				"results": []map[string]any{{
					"uuid": "se-1", "name": "se-a", "controller_ip": "192.0.2.5", "availability_zone": "zone-a",
					"cloud_ref":    "https://controller/api/cloud/cloud-1#cloud-a",
					"se_group_ref": "https://controller/api/serviceenginegroup/seg-1#group-a",
					"tenant_ref":   "https://controller/api/tenant/tenant-1#admin",
					"data_vnics": []map[string]any{{
						"if_name": "eth1", "mac_address": "00:11:22:33:44:55",
						"network_ref":   "https://controller/api/network/network-1#backend-a",
						"vrf_ref":       "https://controller/api/vrfcontext/vrf-1#vrf-a",
						"vnic_networks": addresses,
					}},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	})
	defer controller.Close()

	cfg := testConfig([]string{"admin"}, nil)
	cfg.DisabledModules = []string{
		"cluster", "controller_metrics", "se_inventory", "se_metrics",
		"vs_inventory", "vs_metrics", "pool_inventory", "pool_metrics", "pool_members",
		"vsvip", "pool_group", "gslb", "topology",
	}
	exp, err := NewExporter(cfg, controller.URL, "user", "pass", true, "", 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("first RefreshOnce: %v", err)
	}
	if !exp.Ready() {
		t.Fatalf("Ready() = false after identity modules succeeded")
	}

	mfs := gatherRegisteredExporter(t, exp)
	if got := metricValueForLabels(t, metricFamily(t, mfs, "avi_cluster_node_info"), map[string]string{
		"node": "node-a", "controller_cluster_uuid": "cluster-1", "ip": "192.0.2.5", "vm_uuid": "vm-a",
	}); got != 1 {
		t.Fatalf("avi_cluster_node_info = %v, want 1", got)
	}
	if got := metricValueForLabels(t, metricFamily(t, mfs, "avi_se_info"), map[string]string{
		"se_uuid": "se-1", "cloud": "cloud-a", "cloud_uuid": "cloud-1", "se_group": "group-a", "tenant_uuid": "tenant-1",
	}); got != 1 {
		t.Fatalf("avi_se_info = %v, want 1", got)
	}
	if got := metricValueForLabels(t, metricFamily(t, mfs, "avi_se_address_info"), map[string]string{
		"se_uuid": "se-1", "ip": "10.20.30.40", "address_role": "data", "network_uuid": "network-1", "cidr": "10.20.30.0/24",
	}); got != 1 {
		t.Fatalf("avi_se_address_info = %v, want 1", got)
	}
	if got := len(metricFamily(t, mfs, "avi_se_address_info").Metric); got != 2 {
		t.Fatalf("initial SE address count = %d, want 2", got)
	}

	phase = 1
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("second RefreshOnce: %v", err)
	}
	mfs = gatherRegisteredExporter(t, exp)
	if got := len(metricFamily(t, mfs, "avi_se_address_info").Metric); got != 1 {
		t.Fatalf("SE address count after successful replacement = %d, want 1", got)
	}

	phase = 2
	if err := exp.RefreshOnce(context.Background()); err == nil {
		t.Fatalf("failed RefreshOnce returned nil")
	}
	mfs = gatherRegisteredExporter(t, exp)
	if got := countMetricsWithLabel(metricFamily(t, mfs, "avi_se_address_info"), "ip", "10.20.30.40"); got != 1 {
		t.Fatalf("last-good SE address count = %d, want 1", got)
	}
	status := exp.CacheStatus()
	foundSEConfigError := false
	for _, module := range status.Modules {
		if module.Module == "se_config" && module.Errors == 1 && module.LastError != "" {
			foundSEConfigError = true
		}
	}
	if !foundSEConfigError {
		t.Fatalf("cache status does not expose failed se_config refresh: %#v", status.Modules)
	}
}

func TestIdentityInventoryModulesCanBeDisabled(t *testing.T) {
	calls := 0
	controller := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.NotFound(w, r)
	})
	defer controller.Close()

	exp, err := NewExporter(testConfig([]string{"admin"}, allTestModules), controller.URL, "user", "pass", true, "", 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if err := exp.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce with all modules disabled: %v", err)
	}
	if calls != 0 {
		t.Fatalf("controller API calls with all modules disabled = %d, want 0", calls)
	}
	if keys := exp.requiredModuleKeysLocked(); len(keys) != 0 {
		t.Fatalf("required module keys with all modules disabled = %#v, want none", keys)
	}
}
