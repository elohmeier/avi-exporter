package collector

import (
	"testing"

	"github.com/elohmeier/avi-exporter/avi"
)

func TestMergeVSConfigMetadataRestoresDeliveryRefs(t *testing.T) {
	dst := avi.VSConfig{
		UUID:    "vs-a",
		PoolRef: "pool-from-inventory",
	}
	src := avi.VSConfig{
		VsvipRef:     "vsvip-from-config",
		PoolRef:      "pool-from-config",
		PoolGroupRef: "poolgroup-from-config",
	}

	mergeVSConfigMetadata(&dst, src)

	if dst.VsvipRef != src.VsvipRef {
		t.Fatalf("VsvipRef = %q, want %q", dst.VsvipRef, src.VsvipRef)
	}
	if dst.PoolRef != "pool-from-inventory" {
		t.Fatalf("PoolRef = %q, want existing inventory value", dst.PoolRef)
	}
	if dst.PoolGroupRef != src.PoolGroupRef {
		t.Fatalf("PoolGroupRef = %q, want %q", dst.PoolGroupRef, src.PoolGroupRef)
	}
}

func TestEnrichVsVipInventoryUsesLinkedChildVSMetadata(t *testing.T) {
	items := []avi.VsVipInventoryItem{{
		Config: avi.VsVipConfig{UUID: "vsvip-a", Name: "vip-a"},
	}}
	vsItems := []avi.VSInventoryItem{
		{
			Config: avi.VSConfig{
				UUID:      "vs-parent",
				Name:      "vs-parent",
				CreatedBy: "ako-cluster-a",
				VsvipRef:  "https://controller.example/api/vsvip/vsvip-a",
			},
			Runtime: avi.VSRuntime{VHChildVsRef: []string{"https://controller.example/api/virtualservice/vs-child"}},
		},
		{
			Config: avi.VSConfig{
				UUID:          "vs-child",
				Name:          "vs-child",
				CreatedBy:     "ako-cluster-a",
				VHParentVSRef: "https://controller.example/api/virtualservice/vs-parent",
				Markers: []avi.Marker{
					{Key: "Namespace", Values: []string{"team-a"}},
					{Key: "ServiceName", Values: []string{"svc-a"}},
					{Key: "Host", Values: []string{"app.example.com"}},
				},
			},
		},
	}

	got := enrichVsVipInventory(items, vsItems)
	mi := avi.ParseObjectMetadata(got[0].Config.Markers, got[0].Config.ServiceMetadata)
	if mi.Namespace != "team-a" || mi.ServiceName != "svc-a" || mi.Host != "app.example.com" {
		t.Fatalf("VsVip metadata = %#v, want team-a/svc-a/app.example.com", mi)
	}
	if !avi.IsAKOManaged(got[0].Config.CreatedBy) {
		t.Fatalf("CreatedBy = %q, want AKO-managed value", got[0].Config.CreatedBy)
	}
	if got := len(got[0].Config.VirtualServices); got != 2 {
		t.Fatalf("linked virtual services = %d, want 2", got)
	}
}

func TestEnrichVsVipInventoryKeepsMixedChildMetadataEmpty(t *testing.T) {
	items := []avi.VsVipInventoryItem{{
		Config: avi.VsVipConfig{UUID: "vsvip-a", Name: "vip-a"},
	}}
	vsItems := []avi.VSInventoryItem{
		{
			Config: avi.VSConfig{
				UUID:      "vs-parent",
				Name:      "vs-parent",
				CreatedBy: "ako-cluster-a",
				VsvipRef:  "https://controller.example/api/vsvip/vsvip-a",
			},
		},
		{
			Config: avi.VSConfig{
				UUID:          "vs-child-a",
				Name:          "vs-child-a",
				CreatedBy:     "ako-cluster-a",
				VHParentVSRef: "https://controller.example/api/virtualservice/vs-parent",
				Markers: []avi.Marker{
					{Key: "Namespace", Values: []string{"team-a"}},
					{Key: "ServiceName", Values: []string{"svc-a"}},
				},
			},
		},
		{
			Config: avi.VSConfig{
				UUID:          "vs-child-b",
				Name:          "vs-child-b",
				CreatedBy:     "ako-cluster-a",
				VHParentVSRef: "https://controller.example/api/virtualservice/vs-parent",
				Markers: []avi.Marker{
					{Key: "Namespace", Values: []string{"team-b"}},
					{Key: "ServiceName", Values: []string{"svc-b"}},
				},
			},
		},
	}

	got := enrichVsVipInventory(items, vsItems)
	mi := avi.ParseObjectMetadata(got[0].Config.Markers, got[0].Config.ServiceMetadata)
	if mi.Namespace != "" || mi.ServiceName != "" {
		t.Fatalf("mixed VsVip metadata = %#v, want empty namespace/service", mi)
	}
	if !avi.IsAKOManaged(got[0].Config.CreatedBy) {
		t.Fatalf("CreatedBy = %q, want AKO-managed value", got[0].Config.CreatedBy)
	}
	if got := len(got[0].Config.VirtualServices); got != 3 {
		t.Fatalf("linked virtual services = %d, want 3", got)
	}
}
