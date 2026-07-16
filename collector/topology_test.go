package collector

import (
	"net/http"
	"testing"

	"github.com/elohmeier/avi-exporter/avi"
)

func TestCollectTopologyUsesPoolReverseVirtualServiceRefs(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"tenant-a"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	const (
		vsUUID   = "virtualservice-policy"
		poolUUID = "pool-policy"
	)
	exp.collectTopology("tenant-a",
		[]avi.VSInventoryItem{{
			Config: avi.VSConfig{
				UUID: vsUUID,
				Name: "vs-policy",
				ServiceMetadata: avi.ServiceMetadata{
					NamespaceSvcName: []string{"namespace-a/service-a"},
				},
			},
		}},
		[]avi.PoolInventoryItem{{
			Config: avi.PoolConfig{UUID: poolUUID, Name: "pool-policy"},
			VirtualServices: []avi.VsRef{
				{Ref: "https://controller.example/api/virtualservice/" + vsUUID + "#vs-policy"},
				{UUID: vsUUID},
				{UUID: "virtualservice-not-in-the-tenant-snapshot"},
			},
		}},
		nil,
		nil,
	)

	edges := metricFamily(t, gatherRegisteredExporter(t, exp), "avi_topology_edge")
	edgeID := "vs:" + vsUUID + "->pool:" + poolUUID
	if got := countMetricsWithLabel(edges, "id", edgeID); got != 1 {
		t.Fatalf("reverse VS to pool edge count = %d, want 1", got)
	}
	if got := metricValueForLabels(t, edges, map[string]string{
		"tenant": "tenant-a",
		"id":     edgeID,
		"source": "vs:" + vsUUID,
		"target": "pool:" + poolUUID,
		"chain":  "namespace-a/service-a",
	}); got != 1 {
		t.Fatalf("reverse VS to pool edge value = %v, want 1", got)
	}
}

func TestCollectTopologyDeduplicatesDirectAndReversePoolEdges(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	exp.collectTopology("admin",
		[]avi.VSInventoryItem{{Config: avi.VSConfig{
			UUID:    "vs-direct",
			Name:    "vs-direct",
			PoolRef: "https://controller.example/api/pool/pool-direct#pool-direct",
		}}},
		[]avi.PoolInventoryItem{{
			Config: avi.PoolConfig{UUID: "pool-direct", Name: "pool-direct"},
			VirtualServices: []avi.VsRef{
				{UUID: "vs-direct"},
				{UUID: "vs-direct"},
			},
		}},
		nil,
		nil,
	)

	edges := metricFamily(t, gatherRegisteredExporter(t, exp), "avi_topology_edge")
	edgeID := "vs:vs-direct->pool:pool-direct"
	if got := countMetricsWithLabel(edges, "id", edgeID); got != 1 {
		t.Fatalf("direct and reverse VS to pool edge count = %d, want 1", got)
	}
}

func TestCollectTopologyDoesNotFlattenKnownPoolGroup(t *testing.T) {
	exp, _ := newEdgeExporter(t, testConfig([]string{"admin"}, allTestModules), func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	exp.collectTopology("admin",
		[]avi.VSInventoryItem{{Config: avi.VSConfig{
			UUID:         "vs-group",
			Name:         "vs-group",
			PoolGroupRef: "https://controller.example/api/poolgroup/poolgroup-a#poolgroup-a",
		}}},
		[]avi.PoolInventoryItem{{
			Config:          avi.PoolConfig{UUID: "pool-a", Name: "pool-a"},
			VirtualServices: []avi.VsRef{{UUID: "vs-group"}},
		}},
		nil,
		nil,
	)

	edges := metricFamily(t, gatherRegisteredExporter(t, exp), "avi_topology_edge")
	if got := countMetricsWithLabel(edges, "id", "vs:vs-group->poolgroup:poolgroup-a"); got != 1 {
		t.Fatalf("VS to pool group edge count = %d, want 1", got)
	}
	if got := countMetricsWithLabel(edges, "id", "vs:vs-group->pool:pool-a"); got != 0 {
		t.Fatalf("flattened VS to pool edge count = %d, want 0", got)
	}
}
