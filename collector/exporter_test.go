package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

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
		"cluster",
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
		"cluster",
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
