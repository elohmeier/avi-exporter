package avi

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (errReadCloser) Close() error {
	return nil
}

func newAviTestClient(t *testing.T, h http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)

	client, err := NewClient(server.URL+"/", "admin", "secret", "30.2.1", false, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client, server
}

func writeLoginCookies(w http.ResponseWriter, csrf, session string) {
	http.SetCookie(w, &http.Cookie{Name: "csrftoken", Value: csrf})
	http.SetCookie(w, &http.Cookie{Name: "sessionid", Value: session})
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func writeLoginTenants(w http.ResponseWriter, csrf, session string) {
	http.SetCookie(w, &http.Cookie{Name: "csrftoken", Value: csrf})
	http.SetCookie(w, &http.Cookie{Name: "sessionid", Value: session})
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"tenants":[{"uuid":"tenant-a-uuid","name":"tenant-a"},{"uuid":"tenant-b-uuid","name":"tenant-b"}]}`))
}

func TestNewClientTLSOptions(t *testing.T) {
	insecure, err := NewClient(" https://avi.example.com/ ", "u", "p", "v", true, "", nil)
	if err != nil {
		t.Fatalf("NewClient ignore cert: %v", err)
	}
	if insecure.baseURL != "https://avi.example.com" {
		t.Fatalf("baseURL = %q, want trimmed URL", insecure.baseURL)
	}
	if tr := insecure.client.Transport.(*http.Transport); tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("ignoreCert did not configure InsecureSkipVerify")
	}

	if _, err := NewClient("https://avi.example.com", "u", "p", "v", false, "/does/not/exist", nil); err == nil {
		t.Fatalf("NewClient accepted missing CA file")
	}

	badCA := t.TempDir() + "/bad.pem"
	if err := os.WriteFile(badCA, []byte("not a cert"), 0o600); err != nil {
		t.Fatalf("write bad CA: %v", err)
	}
	if _, err := NewClient("https://avi.example.com", "u", "p", "v", false, badCA, nil); err == nil {
		t.Fatalf("NewClient accepted invalid CA file")
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).Certificate().Raw})
	caFile := t.TempDir() + "/ca.pem"
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	withCA, err := NewClient("https://avi.example.com", "u", "p", "v", false, caFile, nil)
	if err != nil {
		t.Fatalf("NewClient CA file: %v", err)
	}
	if tr := withCA.client.Transport.(*http.Transport); tr.TLSClientConfig == nil || tr.TLSClientConfig.RootCAs == nil {
		t.Fatalf("CA file did not configure RootCAs")
	}

	emptyPool := x509.NewCertPool()
	if emptyPool == nil {
		t.Fatalf("x509.NewCertPool returned nil")
	}
}

func TestLoginSuccessNoopAndCloseIdle(t *testing.T) {
	var logins atomic.Int64
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/login" {
			http.NotFound(w, r)
			return
		}
		logins.Add(1)
		if r.Method != http.MethodPost {
			t.Fatalf("login method = %s", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q", got)
		}
		if got := r.Header.Get("Referer"); got != server.URL+"/" {
			t.Fatalf("Referer = %q", got)
		}
		if got := r.Header.Get("X-Avi-Version"); got != "30.2.1" {
			t.Fatalf("X-Avi-Version = %q", got)
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode login body: %v", err)
		}
		if payload["username"] != "admin" || payload["password"] != "secret" {
			t.Fatalf("login payload = %#v", payload)
		}
		writeLoginCookies(w, "csrf-1", "session-1")
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(server.URL, "admin", "secret", "30.2.1", false, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.Login(context.Background()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if !client.hasSession() {
		t.Fatalf("hasSession() = false after login")
	}
	if err := client.Login(context.Background()); err != nil {
		t.Fatalf("second Login: %v", err)
	}
	if got := logins.Load(); got != 1 {
		t.Fatalf("login requests = %d, want 1", got)
	}
	client.CloseIdleConnections()
}

func TestLoginTenantsFromLoginResponse(t *testing.T) {
	client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/login" {
			http.NotFound(w, r)
			return
		}
		writeLoginTenants(w, "csrf", "session")
	})

	tenants, err := client.LoginTenants(context.Background())
	if err != nil {
		t.Fatalf("LoginTenants: %v", err)
	}
	if got, want := []string{tenants[0].Name, tenants[1].Name}, []string{"tenant-a", "tenant-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("login tenants = %#v, want %#v", got, want)
	}

	tenants[0].Name = "mutated"
	again, err := client.LoginTenants(context.Background())
	if err != nil {
		t.Fatalf("second LoginTenants: %v", err)
	}
	if again[0].Name != "tenant-a" {
		t.Fatalf("LoginTenants returned mutable client state: %#v", again)
	}
}

func TestLoginErrors(t *testing.T) {
	t.Run("request build", func(t *testing.T) {
		client, err := NewClient("http://[::1", "u", "p", "", false, "", nil)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		if err := client.Login(context.Background()); err == nil {
			t.Fatalf("Login accepted invalid URL")
		}
	})

	t.Run("transport", func(t *testing.T) {
		client, err := NewClient("http://avi.example", "u", "p", "", false, "", nil)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		client.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("boom")
		})
		if err := client.Login(context.Background()); err == nil {
			t.Fatalf("Login succeeded with transport error")
		}
	})

	t.Run("status", func(t *testing.T) {
		client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, strings.Repeat("x", 250), http.StatusForbidden)
		})
		if err := client.Login(context.Background()); err == nil || !strings.Contains(err.Error(), "...") {
			t.Fatalf("Login status error = %v, want truncated body", err)
		}
	})

	t.Run("missing cookies", func(t *testing.T) {
		client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		})
		if err := client.Login(context.Background()); err == nil {
			t.Fatalf("Login accepted response without cookies")
		}
	})
}

func TestGetPostRetryAndCookieRefresh(t *testing.T) {
	var loginCount atomic.Int64
	var apiCount atomic.Int64
	client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login":
			n := loginCount.Add(1)
			writeLoginCookies(w, "csrf-"+string(rune('0'+n)), "session-"+string(rune('0'+n)))
		case "/api/resource":
			n := apiCount.Add(1)
			if r.Method != http.MethodGet {
				t.Fatalf("method = %s, want GET", r.Method)
			}
			if got := r.URL.Query().Get("existing"); got != "1" {
				t.Fatalf("existing query = %q", got)
			}
			if got := r.URL.Query().Get("extra"); got != "2" {
				t.Fatalf("extra query = %q", got)
			}
			if got := r.Header.Get("X-Avi-Tenant"); got != "tenant-a" {
				t.Fatalf("tenant header = %q", got)
			}
			if n == 1 {
				if got := r.Header.Get("X-CSRFToken"); got != "csrf-1" {
					t.Fatalf("initial csrf = %q", got)
				}
				http.Error(w, "expired", 419)
				return
			}
			if got := r.Header.Get("X-CSRFToken"); got != "csrf-2" {
				t.Fatalf("retry csrf = %q", got)
			}
			http.SetCookie(w, &http.Cookie{Name: "csrftoken", Value: "csrf-3"})
			http.SetCookie(w, &http.Cookie{Name: "avi-sessionid", Value: "session-3"})
			_, _ = w.Write([]byte(`{"value":"ok"}`))
		case "/api/post":
			if r.Method != http.MethodPost {
				t.Fatalf("method = %s, want POST", r.Method)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("post Content-Type = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode post body: %v", err)
			}
			if body["hello"] != "world" {
				t.Fatalf("post body = %#v", body)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})

	var out struct {
		Value string `json:"value"`
	}
	query := url.Values{"extra": []string{"2"}}
	if err := client.Get(context.Background(), "/api/resource?existing=1", &out, RequestOptions{Tenant: "tenant-a", Query: query}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if out.Value != "ok" {
		t.Fatalf("Get value = %q", out.Value)
	}
	if client.csrfToken != "csrf-3" || client.sessionID != "session-3" {
		t.Fatalf("refreshed session = %q/%q", client.csrfToken, client.sessionID)
	}
	if err := client.Post(context.Background(), "/api/post", nil, RequestOptions{Body: map[string]string{"hello": "world"}}); err != nil {
		t.Fatalf("Post: %v", err)
	}
}

func TestGetRaw(t *testing.T) {
	client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login":
			writeLoginCookies(w, "csrf", "session")
		case "/api/plain":
			if got := r.Header.Get("Accept"); got != "application/json, */*;q=0.5" {
				t.Fatalf("Accept = %q, want broad raw-response accept", got)
			}
			if got := r.URL.Query().Get("tenant"); got != "admin,tenant-a" {
				t.Fatalf("tenant query = %q", got)
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("plain response"))
		default:
			http.NotFound(w, r)
		}
	})

	query := url.Values{"tenant": []string{"admin,tenant-a"}}
	raw, err := client.GetRaw(context.Background(), "/api/plain", RequestOptions{Query: query})
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	if string(raw) != "plain response" {
		t.Fatalf("GetRaw body = %q", raw)
	}
}

func TestRequestErrors(t *testing.T) {
	t.Run("login required", func(t *testing.T) {
		client, _ := NewClient("http://bad.example", "u", "p", "", false, "", nil)
		client.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("login failed")
		})
		if err := client.Get(context.Background(), "/api/resource", nil, RequestOptions{}); err == nil {
			t.Fatalf("Get succeeded with login transport error")
		}
	})

	t.Run("marshal body", func(t *testing.T) {
		client, _ := NewClient("http://avi.example", "u", "p", "", false, "", nil)
		client.sessionID = "session"
		if err := client.Post(context.Background(), "/api/resource", nil, RequestOptions{Body: make(chan int)}); err == nil {
			t.Fatalf("Post accepted unmarshalable body")
		}
	})

	t.Run("build request", func(t *testing.T) {
		client, _ := NewClient("http://[::1", "u", "p", "", false, "", nil)
		client.sessionID = "session"
		if err := client.Get(context.Background(), "/api/resource", nil, RequestOptions{}); err == nil {
			t.Fatalf("Get accepted invalid URL")
		}
	})

	t.Run("transport", func(t *testing.T) {
		client, _ := NewClient("http://avi.example", "u", "p", "", false, "", nil)
		client.sessionID = "session"
		client.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("boom")
		})
		if err := client.Get(context.Background(), "/api/resource", nil, RequestOptions{}); err == nil {
			t.Fatalf("Get succeeded with transport error")
		}
	})

	t.Run("read response", func(t *testing.T) {
		client, _ := NewClient("http://avi.example", "u", "p", "", false, "", nil)
		client.sessionID = "session"
		client.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: errReadCloser{}, Header: http.Header{}}, nil
		})
		if err := client.Get(context.Background(), "/api/resource", nil, RequestOptions{}); err == nil {
			t.Fatalf("Get succeeded with unreadable response")
		}
	})

	t.Run("status", func(t *testing.T) {
		client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				writeLoginCookies(w, "csrf", "session")
				return
			}
			http.Error(w, strings.Repeat("x", 250), http.StatusInternalServerError)
		})
		if err := client.Get(context.Background(), "/api/fail", nil, RequestOptions{}); err == nil || !strings.Contains(err.Error(), "...") {
			t.Fatalf("Get status error = %v, want truncated body", err)
		}
	})

	t.Run("unmarshal", func(t *testing.T) {
		client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				writeLoginCookies(w, "csrf", "session")
				return
			}
			_, _ = w.Write([]byte(strings.Repeat("x", 1100) + `not-json`))
		})
		var out struct{}
		err := client.Get(context.Background(), "/api/bad-json", &out, RequestOptions{})
		if err == nil {
			t.Fatalf("Get accepted invalid JSON")
		}
		if !strings.Contains(err.Error(), "response excerpt near byte") || !strings.Contains(err.Error(), "...") {
			t.Fatalf("Get unmarshal error = %v, want bounded response excerpt", err)
		}
	})
}

func TestAuthRetryFailures(t *testing.T) {
	t.Run("relogin fails", func(t *testing.T) {
		var logins atomic.Int64
		client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				if logins.Add(1) == 1 {
					writeLoginCookies(w, "csrf", "session")
					return
				}
				http.Error(w, "no", http.StatusForbidden)
				return
			}
			http.Error(w, "expired", 419)
		})
		if err := client.Get(context.Background(), "/api/resource", nil, RequestOptions{}); err == nil || !strings.Contains(err.Error(), "re-login") {
			t.Fatalf("Get error = %v, want re-login failure", err)
		}
	})

	t.Run("retry still unauthorized", func(t *testing.T) {
		client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				writeLoginCookies(w, "csrf", "session")
				return
			}
			http.Error(w, "expired", http.StatusUnauthorized)
		})
		if err := client.Get(context.Background(), "/api/resource", nil, RequestOptions{}); err == nil || !strings.Contains(err.Error(), "after re-login") {
			t.Fatalf("Get error = %v, want after re-login failure", err)
		}
	})
}

func TestCollectMetrics(t *testing.T) {
	client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			writeLoginCookies(w, "csrf", "session")
			return
		}
		if r.URL.Path != "/api/analytics/metrics/collection" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("X-Avi-Tenant"); got != "tenant-a" {
			t.Fatalf("tenant = %q", got)
		}
		_, _ = w.Write([]byte(`{}`))
	})

	empty, err := client.CollectMetrics(context.Background(), "tenant-a", MetricsCollectionRequest{})
	if err != nil {
		t.Fatalf("empty CollectMetrics: %v", err)
	}
	if len(empty.Series) != 0 {
		t.Fatalf("empty CollectMetrics series = %#v", empty.Series)
	}

	resp, err := client.CollectMetrics(context.Background(), "tenant-a", MetricsCollectionRequest{
		MetricRequests: []MetricQuery{{MetricID: "metric.one"}},
	})
	if err != nil {
		t.Fatalf("CollectMetrics: %v", err)
	}
	if len(resp.Series) != 0 {
		t.Fatalf("CollectMetrics nil series normalized to %#v, want empty", resp.Series)
	}
}

func TestCollectMetricsError(t *testing.T) {
	client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			writeLoginCookies(w, "csrf", "session")
			return
		}
		http.Error(w, "bad", http.StatusInternalServerError)
	})
	_, err := client.CollectMetrics(context.Background(), "tenant-a", MetricsCollectionRequest{
		MetricRequests: []MetricQuery{{MetricID: "metric.one"}},
	})
	if err == nil {
		t.Fatalf("CollectMetrics succeeded with server error")
	}
}

func TestInventoryWrappers(t *testing.T) {
	client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			writeLoginCookies(w, "csrf", "session")
			return
		}
		switch r.URL.Path {
		case "/api/tenant":
			if got := r.Header.Get("X-Avi-Tenant"); got != "*" {
				t.Fatalf("tenant discovery header = %q", got)
			}
			if got := r.URL.Query().Get("page_size"); got != "200" {
				t.Fatalf("page_size = %q", got)
			}
			if r.URL.Query().Get("page") == "1" {
				_, _ = w.Write([]byte(`{"results":[{"uuid":"tenant-a","name":"tenant-a"}],"next":"/api/tenant?page=2"}`))
				return
			}
			_, _ = w.Write([]byte(`{"results":[{"uuid":"tenant-b","name":"tenant-b"}]}`))
		case "/api/cluster/runtime":
			if got := r.Header.Get("X-Avi-Tenant"); got != "admin" {
				t.Fatalf("cluster runtime tenant = %q", got)
			}
			_, _ = w.Write([]byte(`{"cluster_state":{"state":"CLUSTER_UP_NO_HA","progress":100},"node_states":[{"name":"node-a","state":"CLUSTER_ACTIVE","role":"CLUSTER_LEADER"}]}`))
		case "/api/cluster":
			_, _ = w.Write([]byte(`{"uuid":"cluster-1","name":"cluster","nodes":[{"name":"node-a","ip":{"addr":"192.0.2.10","type":"V4"},"ip6":{"addr":"2001:db8::10","type":"V6"},"public_ip_or_name":{"addr":"controller.example.com"},"vm_hostname":"controller-a","vm_name":"avi-controller-a","vm_uuid":"vm-1"}]}`))
		case "/api/virtualservice-inventory":
			requireInventoryQuery(t, r)
			_, _ = w.Write([]byte(`{"results":[{"config":{"uuid":"vs-1","name":"vs-1"},"runtime":{"oper_status":{"state":"OPER_UP"}}}]}`))
		case "/api/virtualservice":
			if got := r.Header.Get("X-Avi-Tenant"); got != "tenant-a" {
				t.Fatalf("VS config tenant = %q", got)
			}
			fields := r.URL.Query().Get("fields")
			for _, field := range []string{"service_metadata", "vsvip_ref", "pool_ref", "pool_group_ref"} {
				if !strings.Contains(fields, field) {
					t.Fatalf("VS config fields %q omit %q", fields, field)
				}
			}
			_, _ = w.Write([]byte(`{"results":[{"uuid":"vs-1","name":"vs-1","service_metadata":"{\"namespace\":\"team-a\",\"hostnames\":[\"app.example.com\"]}"}]}`))
		case "/api/pool-inventory":
			requireInventoryQuery(t, r)
			_, _ = w.Write([]byte(`{"results":[{"config":{"uuid":"pool-1","name":"pool-1"},"runtime":{"oper_status":{"state":"OPER_UP"}}}]}`))
		case "/api/pool":
			if got := r.Header.Get("X-Avi-Tenant"); got != "tenant-a" {
				t.Fatalf("pool config tenant = %q", got)
			}
			if got := r.URL.Query().Get("fields"); !strings.Contains(got, "service_metadata") {
				t.Fatalf("pool config fields = %q", got)
			}
			_, _ = w.Write([]byte(`{"results":[{"uuid":"pool-1","name":"pool-1","service_metadata":{"namespace":"team-a","ingress_name":"ing-a"}}]}`))
		case "/api/serviceengine-inventory":
			requireInventoryQuery(t, r)
			if got := r.Header.Get("X-Avi-Tenant"); got != "admin" {
				t.Fatalf("SE inventory tenant = %q", got)
			}
			_, _ = w.Write([]byte(`{"results":[{"config":{"uuid":"se-1","name":"se-1"},"runtime":{"oper_status":{"state":"OPER_UP"}}}]}`))
		case "/api/serviceengine":
			if got := r.Header.Get("X-Avi-Tenant"); got != "admin" {
				t.Fatalf("SE config tenant = %q", got)
			}
			if got := r.URL.Query().Get("include_name"); got != "true" {
				t.Fatalf("SE config include_name = %q", got)
			}
			fields := r.URL.Query().Get("fields")
			for _, field := range []string{"cloud_ref", "mgmt_vnic", "data_vnics"} {
				if !strings.Contains(fields, field) {
					t.Fatalf("SE config fields %q omit %q", fields, field)
				}
			}
			_, _ = w.Write([]byte(`{"results":[{"uuid":"se-1","name":"se-1","cloud_ref":"https://controller/api/cloud/cloud-1#cloud-a","mgmt_vnic":{"if_name":"Management","vnic_networks":[{"ip":{"ip_addr":{"addr":"192.0.2.20","type":"V4"},"mask":24}}]}}]}`))
		case "/api/vsvip-inventory":
			requireInventoryQuery(t, r)
			_, _ = w.Write([]byte(`{"results":[{"config":{"uuid":"vip-1","name":"vip-1"},"runtime":null}]}`))
		case "/api/poolgroup-inventory":
			if got := r.URL.Query().Get("include_name"); got != "true" {
				t.Fatalf("pool group include_name = %q", got)
			}
			_, _ = w.Write([]byte(`{"results":[{"config":{"uuid":"pg-1","name":"pg-1"}}]}`))
		case "/api/gslbservice-inventory":
			requireInventoryQuery(t, r)
			_, _ = w.Write([]byte(`{"results":[{"config":{"uuid":"gslb-1","name":"gslb-1"},"runtime":{"oper_status":{"state":"OPER_UP"}}}]}`))
		case "/api/pool/null/runtime/server/detail/":
			_, _ = w.Write([]byte(`null`))
		case "/api/pool/array/runtime/server/detail/":
			_, _ = w.Write([]byte(`[{"ip_addr":{"addr":"10.0.0.1"},"port":80,"oper_status":{"state":"OPER_UP"}}]`))
		case "/api/pool/wrapped/runtime/server/detail/":
			_, _ = w.Write([]byte(`{"server":[{"ip_addr":{"addr":"10.0.0.2"},"port":443,"oper_status":{"state":"OPER_DOWN"}}]}`))
		case "/api/pool/paged/runtime/server/detail/":
			if got := r.URL.Query().Get("page_size"); got != "200" {
				t.Fatalf("pool detail page_size = %q", got)
			}
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"results":[{"ip_addr":{"addr":"10.0.0.4"},"port":8443,"oper_status":{"state":"OPER_DOWN"}}]}`))
				return
			}
			_, _ = w.Write([]byte(poolRuntimeDetailResults(200, 8080, "/api/pool/paged/runtime/server/detail/?page=2")))
		case "/api/pool/lying-next/runtime/server/detail/":
			_, _ = w.Write([]byte(`{"results":[{"ip_addr":{"addr":"10.0.0.5"},"port":9443,"oper_status":{"state":"OPER_UP"}}],"next":"/api/pool/lying-next/runtime/server/detail/?page=2"}`))
		case "/api/pool/bad/runtime/server/detail/":
			_, _ = w.Write([]byte(`"bad"`))
		default:
			http.NotFound(w, r)
		}
	})

	tenants, err := client.ListTenants(context.Background())
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if got, want := []string{tenants[0].Name, tenants[1].Name}, []string{"tenant-a", "tenant-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tenants = %#v, want %#v", got, want)
	}
	if rt, err := client.GetClusterRuntime(context.Background()); err != nil || rt.ClusterState.Progress != 100 {
		t.Fatalf("GetClusterRuntime = %#v, %v", rt, err)
	}
	if cluster, err := client.GetCluster(context.Background()); err != nil || cluster.UUID != "cluster-1" || cluster.Nodes[0].VMUUID != "vm-1" || cluster.Nodes[0].IP6.Addr != "2001:db8::10" {
		t.Fatalf("GetCluster = %#v, %v", cluster, err)
	}
	if items, err := client.ListVSInventory(context.Background(), "tenant-a"); err != nil || len(items) != 1 {
		t.Fatalf("ListVSInventory = %d, %v", len(items), err)
	}
	if items, err := client.ListVSConfig(context.Background(), "tenant-a"); err != nil || len(items) != 1 || items[0].ServiceMetadata.Namespace != "team-a" {
		t.Fatalf("ListVSConfig = %#v, %v", items, err)
	}
	if items, err := client.ListPoolInventory(context.Background(), "tenant-a"); err != nil || len(items) != 1 {
		t.Fatalf("ListPoolInventory = %d, %v", len(items), err)
	}
	if items, err := client.ListPoolConfig(context.Background(), "tenant-a"); err != nil || len(items) != 1 || items[0].ServiceMetadata.IngressName != "ing-a" {
		t.Fatalf("ListPoolConfig = %#v, %v", items, err)
	}
	if items, err := client.ListSEInventory(context.Background()); err != nil || len(items) != 1 {
		t.Fatalf("ListSEInventory = %d, %v", len(items), err)
	}
	if items, err := client.ListSEConfig(context.Background()); err != nil || len(items) != 1 || items[0].MgmtVNIC == nil || items[0].MgmtVNIC.VNICNetworks[0].IP.Mask == nil || *items[0].MgmtVNIC.VNICNetworks[0].IP.Mask != 24 {
		t.Fatalf("ListSEConfig = %#v, %v", items, err)
	}
	if items, err := client.ListVsVipInventory(context.Background(), "tenant-a"); err != nil || len(items) != 1 {
		t.Fatalf("ListVsVipInventory = %d, %v", len(items), err)
	}
	if items, err := client.ListPoolGroupInventory(context.Background(), "tenant-a"); err != nil || len(items) != 1 {
		t.Fatalf("ListPoolGroupInventory = %d, %v", len(items), err)
	}
	if items, err := client.ListGslbServiceInventory(context.Background(), "tenant-a"); err != nil || len(items) != 1 {
		t.Fatalf("ListGslbServiceInventory = %d, %v", len(items), err)
	}
	if items, err := client.GetPoolRuntimeDetail(context.Background(), "tenant-a", "null"); err != nil || items != nil {
		t.Fatalf("GetPoolRuntimeDetail null = %#v, %v", items, err)
	}
	if items, err := client.GetPoolRuntimeDetail(context.Background(), "tenant-a", "array"); err != nil || len(items) != 1 || items[0].Port != 80 {
		t.Fatalf("GetPoolRuntimeDetail array = %#v, %v", items, err)
	}
	if items, err := client.GetPoolRuntimeDetail(context.Background(), "tenant-a", "wrapped"); err != nil || len(items) != 1 || items[0].Port != 443 {
		t.Fatalf("GetPoolRuntimeDetail wrapped = %#v, %v", items, err)
	}
	if items, err := client.GetPoolRuntimeDetail(context.Background(), "tenant-a", "paged"); err != nil || len(items) != 201 || items[200].Port != 8443 {
		t.Fatalf("GetPoolRuntimeDetail paged = %#v, %v", items, err)
	}
	if items, err := client.GetPoolRuntimeDetail(context.Background(), "tenant-a", "lying-next"); err != nil || len(items) != 1 || items[0].Port != 9443 {
		t.Fatalf("GetPoolRuntimeDetail lying next = %#v, %v", items, err)
	}
	if _, err := client.GetPoolRuntimeDetail(context.Background(), "tenant-a", "bad"); err == nil {
		t.Fatalf("GetPoolRuntimeDetail accepted invalid shape")
	}
}

func poolRuntimeDetailResults(count, startPort int, next string) string {
	var b strings.Builder
	b.WriteString(`{"results":[`)
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"ip_addr":{"addr":"10.0.1.%d"},"port":%d,"oper_status":{"state":"OPER_UP"}}`, i+1, startPort+i)
	}
	b.WriteByte(']')
	if next != "" {
		fmt.Fprintf(&b, `,"next":%q`, next)
	}
	b.WriteByte('}')
	return b.String()
}

func requireInventoryQuery(t *testing.T, r *http.Request) {
	t.Helper()
	q := r.URL.Query()
	if got := q.Get("include_name"); got != "true" {
		t.Fatalf("%s include_name = %q", r.URL.Path, got)
	}
	if got := q.Get("include"); got != "runtime,health_score" {
		t.Fatalf("%s include = %q", r.URL.Path, got)
	}
	if got := q.Get("page"); got == "" {
		t.Fatalf("%s missing page", r.URL.Path)
	}
	if got := q.Get("page_size"); got != "200" {
		t.Fatalf("%s page_size = %q", r.URL.Path, got)
	}
}

func TestInventoryWrapperErrors(t *testing.T) {
	client, _ := newAviTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			writeLoginCookies(w, "csrf", "session")
			return
		}
		http.Error(w, "no", http.StatusInternalServerError)
	})
	if _, err := client.ListTenants(context.Background()); err == nil {
		t.Fatalf("ListTenants succeeded with server error")
	}
	if _, err := client.GetClusterRuntime(context.Background()); err == nil {
		t.Fatalf("GetClusterRuntime succeeded with server error")
	}
	if _, err := client.GetCluster(context.Background()); err == nil {
		t.Fatalf("GetCluster succeeded with server error")
	}
	if _, err := client.ListSEConfig(context.Background()); err == nil {
		t.Fatalf("ListSEConfig succeeded with server error")
	}
	if _, err := client.GetPoolRuntimeDetail(context.Background(), "tenant-a", "pool-1"); err == nil {
		t.Fatalf("GetPoolRuntimeDetail succeeded with server error")
	}
}

func TestMetricSeriesLast(t *testing.T) {
	var empty MetricSeries
	if got, ok := empty.Last(); got != 0 || ok {
		t.Fatalf("empty Last() = %v, %v; want 0, false", got, ok)
	}
	series := MetricSeries{Data: []DataPoint{{Value: 1}, {Value: 2}}}
	if got, ok := series.Last(); got != 2 || !ok {
		t.Fatalf("Last() = %v, %v; want 2, true", got, ok)
	}
}

func TestMarkersAndRefUUID(t *testing.T) {
	info := ParseMarkers([]Marker{
		{Key: "clustername", Values: []string{"cluster-a"}},
		{Key: "Namespace", Values: []string{"ns"}},
		{Key: "ServiceName", Values: []string{"svc"}},
		{Key: "IngressName", Values: []string{"ing"}},
		{Key: "GatewayName", Values: []string{"gw"}},
		{Key: "Host", Values: []string{"one.example", "two.example"}},
		{Key: "Path", Values: []string{"/"}},
		{Key: "InfrasettingName", Values: []string{"infra"}},
		{Key: "ignored", Values: []string{"ignored"}},
	})
	want := MarkerInfo{
		ClusterName:  "cluster-a",
		Namespace:    "ns",
		ServiceName:  "svc",
		IngressName:  "ing",
		GatewayName:  "gw",
		Host:         "one.example,two.example",
		Path:         "/",
		InfraSetting: "infra",
	}
	if !reflect.DeepEqual(info, want) {
		t.Fatalf("ParseMarkers() = %#v, want %#v", info, want)
	}
	if !IsAKOManaged("ako-cluster-a") || IsAKOManaged("manual") {
		t.Fatalf("IsAKOManaged returned unexpected result")
	}
	for input, want := range map[string]string{
		"":                                   "",
		"pool-1":                             "pool-1",
		"https://controller/api/pool/pool-1": "pool-1",
		"https://controller/api/pool/pool-1#poolone": "pool-1",
	} {
		if got := RefUUID(input); got != want {
			t.Fatalf("RefUUID(%q) = %q, want %q", input, got, want)
		}
	}
	for input, want := range map[string]string{
		"":                                     "",
		"https://controller/api/cloud/cloud-1": "",
		"https://controller/api/cloud/cloud-1#cloud-primary": "cloud-primary",
	} {
		if got := RefName(input); got != want {
			t.Fatalf("RefName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestObjectMetadataFallbacks(t *testing.T) {
	var fromString ServiceMetadata
	if err := json.Unmarshal([]byte(`"{\"namespace\":\"team-a\",\"namespace_ingress_name\":[\"team-a/ing-a\"],\"hostnames\":[\"app.example.com\"]}"`), &fromString); err != nil {
		t.Fatalf("unmarshal string service_metadata: %v", err)
	}
	info := ParseObjectMetadata(nil, fromString)
	if info.Namespace != "team-a" || info.IngressName != "ing-a" || info.Host != "app.example.com" {
		t.Fatalf("metadata info = %#v", info)
	}

	var fromObject ServiceMetadata
	if err := json.Unmarshal([]byte(`{"namespace_svc_name":["team-b/svc-b"],"hostnames":"api.example.com"}`), &fromObject); err != nil {
		t.Fatalf("unmarshal object service_metadata: %v", err)
	}
	info = ParseObjectMetadata([]Marker{{Key: "Namespace", Values: []string{"marker-ns"}}}, fromObject)
	if info.Namespace != "marker-ns" || info.ServiceName != "svc-b" || info.Host != "api.example.com" {
		t.Fatalf("merged metadata info = %#v", info)
	}

	var withObjectField ServiceMetadata
	if err := json.Unmarshal([]byte(`"{\"namespace_svc_name\":[\"team-c/svc-c\"],\"host_namespace_ingress_name\":{\"app.example.com\":\"team-c/ing-c\"}}"`), &withObjectField); err != nil {
		t.Fatalf("unmarshal object host_namespace_ingress_name: %v", err)
	}
	info = ParseObjectMetadata(nil, withObjectField)
	if info.Namespace != "team-c" || info.ServiceName != "svc-c" {
		t.Fatalf("metadata with object field info = %#v", info)
	}
}
