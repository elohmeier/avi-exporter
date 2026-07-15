package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

const allModulesCSV = "cluster,cluster_inventory,controller_metrics,se_config,se_inventory,se_metrics,vs_inventory,vs_metrics,pool_inventory,pool_metrics,pool_members,vsvip,pool_group,gslb,topology"

type exitCalled struct {
	code int
}

func clearMainEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AVI_URL", "AVI_USERNAME", "AVI_PASSWORD", "AVI_TENANTS", "AVI_API_VERSION",
		"AVI_IGNORE_CERT", "AVI_CA_FILE", "AVI_LABELS", "AVI_DISABLED_MODULES",
	} {
		t.Setenv(key, "")
	}
}

func TestRunVersionAndFlagParse(t *testing.T) {
	clearMainEnv(t)
	oldVersion, oldBuild := version, build
	version, build = "1.2.3", "abc"
	t.Cleanup(func() {
		version, build = oldVersion, oldBuild
	})

	var out, stderr bytes.Buffer
	if code := run([]string{"-version"}, &out, &stderr, nil); code != 0 {
		t.Fatalf("run -version code = %d, want 0", code)
	}
	if got := out.String(); !strings.Contains(got, "Avi-Load-Balancer-Exporter v1.2.3 build abc") {
		t.Fatalf("version output = %q", got)
	}

	out.Reset()
	stderr.Reset()
	if code := run([]string{"-unknown"}, &out, &stderr, nil); code != 2 {
		t.Fatalf("run invalid flag code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("invalid flag stderr = %q", stderr.String())
	}
}

func TestDockerfileIncludesSystemCARoots(t *testing.T) {
	raw, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	dockerfile := string(raw)
	if !strings.Contains(dockerfile, "ca-certificates.crt") || !strings.Contains(dockerfile, "COPY --from=") {
		t.Fatalf("Dockerfile does not copy system CA roots into final image:\n%s", dockerfile)
	}
}

func TestRunValidationErrors(t *testing.T) {
	clearMainEnv(t)
	var out, stderr bytes.Buffer
	if code := run(nil, &out, &stderr, nil); code != 1 {
		t.Fatalf("run without URL code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "-url") {
		t.Fatalf("missing URL usage did not mention -url: %q", stderr.String())
	}

	clearMainEnv(t)
	t.Setenv("AVI_URL", "https://controller.example")
	out.Reset()
	stderr.Reset()
	if code := run(nil, &out, &stderr, nil); code != 1 {
		t.Fatalf("run without credentials code = %d, want 1", code)
	}

	clearMainEnv(t)
	t.Setenv("AVI_URL", "https://controller.example")
	t.Setenv("AVI_USERNAME", "user")
	t.Setenv("AVI_PASSWORD", "pass")
	out.Reset()
	stderr.Reset()
	if code := run([]string{"-parallelism", "0"}, &out, &stderr, nil); code != 1 {
		t.Fatalf("run invalid exporter code = %d, want 1", code)
	}

	clearMainEnv(t)
	t.Setenv("AVI_URL", "https://controller.example")
	t.Setenv("AVI_USERNAME", "user")
	t.Setenv("AVI_PASSWORD", "")
	out.Reset()
	stderr.Reset()
	served := false
	if code := run([]string{"-disabled-modules", allModulesCSV}, &out, &stderr, func(*http.Server) error {
		served = true
		return nil
	}); code != 1 {
		t.Fatalf("run without password code = %d, want 1", code)
	}
	if served {
		t.Fatalf("run started server without password")
	}
}

func TestRunRejectsInvalidCustomLabels(t *testing.T) {
	for _, labels := range []string{"tenant=override", "bad-label=value"} {
		t.Run(labels, func(t *testing.T) {
			clearMainEnv(t)
			t.Setenv("AVI_URL", "https://controller.example")
			t.Setenv("AVI_USERNAME", "user")
			t.Setenv("AVI_PASSWORD", "pass")

			var out, stderr bytes.Buffer
			served := false
			code, panicValue := runCatchingPanic([]string{
				"-labels", labels,
				"-disabled-modules", allModulesCSV,
			}, &out, &stderr, func(*http.Server) error {
				served = true
				return nil
			})
			if panicValue != nil {
				t.Fatalf("run panicked for labels %q: %v", labels, panicValue)
			}
			if code != 1 {
				t.Fatalf("run with labels %q code = %d, want 1", labels, code)
			}
			if served {
				t.Fatalf("run started server with invalid labels %q", labels)
			}
			if !strings.Contains(out.String(), "invalid label") {
				t.Fatalf("invalid label log missing for %q: stdout=%q stderr=%q", labels, out.String(), stderr.String())
			}
		})
	}
}

func runCatchingPanic(args []string, stdout, stderr io.Writer, serve func(*http.Server) error) (code int, panicValue any) {
	defer func() {
		panicValue = recover()
	}()
	code = run(args, stdout, stderr, serve)
	return code, nil
}

func TestRunStartsServerAndHandlers(t *testing.T) {
	clearMainEnv(t)
	t.Setenv("AVI_URL", "https://controller.example")
	t.Setenv("AVI_USERNAME", "user")
	t.Setenv("AVI_PASSWORD", "pass")
	t.Setenv("AVI_IGNORE_CERT", "1")
	t.Setenv("AVI_CA_FILE", "/tmp/ignored-ca.pem")
	t.Setenv("AVI_LABELS", "env=prod")
	t.Setenv("AVI_DISABLED_MODULES", allModulesCSV)

	var out, stderr bytes.Buffer
	var sawServer bool
	code := run([]string{
		"-bind-port", "12345",
		"-labels", "site=eu",
		"-disabled-modules", "vs_metrics",
		"-metrics-step", "7",
		"-metrics-limit", "2",
		"-debug",
	}, &out, &stderr, func(srv *http.Server) error {
		sawServer = true
		if srv.Addr != ":12345" {
			t.Fatalf("server addr = %q, want :12345", srv.Addr)
		}
		for _, path := range []string{"/healthz", "/health", "/", "/readyz", "/debug/cache", "/metrics"} {
			rr := httptest.NewRecorder()
			srv.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
			if rr.Code == 0 {
				t.Fatalf("%s produced no status", path)
			}
		}
		return nil
	})
	if code != 0 {
		t.Fatalf("run success code = %d, want 0; stderr=%q stdout=%q", code, stderr.String(), out.String())
	}
	if !sawServer {
		t.Fatalf("serve hook was not called")
	}
	if !strings.Contains(out.String(), "TLS certificate verification disabled") || !strings.Contains(out.String(), "using custom CA file") {
		t.Fatalf("expected startup logs missing: %q", out.String())
	}
}

func TestRunServerError(t *testing.T) {
	clearMainEnv(t)
	t.Setenv("AVI_URL", "https://controller.example")
	t.Setenv("AVI_USERNAME", "user")
	t.Setenv("AVI_PASSWORD", "pass")
	t.Setenv("AVI_DISABLED_MODULES", allModulesCSV)

	var out, stderr bytes.Buffer
	if code := run(nil, &out, &stderr, func(*http.Server) error {
		return errors.New("listen failed")
	}); code != 1 {
		t.Fatalf("run server error code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "server error") {
		t.Fatalf("server error log missing: %q", out.String())
	}
}

func TestRunDefaultsAndServeHTTPWrapper(t *testing.T) {
	if err := serveHTTP(&http.Server{Addr: "bad-address"}); err == nil {
		t.Fatalf("serveHTTP succeeded with invalid address")
	}

	clearMainEnv(t)
	t.Setenv("AVI_URL", "https://controller.example")
	t.Setenv("AVI_USERNAME", "user")
	t.Setenv("AVI_PASSWORD", "pass")
	t.Setenv("AVI_DISABLED_MODULES", allModulesCSV)

	oldServe := serveHTTP
	serveHTTP = func(*http.Server) error { return nil }
	t.Cleanup(func() {
		serveHTTP = oldServe
	})
	if code := run(nil, nil, nil, nil); code != 0 {
		t.Fatalf("run with default writers/serve code = %d, want 0", code)
	}
}

func TestMainInvokesRunAndExit(t *testing.T) {
	clearMainEnv(t)
	t.Setenv("AVI_URL", "https://controller.example")
	t.Setenv("AVI_USERNAME", "user")
	t.Setenv("AVI_PASSWORD", "pass")
	t.Setenv("AVI_DISABLED_MODULES", allModulesCSV)

	oldArgs := os.Args
	oldExit := exitProcess
	oldServe := serveHTTP
	os.Args = []string{"avi-exporter", "-bind-port", "0"}
	serveHTTP = func(*http.Server) error { return nil }
	exitProcess = func(code int) {
		panic(exitCalled{code: code})
	}
	t.Cleanup(func() {
		os.Args = oldArgs
		exitProcess = oldExit
		serveHTTP = oldServe
	})

	defer func() {
		recovered := recover()
		exit, ok := recovered.(exitCalled)
		if !ok {
			t.Fatalf("main panic = %#v, want exitCalled", recovered)
		}
		if exit.code != 0 {
			t.Fatalf("main exit code = %d, want 0", exit.code)
		}
	}()
	main()
}
