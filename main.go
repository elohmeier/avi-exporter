package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/elohmeier/avi-exporter/collector"
	"github.com/elohmeier/avi-exporter/config"
)

var (
	app         = "Avi-Load-Balancer-Exporter"
	version     string
	build       string
	exitProcess = os.Exit
	serveHTTP   = func(srv *http.Server) error {
		return srv.ListenAndServe()
	}
)

var reservedCustomLabelNames = []string{
	"ako",
	"app_profile_type",
	"chain",
	"color",
	"controller_uuid",
	"enable_state",
	"fqdn",
	"gslbservice",
	"gslbservice_uuid",
	"host",
	"id",
	"ingress",
	"ip",
	"license_state",
	"level",
	"mainstat",
	"module",
	"migrate_state",
	"node",
	"node_type",
	"namespace",
	"pool",
	"pool_uuid",
	"poolgroup",
	"poolgroup_uuid",
	"port",
	"power_state",
	"primary",
	"se",
	"se_uuid",
	"secondarystat",
	"service",
	"source",
	"state",
	"subtitle",
	"target",
	"tenant",
	"title",
	"ttl",
	"type",
	"version",
	"vip_id",
	"vs",
	"vs_uuid",
	"vsvip",
	"vsvip_uuid",
}

func main() {
	exitProcess(run(os.Args[1:], os.Stdout, os.Stderr, serveHTTP))
}

func run(args []string, stdout, stderr io.Writer, serve func(*http.Server) error) int {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	var (
		url             string
		tenantsStr      string
		apiVersion      string
		labelsStr       string
		disabledModules string
		metricsStep     int
		metricsLimit    int
		bindPort        int
		parallelism     int
		showVersion     bool
		debug           bool
	)

	fs := flag.NewFlagSet(app, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&url, "url", "", "Avi controller URL (e.g., https://avi.example.com)")
	fs.StringVar(&tenantsStr, "tenants", "", "Comma-separated list of tenant names to scrape, or '*' for all tenants")
	fs.StringVar(&apiVersion, "api-version", "", "X-Avi-Version header (e.g., 30.2.1)")
	fs.StringVar(&labelsStr, "labels", "", "Custom labels in key=value format, comma-separated")
	fs.StringVar(&disabledModules, "disabled-modules", "", "Comma-separated list of modules to disable")
	fs.IntVar(&metricsStep, "metrics-step", 300, "Analytics metrics step in seconds")
	fs.IntVar(&metricsLimit, "metrics-limit", 1, "Analytics metrics sample count per query")
	fs.IntVar(&bindPort, "bind-port", 9290, "Port to bind the exporter endpoint to")
	fs.IntVar(&parallelism, "parallelism", 5, "Maximum concurrent API requests")
	fs.BoolVar(&showVersion, "version", false, "Display application version")
	fs.BoolVar(&debug, "debug", false, "Enable debug logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if showVersion {
		fmt.Fprintf(stdout, "%s v%s build %s\n", app, version, build)
		return 0
	}

	logLevel := slog.LevelInfo
	if debug {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})).With("app", app, "version", "v"+version, "build", build)

	if url == "" {
		url = config.GetURL()
	}
	if url == "" {
		logger.Error("URL is required (use -url flag or AVI_URL env var)")
		fs.Usage()
		return 1
	}

	if apiVersion == "" {
		apiVersion = config.GetAPIVersion()
	}
	if apiVersion == "" {
		// Sensible recent default; controller will accept lower if request fails with 419.
		apiVersion = "30.2.1"
	}

	envLabels := config.ParseLabels(config.GetLabels())
	cliLabels := config.ParseLabels(labelsStr)
	labels := envLabels
	for k, v := range cliLabels {
		labels[k] = v
	}
	if err := config.ValidateLabels(labels, reservedCustomLabelNames); err != nil {
		logger.Error("invalid label configuration", "err", err)
		return 1
	}

	envDisabled := config.ParseCSV(config.GetDisabledModules())
	cliDisabled := config.ParseCSV(disabledModules)
	disabled := append(envDisabled, cliDisabled...)

	envTenants := config.ParseCSV(config.GetTenants())
	cliTenants := config.ParseCSV(tenantsStr)
	tenants := append(envTenants, cliTenants...)
	if len(tenants) == 0 {
		tenants = []string{"admin"}
	}

	cfg := &config.Config{
		Labels:          labels,
		DisabledModules: disabled,
		Tenants:         tenants,
		APIVersion:      apiVersion,
		MetricsStep:     metricsStep,
		MetricsLimit:    metricsLimit,
	}

	username, password := config.GetCredentials()
	if username == "" || password == "" {
		logger.Error("credentials are required (set AVI_USERNAME and AVI_PASSWORD)")
		return 1
	}

	ignoreCert := config.GetIgnoreCert()
	if ignoreCert {
		logger.Info("TLS certificate verification disabled")
	}

	caFile := config.GetCAFile()
	if caFile != "" {
		logger.Info("using custom CA file", "path", caFile)
	}

	logger.Info("starting exporter",
		"url", url,
		"tenants", tenants,
		"api_version", apiVersion,
		"labels", len(labels),
		"disabled_modules", len(disabled),
	)

	exporter, err := collector.NewExporter(cfg, url, username, password, ignoreCert, caFile, parallelism, logger)
	if err != nil {
		logger.Error("failed to create exporter", "err", err)
		return 1
	}
	exporter.Start(context.Background())
	defer exporter.Stop()

	registry := prometheus.NewRegistry()
	registry.MustRegister(exporter)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})
	mux.HandleFunc("/readyz", exporter.ReadyHandler)
	mux.HandleFunc("/debug/cache", exporter.DebugCacheHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(app + "\n\nEndpoints:\n  /metrics\n  /healthz\n  /readyz\n  /debug/cache\n"))
	})

	listenAddr := ":" + strconv.Itoa(bindPort)
	logger.Info("starting server", "addr", listenAddr)

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if serve == nil {
		serve = serveHTTP
	}
	if err := serve(srv); err != nil {
		logger.Error("server error", "err", err)
		return 1
	}
	return 0
}
