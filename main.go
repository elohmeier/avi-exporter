package main

import (
	"flag"
	"fmt"
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
	app     = "Avi-Load-Balancer-Exporter"
	version string
	build   string
)

func main() {
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

	flag.StringVar(&url, "url", "", "Avi controller URL (e.g., https://avi.example.com)")
	flag.StringVar(&tenantsStr, "tenants", "", "Comma-separated list of tenant names to scrape, or '*' for all tenants")
	flag.StringVar(&apiVersion, "api-version", "", "X-Avi-Version header (e.g., 30.2.1)")
	flag.StringVar(&labelsStr, "labels", "", "Custom labels in key=value format, comma-separated")
	flag.StringVar(&disabledModules, "disabled-modules", "", "Comma-separated list of modules to disable")
	flag.IntVar(&metricsStep, "metrics-step", 300, "Analytics metrics step in seconds")
	flag.IntVar(&metricsLimit, "metrics-limit", 1, "Analytics metrics sample count per query")
	flag.IntVar(&bindPort, "bind-port", 9290, "Port to bind the exporter endpoint to")
	flag.IntVar(&parallelism, "parallelism", 5, "Maximum concurrent API requests")
	flag.BoolVar(&showVersion, "version", false, "Display application version")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.Parse()

	if showVersion {
		fmt.Printf("%s v%s build %s\n", app, version, build)
		os.Exit(0)
	}

	logLevel := slog.LevelInfo
	if debug {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})).With("app", app, "version", "v"+version, "build", build)

	if url == "" {
		url = config.GetURL()
	}
	if url == "" {
		logger.Error("URL is required (use -url flag or AVI_URL env var)")
		flag.Usage()
		os.Exit(1)
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
	if username == "" {
		logger.Error("credentials are required (set AVI_USERNAME and AVI_PASSWORD)")
		os.Exit(1)
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
		os.Exit(1)
	}

	prometheus.MustRegister(exporter)

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(app + " - /metrics for Prometheus metrics"))
	})

	listenAddr := ":" + strconv.Itoa(bindPort)
	logger.Info("starting server", "addr", listenAddr)

	srv := &http.Server{
		Addr:              listenAddr,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil {
		logger.Error("server error", "err", err)
		os.Exit(1)
	}
}
