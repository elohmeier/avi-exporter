# Avi Load Balancer Exporter

[![CI](https://github.com/elohmeier/avi-exporter/actions/workflows/ci.yml/badge.svg)](https://github.com/elohmeier/avi-exporter/actions/workflows/ci.yml)
[![Release](https://github.com/elohmeier/avi-exporter/actions/workflows/release.yml/badge.svg)](https://github.com/elohmeier/avi-exporter/actions/workflows/release.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/elohmeier/avi-exporter)](go.mod)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](#development)
[![GitHub release](https://img.shields.io/github/v/release/elohmeier/avi-exporter?sort=semver)](https://github.com/elohmeier/avi-exporter/releases)
[![GHCR](https://img.shields.io/badge/ghcr.io-elohmeier%2Favi--exporter-blue)](https://github.com/elohmeier/avi-exporter/pkgs/container/avi-exporter)

Prometheus exporter for VMware NSX Advanced Load Balancer (formerly Avi
Networks). Talks to the Avi Controller's REST API: inventory endpoints for
current state/health and the analytics `metrics/collection` endpoint for
time-series counters.

## Design at a glance

- One controller URL + one set of credentials, but **multi-tenant aware** —
  every per-tenant metric carries a `tenant` label.
- Auth is session+CSRF (matches the official `alb-sdk`): `POST /login` captures
  `sessionid`+`csrftoken` cookies, every subsequent request carries
  `X-CSRFToken`, `Referer`, `X-Avi-Version`, optional `X-Avi-Tenant`. CSRF
  rotates mid-session and the client picks up the new token from each response.
- A background scheduler refreshes inventory and analytics into an in-process
  cache. Prometheus scrapes read the cache only, so scrape fanout does not
  directly become Avi API load.
- Modules can be disabled if your controller can't serve them or you don't want
  them.
- **AKO awareness:** when an object was created by AKO (`created_by` prefix
  `ako-`), the `markers` field is parsed and `namespace`/`service`/`ingress`/
  `host` labels are populated automatically.
- **Topology metrics** (`avi_topology_node`, `avi_topology_edge`) emit a
  Grafana-node-graph-friendly view of
  `vsvip → virtualservice → (pool | poolgroup → pool) → poolmember`, with a
  `chain` label grouping all nodes belonging to the same AKO app.

## Quick start

### Environment variables

| Variable | Description | Required |
| --- | --- | --- |
| `AVI_URL` | Controller URL, e.g. `https://avi.example.com` | yes (or `-url`) |
| `AVI_USERNAME` | API username | yes |
| `AVI_PASSWORD` | API password (or API token, passed as password) | yes |
| `AVI_TENANTS` | Comma-separated tenant names, or `*` for all (default `admin`) | no |
| `AVI_API_VERSION` | `X-Avi-Version` header (default `30.2.1`) | no |
| `AVI_IGNORE_CERT` | Skip TLS verification (`true`/`1`) | no |
| `AVI_CA_FILE` | Path to a CA bundle for TLS verification | no |
| `AVI_LABELS` | Base labels (`k=v,k=v`), merged with `-labels` | no |
| `AVI_DISABLED_MODULES` | Base disabled modules, merged with `-disabled-modules` | no |

Tenant selection defaults to `admin`. When `*` is present, the exporter
discovers tenants from `/api/tenant`; any explicit tenant names are kept and
merged with the discovered set.

### CLI flags

| Flag | Description | Default |
| --- | --- | --- |
| `-url` | Controller URL (overrides `AVI_URL`) | |
| `-tenants` | Tenants to scrape, comma-separated; `*` for all | `admin` |
| `-api-version` | `X-Avi-Version` header value | `30.2.1` |
| `-labels` | Extra labels (`k=v,k=v`) | |
| `-disabled-modules` | Modules to skip (comma-separated) | |
| `-metrics-step` | Analytics step in seconds | `300` |
| `-metrics-limit` | Analytics samples per query | `1` |
| `-bind-port` | HTTP bind port | `9290` |
| `-parallelism` | Max concurrent per-tenant scrapers | `5` |
| `-debug` | Verbose logging | `false` |
| `-version` | Print version and exit | |

### Examples

Single tenant:

```bash
export AVI_URL=https://avi.example.com
export AVI_USERNAME=admin
export AVI_PASSWORD=secret
./avi-exporter
```

Scrape every tenant on the controller and add static labels:

```bash
./avi-exporter \
  -url https://avi.example.com \
  -tenants '*' \
  -labels "env=prod,site=eu-west" \
  -disabled-modules "se_metrics"
```

Docker:

```bash
docker run -p 9290:9290 \
  -e AVI_URL=https://avi.example.com \
  -e AVI_USERNAME=admin -e AVI_PASSWORD=secret \
  -e AVI_TENANTS='*' \
  ghcr.io/elohmeier/avi-exporter
```

## Release automation

Pushes to `main` run the release workflow after its test, vet, race, and
GoReleaser config checks pass. Conventional Commits drive the next version
(`fix` = patch, `feat` = minor, breaking changes = major). `semantic-release`
creates the `vX.Y.Z` tag and GitHub release notes.

When a release is created, GoReleaser attaches the Linux/macOS binaries and
publishes the multi-arch container image to `ghcr.io/elohmeier/avi-exporter`
with both the semver tag (`X.Y.Z`) and `latest`.

Renovate is configured to keep Go modules, GitHub Actions, and npm release
tooling current, grouped by ecosystem.

## Modules

| Module | Description |
| --- | --- |
| `cluster` | Controller cluster + per-node up/leader status |
| `controller_metrics` | Controller CPU, memory, disk, active virtual services, and backend server analytics |
| `vs_inventory` | Per-VS oper_status, enabled, health_score, percent_ses_up |
| `vs_metrics` | Per-VS L4/L7 client analytics (bandwidth, conns, 2xx/4xx/5xx, apdex, RTT) |
| `pool_inventory` | Per-pool oper_status, enabled, health_score, server counts, alert level, and app-profile type |
| `pool_metrics` | Per-pool L4/L7 server analytics (bandwidth, conns, RTT, latency, error responses, health status, uptime) |
| `pool_members` | Per-server oper_status. Costs one extra API call per pool (`/api/pool/<uuid>/runtime/server/detail/`). |
| `pool_group` | Pool group inventory + topology edges (vs → pool_group → pools). Catches SNI/HTTP-policy fan-out invisible to plain VS→pool edges. |
| `se_inventory` | Per-SE oper_status, enabled, health_score, plus se_connected/bgp_peers_up/gateway_up/license_state/power_state/migrate_state/version. |
| `se_metrics` | Per-SE CPU/mem/disk/conns/bytes, bandwidth, packet-buffer/persistence/SSL-session-cache usage. |
| `vsvip` | Per-VsVip oper_status, percent_ses_up, SE placement, floating-IP, auto-allocation, DNS records, plus a `vip_shared_by_vs_count` that exposes shared-listener / SNI patterns. |
| `gslb` | GSLB service oper_status, enabled, member count, FQDNs. Set `-tenants admin` if your GSLB lives there. |
| `topology` | Node + edge metrics for Grafana node-graph visualization (`vsvip → vs → (pool \| poolgroup → pool) → poolmember`). |

Disable with `-disabled-modules vs_metrics,se_metrics`.

### Exporter self-metrics (always emitted)

| Metric | Description |
| --- | --- |
| `avi_up{tenant}` | 1 if the last per-tenant background refresh had no module errors |
| `avi_module_last_success_timestamp_seconds{module,tenant}` | Unix timestamp of the last successful module refresh |
| `avi_module_last_attempt_timestamp_seconds{module,tenant}` | Unix timestamp of the last attempted module refresh |
| `avi_module_age_seconds{module,tenant}` | Age of the cached data currently served |
| `avi_module_stale{module,tenant}` | 1 when cached data is older than the module stale threshold |
| `avi_module_refresh_duration_seconds{module,tenant}` | Wall-clock duration of each module's last refresh |
| `avi_module_refresh_total{module,tenant}` | Cumulative refresh attempt count |
| `avi_module_refresh_errors_total{module,tenant}` | Cumulative refresh error count |
| `avi_scrape_duration_seconds{module,tenant}` | Compatibility alias for module refresh duration |
| `avi_scrape_total{module,tenant}` | Compatibility alias for module refresh attempts |
| `avi_scrape_errors_total{module,tenant}` | Compatibility alias for module refresh errors |

### Operational-status info metrics

Every entity that has an `*_oper_up` gauge (`vs`, `pool`, `pool_member`, `se`,
`vip`, `vs_vip`, `gslb_service`) also exposes a parallel `*_oper_status_info`
metric carrying the raw enum value as a `state` label
(`OPER_UP`, `OPER_DOWN`, `OPER_DISABLED`, `OPER_PARTITIONED`, `OPER_FAILED`,
`OPER_RESOURCES`, `OPER_INITIALIZING`, ...). Use these to distinguish
admin-disabled from genuinely broken.

## Endpoints

| Path | Description |
| --- | --- |
| `/metrics` | Cached Prometheus metrics; does not call Avi |
| `/healthz` | Liveness probe (always 200 OK) |
| `/health` | Backward-compatible liveness probe (always 200 OK) |
| `/readyz` | Readiness probe; returns 503 when required cached module data is missing or stale |
| `/debug/cache` | JSON cache/module freshness status |
| `/` | Plain-text endpoint index |

## Auth & versioning notes

- The Avi controller pins requests to an API schema via the `X-Avi-Version`
  header. Set `-api-version` to match (or be lower than) your controller's
  version.
- API token auth: the controller's `POST /login` accepts either
  `{"username","password"}` or `{"username","token"}`. The exporter currently
  only sends the password form (`AVI_PASSWORD`); if you want token auth, paste
  the generated token into `AVI_PASSWORD` and the controller will accept it
  for sessions that have token-as-password compatibility enabled, otherwise
  use a service account. Bearer-token (`Authorization: Bearer ...`) auth is
  not supported by the on-prem Controller.
- The exporter is read-only — it only `GET`s inventory and `POST`s the
  analytics collection endpoint.

## Recommended scrape interval

Avi aggregates analytics on a 5-minute boundary. The defaults (`step=300`,
`limit=1`) return the latest 5-min average. The exporter refreshes its cache in
the background and `/metrics` never calls Avi, but scraping faster than the cache
and aggregation cadence still gives you no fresher controller data.

## Development

Run the full test suite with coverage:

```bash
go test ./... -cover
```

The repository currently maintains 100% statement coverage across all packages.

## License

MIT
