# VMware NSX Advanced Load Balancer (Avi) REST API — Research Notes

Research notes for building a Go-based Prometheus exporter for VMware NSX Advanced
Load Balancer (formerly Avi Networks, now Broadcom VMware Avi Load Balancer).
Modelled in style after a Citrix NetScaler Nitro exporter.

This document contains **no Go code** — only the information needed to design the
HTTP client, choose endpoints, and pick metric_ids. All facts are linked back to
authoritative sources.

---

## Table of contents

1. [Product naming and doc landscape](#1-product-naming-and-doc-landscape)
2. [Authentication](#2-authentication)
3. [Standard headers & versioning](#3-standard-headers--versioning)
4. [Inventory / runtime endpoints](#4-inventory--runtime-endpoints)
5. [Analytics / metrics endpoints](#5-analytics--metrics-endpoints)
6. [Health score endpoint](#6-health-score-endpoint)
7. [Pagination & filtering](#7-pagination--filtering)
8. [Existing Go ecosystem & reference exporters](#8-existing-go-ecosystem--reference-exporters)
9. [Practical client design recommendations](#9-practical-client-design-recommendations)
10. [Source URLs](#10-source-urls)

---

## 1. Product naming and doc landscape

| Era | Product name | Doc home |
| --- | --- | --- |
| Pre-2019 | Avi Vantage / Avi Networks | `avinetworks.com/docs/<ver>` (legacy; redirects to Broadcom) |
| 2019–2023 | VMware NSX Advanced Load Balancer (NSX ALB) | docs.vmware.com (legacy) |
| 2024+ | VMware Avi Load Balancer (Broadcom) | `techdocs.broadcom.com/.../avi-load-balancer` |
| SDK | "alb-sdk" (Avi Load Balancer SDK) | `github.com/vmware/alb-sdk` |

- The legacy `avinetworks.com` doc site was decommissioned **2025-01-30**; current
  authoritative docs live at `techdocs.broadcom.com`.
- API developer portal: `developer.broadcom.com/xapis/vmware-avi-load-balancer/latest/`
  (Swagger/OpenAPI reference).
- Supported / documented major versions as of mid-2026: **31.2, 31.1, 30.2, 30.1,
  22.1, 21.1**. The current stable LTS line is **22.1.x** (long-running) and
  **31.x** is the latest mainline. The `X-Avi-Version` header should match the
  controller's actual deployed version.

The product still uses `avinetworks.com/docs/<old_version>` URLs in many search
hits — most of these still work but redirect; treat Broadcom TechDocs as the
canonical source for current behavior.

---

## 2. Authentication

The controller exposes the API at `https://<controller_ip>/api/`. There are
**three** authentication modes:

1. **Session authentication** (default; recommended) — cookie + CSRF token.
2. **Basic authentication** — disabled by default; can be enabled per controller.
3. **API token / API key** (SaaS, also usable on on-prem) — token is passed as the
   "password" via Basic-style auth.

### 2.1 Session authentication (recommended)

#### Login

```
POST https://<controller>/login
Content-Type: application/json

{"username": "admin", "password": "<password>"}
```

cURL:

```bash
curl -X POST -k https://<controller>/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"<password>"}' \
  -c cookies.txt
```

#### Response

- HTTP 200 on success.
- Sets cookies in the response:
  - `sessionid` — opaque session ID, sent on subsequent calls.
  - `csrftoken` — required to mirror into `X-CSRFToken` header on write requests.
  - `avi-sessionid` — on newer versions (≥ 22.1.x) you may also see
    `avi-sessionid` alongside `sessionid`; both are set, and clients should send
    whichever the controller assigned. Practical advice: store the entire
    `Set-Cookie` jar and replay it.

The login endpoint also accepts form-urlencoded body
(`username=admin&password=...`) — both are supported.

#### Subsequent requests

For **GET** requests you need only the session cookie:

```bash
curl -X GET -k https://<controller>/api/cluster/version -b cookies.txt
```

For **POST / PUT / PATCH / DELETE**, the controller enforces CSRF protection.
You MUST send:

- `Cookie: sessionid=<value>; csrftoken=<value>` (use the jar)
- `X-CSRFToken: <csrftoken cookie value>`
- `Referer: https://<controller>/` (must point to the controller; the controller
  validates Referer in addition to CSRF token)
- `Content-Type: application/json`

Note: even though `POST /api/analytics/metrics/collection` is a "read" in
practice, it is still a POST and **requires the CSRF / Referer headers**.

#### CSRF token rotation

The controller may rotate the CSRF token mid-session: after any response, if a
`Set-Cookie: csrftoken=...` is present, update the stored value and use the new
token for the next write. Quoting the official docs: *"Avi Controller may send
back CSRF token in the response cookies. The caller should update the request
headers with this token else controller will reject requests."*

#### Logout

```bash
curl -X POST -k https://<controller>/logout \
  -H "X-CSRFToken: <csrftoken>" \
  -H "Referer: https://<controller>/" \
  -b cookies.txt
```

Logging out is not strictly required (sessions time out), but doing so is
polite and frees server-side session state.

### 2.2 Basic authentication

Disabled by default; can be enabled via system settings. When enabled:

```bash
curl -u admin:<password> -k https://<controller>/api/virtualservice
```

The Authorization header is sent on every request. No cookie, no CSRF. Quoting
the docs: *"Basic Authentication is disabled by default and Session
Authentication should be leveraged in most use cases."*

For an exporter this is appealingly simple but **expect operators to leave
Basic Auth disabled**, so the client must support session auth as the primary
mode.

### 2.3 API token / API key

Supported on both SaaS and on-prem 22.1+. A user generates a token via the UI
(Administration → Users → ⋮ → Generate Token), specifies a TTL in hours (0 =
single-use; max 87,600 hours ≈ 10 years), and copies it.

Token is then used in place of the password — the Python SDK calls `ApiSession`
with `password=<token>` and the SDK passes it via Basic auth. From the docs:

```python
from avi.sdk.avi_api import ApiSession
api = ApiSession.get_session("controller.example", "[email protected]",
    "499e4c833c183312f3eeab2c9f5e8bd47c48d440",  # token-as-password
    tenant="kb")
```

For the exporter, treat tokens as a config-level alternative to a password and
feed them through the same Basic-Auth or login-and-cookie flow. There is **no
documented `Authorization: Bearer …` mode** — tokens go through the same
mechanisms as passwords.

### 2.4 Auth flow for an exporter — practical advice

The session-auth flow is what every existing Avi tool (CLI, vRO, William Lam's
PowerShell, the alb-sdk Python and Go SDKs) uses. Recommended design:

- One long-lived session per controller, refreshed on `401 Unauthorized` or
  `419 CSRF Failure`.
- Use a `cookiejar.Jar` and capture `csrftoken` on every response (not just
  login), because the controller rotates it.
- For our exporter all reads are GETs **except** `/api/analytics/metrics/collection`
  which is a POST. The exporter therefore unavoidably needs CSRF handling.

---

## 3. Standard headers & versioning

### 3.1 `X-Avi-Version` (recommended; effectively required)

Specifies which schema the client expects. The controller will:

- Respond using the field set / defaults of that version.
- Reject POST/PUT bodies that aren't compatible with that version.

Quoting the API reference: *"The caller is required to set Avi Version Header to
the expected version of configuration. The response from the controller will
provide and accept data according to the specified version."*

Practical choices for an exporter:

- **Pin a baseline** like `22.1.5` for broad compatibility — this version is
  supported on all currently-documented Avi releases (21.1, 22.1, 30.x, 31.x).
- **Make it configurable** so the operator can set it to match their cluster.
- Optionally **auto-detect** by hitting `GET /api/cluster/version` (no version
  header required for this read) and using the `Version` field.

There is also an `api_version=...` query parameter as an alternative, but the
header is the conventional form.

### 3.2 `X-Avi-Tenant`

By default API calls run as the user's "default tenant" (typically `admin`).
To query under a different tenant, set:

```
X-Avi-Tenant: <tenant_name>
```

Alternatively `X-Avi-Tenant-UUID: <uuid>` for UUID-based addressing.

For an exporter scanning multi-tenant clusters, the typical pattern is:

- Either run as a user that has the special "All Tenants" scope (login with
  `X-Avi-Tenant: *` — controller-level access), or
- Iterate tenants from `/api/tenant` and re-issue inventory calls per tenant
  with `X-Avi-Tenant: <name>`.

### 3.3 Other useful headers

- `X-Avi-Referer` — alternative to standard `Referer` for some odd reverse-proxy
  setups. Rarely needed.
- `Content-Type: application/json` on every write.
- `Accept: application/json` (default for the API but be explicit).
- `X-Avi-Internal-Login`: internal only — do not use.

---

## 4. Inventory / runtime endpoints

The "inventory" endpoints (`<resource>-inventory`) are the workhorse for a
state-snapshot exporter. They are explicitly designed to bundle config +
runtime + alert + health-score + faults into a single response so you don't
have to do N+1 fetches.

Generic shape (collection):

```
GET /api/<resource>-inventory?page=<n>&page_size=<m>&include_name=true
    &include=runtime,health_score,alert,faults
    &join_subresources=runtime
    &fields=<csv>
    &skip_default=true
```

Generic shape (single):

```
GET /api/<resource>-inventory/<uuid>?include_name=true&include=runtime,health_score,alert,faults&step=300&limit=72
```

Response envelope (collection):

```json
{
  "count": 138,
  "next": "/api/virtualservice-inventory?page=2&page_size=25",
  "results": [ {...inventory item...}, ... ]
}
```

`next` is **absent on the last page** — that is the canonical loop termination.

### 4.1 `/api/virtualservice-inventory`

The most important endpoint. Returns an array of `VsInventory` objects each
containing:

| Field | Type | Notes |
| --- | --- | --- |
| `config` | object | The VirtualService config: `uuid`, `name`, `cloud_ref`, `pool_ref`, `vsvip_ref`, `services` (`port`, `enable_ssl`), `vip` (list of `ip_address`), `enabled`, `traffic_enabled`, `tenant_ref`, `se_group_ref`, etc. |
| `runtime` | object | VIP summary, SE assignments per VIP, operational counters. |
| `oper_status` | object | `{state: OPER_UP|OPER_DOWN|OPER_DISABLED|OPER_INITIALIZING|OPER_ERROR_DISABLED|..., last_changed_time: {secs,usecs}, reason: [...], reason_code: <int>}` |
| `health_score` | object | `{health_score: 0–100, anomaly_penalty: float, performance_score: float, security_penalty: float, resources_penalty: float}` plus APDEX. |
| `alert` | object | Alert level summary (e.g. counts by `LOW`/`MEDIUM`/`HIGH`). |
| `faults` | object | **Excluded by default** — add `include=faults` query param to request. |

`oper_status.state` is the gold metric for `avi_vs_up` — map `OPER_UP` to 1 and
everything else to 0 (or expose a stateset).

Example URL pulled from operator blogs:

```
https://avi-controller.home.lab/api/virtualservice-inventory/virtualservice-5b091f06-7f6a-4d0f-85fc-b108e9ab6c2a/?include_name=true&include=health_score,runtime,alert,faults&step=300&limit=72
```

Note the `step` and `limit` on the inventory call — when those are present the
controller embeds aggregated metric series alongside the inventory entry. For
an exporter doing explicit metric collection separately, you can omit them.

Known issue: in 22.1.6, the inventory endpoint omits blank string fields
instead of returning them as `""` (`AV-184622` in release notes). Code should
not assume every documented field is present in the response.

### 4.2 `/api/pool-inventory`

Same envelope. Per-pool item includes `config` (with `default_server_port`,
`health_monitor_refs`, `lb_algorithm`), `runtime`, `health_score`, and
crucially the **per-server status**.

The per-server runtime lives under `runtime.server_states[]` or
`runtime.oper_status` depending on Avi version, with each member exposing:

- `ip_addr.addr` / `port`
- `oper_status.state` (`OPER_UP` / `OPER_DOWN` / `OPER_DISABLED` / `OPER_OUT_OF_SERVICE`)
- `oper_status.reason` (e.g. health monitor name that failed)
- `oper_status.last_changed_time`

This maps cleanly to Prometheus metrics like
`avi_pool_member_up{pool,server_ip,server_port}`.

If `pool-inventory` is not present on a given Avi release (older 17.x), the
fallback is `GET /api/pool?join_subresources=runtime` — explicitly joining
the runtime sub-resource gets you the same per-member data.

### 4.3 `/api/serviceengine-inventory`

Per-SE item includes `config` (`name`, `se_group_ref`, `cloud_ref`,
`mgmt_vnic`), `runtime`, and `health_score`. Useful fields on `runtime`:

- `oper_status.state` (`OPER_UP`, `OPER_DOWN`, `OPER_REBOOTING`,
  `OPER_PARTITIONED`, ...)
- `se_connected` (bool)
- `power_state`
- `enable_state`
- `gateway_up`
- Aggregated CPU/mem are exposed via the metrics API rather than inventory.

The CLI command `show serviceengine` maps to
`GET /api/serviceengine?owned_by_controller=True&join_subresources=runtime`,
which is a useful alternative for older controllers.

### 4.4 `/api/cluster` and `/api/cluster/runtime`

Used for **controller cluster** health (the controllers themselves, not the
SEs).

```http
GET /api/cluster
```

```json
{
  "url": "https://10.10.5.27/api/cluster",
  "uuid": "cluster-005056ac9e91",
  "name": "cluster-0-1",
  "tenant_ref": "https://10.10.5.27/api/tenant/admin",
  "virtual_ip": {"type": "V4", "addr": "10.10.5.27"},
  "nodes": [
    {"ip": {"type":"V4","addr":"10.10.5.16"},
     "vm_hostname":"node1.controller.local",
     "vm_uuid":"005056ac9e91",
     "name":"10.10.5.16"}
  ]
}
```

```http
GET /api/cluster/runtime
```

```json
{
  "node_states": [
    {"up_since": "2016-03-26 16:06:21",
     "state": "CLUSTER_ACTIVE",
     "role": "CLUSTER_LEADER",
     "name": "10.10.5.15"}
  ],
  "cluster_state": {
    "up_since": "2016-03-26 16:06:21",
    "progress": 100,
    "state": "CLUSTER_UP_HA_ACTIVE"
  }
}
```

Cluster-state enum values worth knowing:

- `CLUSTER_UP_HA_ACTIVE` — healthy 3-node
- `CLUSTER_UP_NO_HA` — healthy 1-node
- `CLUSTER_UP_HA_NOT_READY` — HA degraded
- `CLUSTER_UP_HA_COMPROMISED` — running but one node lost

Per-node `state` values:

- `CLUSTER_ACTIVE`, `CLUSTER_STANDBY`, `CLUSTER_UP`, `CLUSTER_DOWN`,
  `CLUSTER_NODE_INIT`, `CLUSTER_NODE_DB_FAILED`, `CLUSTER_NODE_DB_PROCESSING`,
  ...

Per-node `role`: `CLUSTER_LEADER`, `CLUSTER_FOLLOWER`.

### 4.5 `/api/cluster/version` (controller version / license)

```http
GET /api/cluster/version
```

Returns the controller version string (the value to pin into `X-Avi-Version`
auto-detection):

```json
{
  "Tag": "Wed Mar 25 22:47:38 2025",
  "Version": "22.1.5",
  "build": "9012",
  "min_supported_api_version": "17.1.1"
}
```

For license, use `GET /api/license` and `GET /api/licenseusage?limit=365&step=86400`
(the latter is what mslocrian's exporter calls). The license usage endpoint
also exposes seat counts and expirations.

### 4.6 Helpful supporting reads

For label enrichment:

- `GET /api/tenant?page_size=200` — tenant id → name
- `GET /api/cloud?page_size=200` — cloud id → name
- `GET /api/serviceenginegroup?page_size=200` — SE group id → name

These are tiny and can be cached at exporter startup, then refreshed
periodically (every few minutes).

---

## 5. Analytics / metrics endpoints

The metrics API is the time-series side. Two shapes:

- **Single-entity GET** — easy, one query at a time.
- **Bulk POST** — `POST /api/analytics/metrics/collection` with an array of
  per-entity per-metric requests. **This is what every real exporter uses**
  because it amortizes the round-trip cost.

### 5.1 GET form

```
GET /api/analytics/metrics/{object_type}/{object_uuid}?
    metric_id=<csv>&step=<seconds>&limit=<n>&start=<ISO8601>&stop=<ISO8601>
    &dimensions=<csv>&dimension_limit=<n>&obj_id=<value>&pad_missing_data=false
```

`object_type` ∈ `virtualservice` | `pool` | `serviceengine` | `controller`.

Example — last 6h of bandwidth and complete responses at 5-min resolution:

```
GET /api/analytics/metrics/virtualservice/virtualservice-abc/
    ?metric_id=l4_client.avg_bandwidth,l7_client.avg_complete_responses
    &limit=72&step=300
```

Standard windows:

| Window | step | limit |
| --- | --- | --- |
| Last 30m (5s realtime) | 5 | 360 |
| Last 30m (5m agg) | 300 | 6 |
| Last 6h | 300 | 72 |
| Last 24h | 300 | 288 |
| Latest single sample | 300 | 1 |

Supported `step` values: **5** (realtime, requires `realtime=true` or recent
versions; subset of metrics only), **300**, **3600**, **86400**.

### 5.2 POST `/api/analytics/metrics/collection` (preferred)

```
POST /api/analytics/metrics/collection
Content-Type: application/json
X-CSRFToken: <token>
Referer: https://<controller>/
X-Avi-Version: 22.1.5
```

Request body — confirmed from the `mslocrian/avi_exporter` Go code that talks
to a real controller:

```json
{
  "metric_requests": [
    {
      "step": 5,
      "limit": 1,
      "entity_uuid": "*",
      "metric_entity": "VSERVER_METRICS_ENTITY",
      "metric_id": "l4_client.avg_bandwidth"
    },
    {
      "step": 5,
      "limit": 1,
      "entity_uuid": "*",
      "metric_entity": "SE_METRICS_ENTITY",
      "metric_id": "se_stats.avg_cpu_usage"
    }
  ]
}
```

Key points:

- `entity_uuid: "*"` returns the metric for **all** entities of that type — this
  is how the exporter fans out across all VSes / SEs / controllers without an
  enumeration pass.
- `metric_entity` enum (the most common values):
  - `VSERVER_METRICS_ENTITY` (Virtual Service)
  - `SE_METRICS_ENTITY` (Service Engine)
  - `VM_METRICS_ENTITY` (Server / Pool member)
  - `CONTROLLER_METRICS_ENTITY` (Controller node)
- `metric_id` is a single ID per request entry (use multiple entries to batch).
  Note: in the GET form `metric_id` is comma-separated, but in the POST
  collection form it's one ID per item.
- `step` in **seconds**. For an exporter scraping every 60–300s, use
  `step=300, limit=1` to get the most recent 5-min average; using `step=5,
  limit=1` returns realtime per-sample data but is heavier on the controller.
- `obj_id` is an optional sub-entity filter:
  - For pool member metrics: `obj_id=<server_ip>:<port>`.
  - For URL/path metrics: the URL string.
  - `obj_id=*` returns all sub-entities aggregated/dimensioned.
- `pool_uuid` and `serviceengine_uuid` are optional cross-filters when querying
  VS metrics, e.g. *"VS metrics restricted to pool X"*.
- `dimensions=os,browser,country` groups results by those dimensions.
  `dimension_limit=10` (default) caps cardinality.

#### Response shape

The response is keyed by `series` and is an **array per request entry**:

```json
{
  "series": {
    "virtualservice-aaaa-...": [
      {
        "header": {
          "name": "l4_client.avg_bandwidth",
          "entity_uuid": "virtualservice-aaaa-...",
          "tenant_uuid": "admin",
          "units": "BITS_PER_SECOND",
          "obj_id_type": "METRICS_OBJ_ID_TYPE_VIRTUALSERVICE",
          "priority": false,
          "metrics_min_scale": 0.0,
          "metric_description": "Average Bandwidth",
          "statistics": {
            "min": 0.0, "max": 1234567.0, "mean": 543210.0,
            "min_ts": "2026-06-11T11:00:00Z",
            "max_ts": "2026-06-11T11:55:00Z",
            "num_samples": 12, "trend": 0.0
          },
          "derivation_data": null
        },
        "data": [
          { "timestamp": "2026-06-11T11:55:00Z", "value": 543210.0 }
        ]
      }
    ],
    "virtualservice-bbbb-...": [ ... ]
  }
}
```

For Prometheus you typically take the **last** value from `data[]` (or
`statistics.mean` if you want the window average) and emit it as a Gauge with
labels.

### 5.3 Metric ID catalog

Found / used by the existing exporters (a working subset of the ~800 metrics
the controller can expose). Use `GET /api/analytics/metric_id` to enumerate
everything supported on a specific controller.

#### VirtualService — `VSERVER_METRICS_ENTITY`

**L4 client (frontend):**

```
l4_client.apdexc                        APDEX for connection latency
l4_client.apdexrtt                      APDEX for RTT
l4_client.avg_bandwidth                 bits/sec total
l4_client.avg_rx_bytes
l4_client.avg_tx_bytes
l4_client.avg_complete_conns            completed conns / sec
l4_client.avg_new_established_conns
l4_client.avg_connections_dropped
l4_client.avg_errored_connections
l4_client.avg_lossy_connections
l4_client.avg_l4_client_latency
l4_client.avg_total_rtt
l4_client.avg_policy_drops
l4_client.avg_dos_attacks
l4_client.avg_application_dos_attacks
l4_client.avg_network_dos_attacks
l4_client.avg_dos_bandwidth
l4_client.max_open_conns
l4_client.pct_connection_errors
l4_client.pct_policy_drops
l4_client.pct_dos_bandwidth
l4_client.pct_dos_rx_bytes
l4_client.pct_pkts_dos_attacks
l4_client.pct_application_dos_attacks
l4_client.pct_connections_dos_attacks
l4_client.sum_connection_errors
l4_client.sum_finished_conns
l4_client.sum_lossy_connections
l4_client.sum_lossy_req
```

**L7 client (frontend HTTP):**

```
l7_client.apdexr                        APDEX for response time
l7_client.avg_total_requests
l7_client.avg_complete_responses
l7_client.avg_error_responses
l7_client.avg_satisfactory_responses
l7_client.avg_tolerated_responses
l7_client.avg_frustrated_responses
l7_client.avg_resp_1xx
l7_client.avg_resp_2xx
l7_client.avg_resp_3xx
l7_client.avg_resp_4xx
l7_client.avg_resp_4xx_avi_errors
l7_client.avg_resp_5xx
l7_client.avg_resp_5xx_avi_errors
l7_client.avg_application_response_time
l7_client.avg_page_load_time
l7_client.avg_page_download_time
l7_client.avg_dns_lookup_time
l7_client.avg_connection_time
l7_client.avg_blocking_time
l7_client.avg_browser_rendering_time
l7_client.avg_redirection_time
l7_client.avg_dom_content_load_time
l7_client.avg_client_data_transfer_time
l7_client.avg_client_rtt
l7_client.avg_client_txn_latency
l7_client.avg_server_rtt
l7_client.avg_service_time
l7_client.avg_waiting_time
l7_client.avg_ssl_connections
l7_client.avg_ssl_errors
l7_client.avg_ssl_failed_connections
l7_client.avg_ssl_handshakes_new
l7_client.avg_ssl_handshakes_reused
l7_client.avg_ssl_handshakes_pfs
l7_client.avg_ssl_handshakes_non_pfs
l7_client.max_ssl_open_sessions
l7_client.pct_cache_hits
l7_client.pct_cacheable_hits
l7_client.pct_response_errors
l7_client.pct_ssl_failed_connections
l7_client.rum_apdexr
l7_client.sum_errors
l7_client.sum_get_reqs
l7_client.sum_post_reqs
l7_client.sum_other_reqs
l7_client.sum_total_responses
l7_client.sum_num_rum_samples
```

#### Pool — also `VSERVER_METRICS_ENTITY` (backend side of a VS)

**L4 server:**

```
l4_server.apdexc
l4_server.avg_bandwidth
l4_server.avg_complete_conns
l4_server.avg_new_established_conns
l4_server.avg_connections_dropped
l4_server.avg_errored_connections
l4_server.avg_lossy_connections
l4_server.avg_open_conns
l4_server.avg_total_rtt
l4_server.avg_health_status
l4_server.avg_uptime
l4_server.avg_goodput
l4_server.avg_available_capacity
l4_server.avg_est_capacity
l4_server.avg_pool_bandwidth
l4_server.avg_pool_complete_conns
l4_server.avg_pool_errored_connections
l4_server.avg_pool_new_established_conns
l4_server.max_open_conns
l4_server.pct_connection_errors
l4_server.pct_connection_saturation
l4_server.sum_connection_errors
l4_server.sum_connections_dropped
l4_server.sum_finished_conns
l4_server.sum_health_check_failures
l4_server.sum_lossy_connections
l4_server.sum_lossy_req
```

**L7 server:**

```
l7_server.apdexr
l7_server.avg_application_response_time
l7_server.avg_resp_latency
l7_server.avg_complete_responses
l7_server.avg_error_responses
l7_server.avg_satisfactory_responses
l7_server.avg_tolerated_responses
l7_server.avg_frustrated_responses
l7_server.avg_total_requests
l7_server.avg_resp_1xx
l7_server.avg_resp_2xx
l7_server.avg_resp_3xx
l7_server.avg_resp_4xx
l7_server.avg_resp_4xx_errors
l7_server.avg_resp_5xx
l7_server.avg_resp_5xx_errors
l7_server.pct_response_errors
l7_server.sum_get_reqs
l7_server.sum_post_reqs
l7_server.sum_other_reqs
l7_server.sum_total_responses
```

Per-pool-member queries: set `metric_entity=VM_METRICS_ENTITY`, set
`entity_uuid` to the VS UUID, `pool_uuid` to the pool UUID, and use
`obj_id=<server_ip>:<port>` to scope to one member (or `obj_id=*` to fan out).

#### Service Engine — `SE_METRICS_ENTITY`

```
se_stats.avg_cpu_usage
se_stats.avg_mem_usage
se_stats.avg_disk1_usage
se_stats.avg_connections
se_stats.avg_connections_dropped
se_stats.pct_connections_dropped
se_stats.avg_connection_mem_usage
se_stats.avg_packet_buffer_usage
se_stats.avg_packet_buffer_header_usage
se_stats.avg_packet_buffer_large_usage
se_stats.avg_packet_buffer_small_usage
se_stats.avg_persistent_table_usage
se_stats.avg_ssl_session_cache_usage
se_stats.pct_syn_cache_usage

se_if.avg_bandwidth
se_if.avg_rx_bytes
se_if.avg_tx_bytes
se_if.avg_rx_pkts
se_if.avg_tx_pkts
```

`se_if.*` is per-interface and benefits from `dimensions=if_name`.

#### Controller — `CONTROLLER_METRICS_ENTITY`

```
controller_stats.avg_cpu_usage
controller_stats.avg_mem_usage
controller_stats.avg_disk_usage
controller_stats.sum_total_vs_client_bytes
```

The set is small because most controller observability is in
`/api/cluster/runtime` rather than the metrics table.

### 5.4 Metric tables (METRICS_TABLE_*) — diagnostic

Internally each metric belongs to a "table":

```
METRICS_TABLE_VSERVER_L4_CLIENT
METRICS_TABLE_VSERVER_L4_SERVER
METRICS_TABLE_VSERVER_L7_CLIENT
METRICS_TABLE_VSERVER_L7_SERVER
METRICS_TABLE_SE_STATS
METRICS_TABLE_CONTROLLER_STATS
METRICS_TABLE_VM_STATS
METRICS_TABLE_HEALTH_SCORE
METRICS_TABLE_ANOMALY
METRICS_TABLE_DOS_ANALYTICS
METRICS_TABLE_APP_INSIGHTS
METRICS_TABLE_RUM_ANALYTICS
METRICS_TABLE_VSERVER_DNS
METRICS_TABLE_SERVER_DNS
METRICS_TABLE_PROCESS_STATS
METRICS_TABLE_USER_METRICS
METRICS_TABLE_WAF_GROUP
METRICS_TABLE_WAF_RULE
METRICS_TABLE_NONE
```

You normally don't pass the table; you pass `metric_id` and the controller
infers the table. Knowing the tables is mainly useful when reading the API
reference.

### 5.5 Performance / pacing

Quoting the Broadcom Prometheus integration docs: *"Avi Load Balancer
Controller might take some time to compute the analytics data and return the
response. Setting a low scrape_interval value might result in API timeouts. In
such cases, use filter params to get relevant data and set the scrape_interval
value to a minimum of 300 seconds in the Prometheus config yaml."*

Practical limits:

- **One big POST collection** with all metric_ids and `entity_uuid: "*"` is
  cheaper than N small GETs.
- **Cache aggressively**. mkarnowski's exporter defaults to a 300s cache so
  Prometheus can scrape every 60s without hammering the controller.
- The lowest sane scrape interval against a real Avi cluster is **60s with a
  300s cache** or **300s without caching**.
- Expect occasional `429`s under load — back off.

---

## 6. Health score endpoint

Six endpoints, all GET:

```
GET /api/analytics/healthscore/virtualservice
GET /api/analytics/healthscore/virtualservice/{uuid}
GET /api/analytics/healthscore/pool
GET /api/analytics/healthscore/pool/{uuid}
GET /api/analytics/healthscore/serviceengine
GET /api/analytics/healthscore/serviceengine/{uuid}
```

The response is roughly the same as the `health_score` field embedded in the
corresponding `-inventory` response, so unless you need historical health-score
queries (with `step` / `limit` / `start`), you don't need this endpoint at all
— the inventory call already includes it.

Health score object fields (typical):

```json
{
  "health_score": 96.0,
  "anomaly_penalty": 0.0,
  "performance_score": 100.0,
  "security_penalty": 0.0,
  "resources_penalty": 4.0,
  "reason": "",
  "reason_attr": ""
}
```

`health_score` is 0–100. The penalty fields sum (roughly) to `100 - health_score`.

---

## 7. Pagination & filtering

### 7.1 Pagination

Query parameters supported on every collection endpoint:

| Param | Default | Range / Notes |
| --- | --- | --- |
| `page` | 1 | 1-indexed page number |
| `page_size` | **25** (docs); some endpoints behave like 200 | **Maximum is 200** — set this on every call |

Real exporters always use `page_size=200` and walk pages.

Response envelope:

```json
{ "count": 412,
  "next": "/api/virtualservice-inventory?page=2&page_size=200",
  "results": [ ... ]
}
```

**Termination:** the `next` field is **absent on the final page**. Do not rely
on `count` vs `len(results)` because filters and tenant scoping can perturb
counts. The reliable loop is *"while response contains `next`, GET page+1"*.

### 7.2 Filtering

| Param | Effect | Example |
| --- | --- | --- |
| `fields` | CSV list of fields to return (always returns `uuid`, `url`, `name`) | `fields=name,uuid,enabled` |
| `include_name` | Expand UUID refs to include human name | `include_name=true` → `pool_ref` becomes `https://.../api/pool/pool-uuid#mypool` |
| `skip_default` | Strip fields equal to their default value | `skip_default=true` |
| `refers_to` | Find objects that reference a given object | `refers_to=pool:<pool_uuid>` |
| `referred_by` | Find objects referenced by a given object | `referred_by=virtualservice:<vs_uuid>` |
| `join_subresources` | Inline a sub-resource (notably `runtime`) | `join_subresources=runtime` |
| `include` | Inline supplemental data on `-inventory` endpoints | `include=runtime,health_score,alert,faults` |
| `name` | Exact name match | `name=my-vs` |
| `name.contains` | Case-sensitive substring | `name.contains=prod` |
| `name.icontains` | Case-insensitive substring | `name.icontains=PROD` |
| `name.in` | CSV exact match | `name.in=vs1,vs2,vs3` |
| `search` / `isearch` | Substring across any string field | `search=prod` |
| `cloud_uuid` | Filter by cloud UUID | `cloud_uuid=cloud-xyz` |
| `cloud_ref.uuid` / `cloud_ref.name` | Same, via ref | `cloud_ref.name=Default-Cloud` |
| `tenant_ref.uuid` | Filter by tenant UUID | (combine with `X-Avi-Tenant: *`) |
| `sort` | Sort by field | `sort=_last_modified` or `sort=-name` |
| `label_key` / `label_value` | Filter by user labels | `label_key=env&label_value=prod` |
| `owned_by_controller` | SE filter (for SEs this controller manages) | `owned_by_controller=true` |
| `limit_by` | Total result cap without pagination | `limit_by=10` (no `next` field) |

For the exporter the most useful set is:
`page_size=200&include_name=true&fields=...&skip_default=true&join_subresources=runtime`.

`fields` + `skip_default` together can shrink responses by 50–80%, which
matters at scale.

---

## 8. Existing Go ecosystem & reference exporters

### 8.1 Official Go SDK — `github.com/vmware/alb-sdk`

- Repo: https://github.com/vmware/alb-sdk (Go source on the `eng` branch under `/go`).
- Module path: `github.com/vmware/alb-sdk/go/{clients,session,models}`.
- Go reference docs:
  - https://pkg.go.dev/github.com/vmware/alb-sdk/go/session
  - https://pkg.go.dev/github.com/vmware/alb-sdk/go/models
- License: Apache 2.0.
- Auto-released to match controller versions (e.g. `31.1.1`, `31.2.1`).

Structure:

- `session` — `AviSession` type with `NewAviSession(host, user, opts...)`,
  `Get`, `Post`, `Put`, `Delete`, `GetCollection`, `GetCollectionRaw`,
  `RestRequest`, plus options like `SetPassword`, `SetTenant`, `SetInsecure`,
  `SetAuthToken`, `SetCSPHost`, `SetCSPToken`, `SetLazyAuthentication`,
  `SetVersion`, `SetTimeout`, `SetControllerStatusCheckLimits`. Handles
  cookie + CSRF rotation automatically. Pools sessions per host.
- `clients` — typed clients per resource (`VirtualServiceClient`,
  `PoolClient`, `ServiceEngineClient`, `TenantClient`, etc.) wrapping the
  session.
- `models` — generated Go structs from the Avi swagger. **Massive** — the
  `models` package is hundreds of files and dwarfs everything else.

Pros / cons for the exporter:

- **Pro:** automatic auth/CSRF handling, version-tracked, official.
- **Con:** the `models` package is hundreds of files of generated code and
  brings in many unused types — significant binary bloat (the existing
  `mslocrian/avi_exporter` is ~150 MB compiled). For a tightly-focused
  exporter, importing a small subset of session helpers and writing your own
  inventory structs is more idiomatic.
- **Con:** the SDK is on the `eng` branch, not `main`. `go get` works but
  pin a tag.
- **Con:** the SDK's `Get` returns `interface{}` / `map[string]interface{}`
  by default — to get typed responses you must use the typed clients, which
  pulls in lots of models.

Recommended hybrid: write the HTTP/auth core ourselves (it's ~200 lines), use
narrowly-defined response structs for just the fields we read, and skip the
SDK entirely. This matches what `mkarnowski/avi_prometheus_exporter` does. If
you'd rather lean on the SDK, just use `session.AviSession` and ignore
`clients`/`models`.

### 8.2 Community exporters

| Repo | Lang | Notes |
| --- | --- | --- |
| [`mkarnowski/avi_prometheus_exporter`](https://github.com/mkarnowski/avi_prometheus_exporter) | Python (container) | The "official-ish" Avi container exporter. Single `/metrics` endpoint, multi-tenant, 5-min averages, 300s default cache, ~800 metrics. Configurable entity types: `virtualservice`, `serviceengine`, `pool`, `controller`. **Best behavioral reference for cache + query shape.** |
| [`ticketmaster/TMNET-avi_exporter`](https://github.com/ticketmaster/TMNET-avi_exporter) | Go | Drives the alb-sdk. JSON metric-list files under `lib/` define which metric_ids to scrape. Env-driven config (`AVI_USERNAME`, `AVI_PASSWORD`, `AVI_CLUSTER`, `AVI_TENANT`, `AVI_APIVERSION`, `AVI_METRICS`). Intended for Kubernetes. |
| [`mslocrian/avi_exporter`](https://github.com/mslocrian/avi_exporter) | Go | Fork of the Ticketmaster one with additional collectors: BGP per SE, `memdist`, `shmallocstats`, `licenseusage`, license. Real-world reference of the **POST `/api/analytics/metrics/collection`** request shape and the `MetricRequest` struct used. Uses `github.com/avinetworks/sdk` (the predecessor of `vmware/alb-sdk`). |
| [`avinetworks/devops/monitoring_tools/grafana/prometheus`](https://github.com/avinetworks/devops/tree/master/monitoring_tools/grafana/prometheus) | Mixed | Reference Grafana dashboards + the official Python exporter mirror. Good source of dashboard PromQL patterns. |

Endpoints actually called by `mslocrian/avi_exporter`:

```
GET  /api/cluster
GET  /api/serviceengine-inventory?page_size=200
GET  /api/tenant?page_size=200
GET  /api/cloud?page_size=200
GET  /api/serviceenginegroup?page_size=200
GET  /api/virtualservice-inventory?page_size=200&page=<n>
GET  /api/serviceengine/<uuid>/memdist
GET  /api/serviceengine/<uuid>/shmallocstats
GET  /api/serviceengine/<uuid>/bgp
GET  /api/licenseusage?limit=365&step=86400
GET  /api/license
POST /api/analytics/metrics/collection
```

This is a solid starting endpoint set for our exporter.

---

## 9. Practical client design recommendations

Synthesized from the above.

1. **Auth layer**
   - Implement session login (POST `/login`) as the primary mode.
   - Persist cookies in a `net/http/cookiejar.Jar` per controller.
   - On every response, re-read `Set-Cookie` and pick up new `csrftoken`.
   - On 401 / 419 (CSRF), re-login transparently and retry once.
   - Optionally support API-token mode by sending the token as Basic-Auth
     password against `/login` (same flow as a normal password).

2. **Headers (always on)**
   - `X-Avi-Version: <configured or auto-detected>`
   - `X-Avi-Tenant: <configured, default "admin", "*" for cluster-wide>`
   - `Content-Type: application/json` on POST
   - `Accept: application/json`
   - `Referer: https://<controller>/` on POST/PUT/DELETE
   - `X-CSRFToken: <from jar>` on POST/PUT/DELETE
   - `User-Agent: avi-exporter/<version>` (good citizenship; helps the Avi
     team's telemetry)

3. **Discovery pass per scrape (cached)**
   - `GET /api/cluster` once — labels `cluster_name`, `cluster_uuid`,
     `controller_node_*`.
   - `GET /api/cluster/runtime` every scrape — `avi_cluster_state`,
     `avi_cluster_node_state`, `avi_cluster_node_role`.
   - `GET /api/cluster/version` once at start — pin/verify `X-Avi-Version`,
     expose `avi_controller_version_info{version="22.1.5"}`.
   - `GET /api/tenant?page_size=200` — cache id→name for ~5 min.
   - `GET /api/cloud?page_size=200` — cache id→name for ~5 min.
   - `GET /api/serviceenginegroup?page_size=200` — cache id→name for ~5 min.

4. **Inventory pass per scrape**
   - `GET /api/virtualservice-inventory?page_size=200&include_name=true&include=runtime,health_score,alert,faults&page=<n>` — walk pages.
   - `GET /api/pool-inventory?page_size=200&include_name=true&include=runtime,health_score&page=<n>`.
   - `GET /api/serviceengine-inventory?page_size=200&include_name=true&include=runtime,health_score&page=<n>`.
   - Emit: `*_up`, `*_oper_status`, `*_health_score`, `*_alert_level`,
     `*_member_up`, etc., based on the runtime/oper_status fields.

5. **Metrics pass per scrape**
   - **One** `POST /api/analytics/metrics/collection` per metric_entity, with
     all desired `metric_id`s and `entity_uuid: "*"`.
   - `step=300, limit=1` is the sweet spot.
   - Iterate the returned `series` map and emit one Gauge per (metric, entity)
     pair.
   - Fold per-pool-member metrics in by adding a separate batch with
     `metric_entity=VM_METRICS_ENTITY, obj_id="*"`.

6. **Pacing & robustness**
   - Single in-flight scrape per controller (mutex/semaphore).
   - Per-scrape deadline (e.g. 60s).
   - Cache the full result set for `min(scrape_interval, 300s)` so concurrent
     scrapes are cheap — matches mkarnowski's design.
   - Retry once on transient errors (timeout, 502, 503) with jitter, then
     surface as a per-collector failure.

7. **Multi-tenant**
   - For the common case, accept a single `X-Avi-Tenant` per exporter
     instance (or `"*"`).
   - For full discovery, log in as a user with cross-tenant rights and walk
     `/api/tenant` then re-issue inventory per tenant header. Don't try to
     thread tenants through the metrics POST — let it pick up the user's
     scope.

8. **What you can skip**
   - `/api/analytics/healthscore/*` — already embedded in `*-inventory`.
   - The `models` half of `alb-sdk` — write narrow structs.
   - GET-form metrics for bulk scraping — POST collection wins.

---

## 10. Source URLs

Authoritative docs:

- Broadcom TechDocs — Using the Avi REST API (31.2):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/31-2/automation-guide/using-the-rest-api.html
- Broadcom TechDocs — Using the Avi REST API (31.1):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/31-1/automation-guide/using-the-rest-api.html
- Broadcom TechDocs — API Parameters and Filters (31.1):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/31-1/automation-guide/using-the-rest-api/api-parameters-and-filters.html
- Broadcom TechDocs — Retrieving Metrics Through APIs (30.2):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/30-2/monitoring-and-operability-guide/nsx-advanced-load-balancer-monitoring-components/retrieving-metrics-through-avi-apis.html
- Broadcom TechDocs — Avi Prometheus Integration (31.2):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/31-2/monitoring-and-operability-guide/nsx-advanced-load-balancer-monitoring-components/avi-vantage-prometheus-integration.html
- Broadcom TechDocs — User-defined Metrics (31.1):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/31-1/monitoring-and-operability-guide/nsx-advanced-load-balancer-monitoring-components/metrics/user-defined-metrics.html
- Broadcom TechDocs — Accessing the SaaS REST API (30.1) (API token):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/30-1/vmware-avi-load-balancer-administration-guide/avi-saas/accessing-the-avi-saas-rest-api.html
- Broadcom TechDocs — API Configuring the Controller Cluster (31.1):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/31-1/vmware-avi-load-balancer-administration-guide/controller-cluster/controller-cluster-ip/api-configuring-the-avi-controller-cluster.html
- Broadcom TechDocs — Automating Avi with Python (31.1):
  https://techdocs.broadcom.com/us/en/vmware-security-load-balancing/avi-load-balancer/avi-load-balancer/31-1/automation-guide/using-the-avi-python-sdk.html

Developer portal (Swagger reference):

- Avi Load Balancer API root:
  https://developer.broadcom.com/xapis/vmware-avi-load-balancer/latest/
- GET `/virtualservice-inventory`:
  https://developer.broadcom.com/xapis/vmware-avi-load-balancer/latest/api/virtualservice-inventory/get/
- Metrics APIs index:
  https://developer.broadcom.com/xapis/vmware-avi-load-balancer/latest/metrics/
- Health Score APIs:
  https://developer.broadcom.com/xapis/vmware-avi-load-balancer/latest/health-score/
- Service Engine APIs:
  https://developer.broadcom.com/xapis/vmware-avi-load-balancer/latest/service-engine/

Legacy Avi Networks docs (most redirect to Broadcom but still useful for
historical fields):

- API guide root: https://avinetworks.com/docs/latest/api-guide/
- API versioning: https://avinetworks.com/docs/latest/api-versioning/
- Metrics: https://avinetworks.com/docs/17.2/api-guide/metrics/index.html
- Health score: https://avinetworks.com/docs/22.1/api-guide/healthscore/index.html
- VirtualService object: https://avinetworks.com/docs/20.1/api-guide/VirtualService/index.html
- Inventory overview: https://avinetworks.com/docs/17.1/api-guide/inventory.html

Community references:

- Matt Adam — Avi API Parameters and Filters (one of the clearest writeups of
  filtering):
  https://mattadam.com/2023/11/21/avi-api-parameters-and-filters/
- William Lam — Automating default admin password change for NSX ALB
  (PowerShell + CSRF flow):
  https://williamlam.com/2021/03/automating-default-admin-password-change-for-nsx-advanced-load-balancer-nsx-alb.html
- Cloud Blogger — Generate NSX ALB CSRFToken in Orchestrator:
  https://cloudblogger.co.in/2023/09/15/generate-nsx-alb-csrftoken-in-orchestrator/
- Rudi Martinsen — Create Avi Virtual Service from vRA (vRO/JS example with
  X-CSRFToken/Referer/X-Avi-Version):
  https://rudimartinsen.com/2021/11/14/create-avi-vs-from-vra/

Go SDK and reference implementations:

- Official SDK: https://github.com/vmware/alb-sdk
- Go README: https://github.com/vmware/alb-sdk/blob/eng/go/README.md
- pkg.go.dev session: https://pkg.go.dev/github.com/vmware/alb-sdk/go/session
- pkg.go.dev models: https://pkg.go.dev/github.com/vmware/alb-sdk/go/models
- Legacy SDK swagger (still useful for inventory schemas):
  https://github.com/avinetworks/sdk/blob/master/swagger/ServiceEngine.yaml
- Avinetworks devops repo (Grafana / Prometheus reference):
  https://github.com/avinetworks/devops/tree/master/monitoring_tools/grafana/prometheus

Community exporters:

- mkarnowski/avi_prometheus_exporter (Python container):
  https://github.com/mkarnowski/avi_prometheus_exporter
- ticketmaster/TMNET-avi_exporter (Go):
  https://github.com/ticketmaster/TMNET-avi_exporter
- mslocrian/avi_exporter (Go, fork with extra collectors):
  https://github.com/mslocrian/avi_exporter
