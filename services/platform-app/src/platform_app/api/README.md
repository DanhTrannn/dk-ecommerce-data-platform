# Ecommerce API and Access Administration

FastAPI runs beside the Streamlit Platform Console in the shared `platform-app` container. Its active surface comprises source transactions, the ID lookups needed by scenarios, two HTTP observability reads, and Polaris administration under `/api/v1/access`. The Streamlit console uses this API; business sales BI uses the separate Superset PoC.

[Models](models) map 28 OLTP tables; the [physical schema](../../../../../database/schema.sql) is PostgreSQL. [Router registration](routes/router.py) and [handlers](modules) define supported endpoints. There is no separate access-admin web service: access routes share the API process, cookie session and port 8000. The credential exporter is a separate internal service on port 8011.

## Structure

Paths below are relative to this directory, `services/platform-app/src/platform_app/api/`.

| Directory | Responsibility |
|---|---|
| `core/` | Settings, JWT/Argon2, errors, middleware and structured access logs |
| `db/` | SQLAlchemy sessions, role/CSRF dependencies and transactional Unit of Work |
| `models/` | Source model mappings |
| `modules/` | Domain routers, request/response schemas and services |
| `routes/` | Router registration and health checks |
| `common/` | Money and pagination helpers |

Shared-container components are siblings: [access administration](../access_admin), [provisioning](../provisioning), [credential export](../credential_export) and [Streamlit UI](../ui). See the [platform-app guide](../../../README.md) for the console and workload credential flow.

## Run with Docker

From the repository root, configure `.env` using `.env.example` and the [setup guide](../../../../../docs/SETUP.md), then:

```bash
make up SERVICES=platform-app
curl -fsS http://127.0.0.1:8000/health/ready
```

Compose starts the dependencies of `platform-app`, including `platform-admin-init` and the credential exporter. The init job prepares private storage, applies `database/schema.sql` only when the source public schema has no tables, and creates the configured bootstrap admin. It preserves existing accounts and passwords and rejects an incomplete schema. Run `make seed` for catalog and admin seed records.

Default addresses are API `http://127.0.0.1:8000`, Swagger `http://127.0.0.1:8000/docs` and Streamlit `http://127.0.0.1:8501`. Compose uses configurable loopback bind addresses. `/health/live` checks the process. `/health/ready` executes `SELECT 1` on the source database.

## Run the API on the host

Use Python 3.11 and the repository uv workspace (`uv sync --locked`). Start PostgreSQL and initialize the source/admin through the supported Compose bootstrap sequence first. For a host API, set `API_DATABASE_URL` to your host-reachable PostgreSQL database and `API_SECRET_KEY` to a local secret of at least 32 characters. Set `API_BOOTSTRAP_ADMIN_EMAIL` to the same bootstrap account if using access administration. Do not reuse the container database hostname from a host process.

```bash
# After configuring the required environment variables:
uv run --locked --package platform-app -- uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --reload
```

API settings load the root `.env` through Pydantic when launched there. The Trino module reads the process environment directly. Export its variables explicitly or pass `--env-file .env` to `uv run`. Infrastructure administration needs provider credentials, private credential and audit paths, and exporter configuration. Compose supplies these mounts and settings. FastAPI writes structured access logs to stdout.

For the two analytics reads, the [Trino client](modules/analytics/trino_client.py) chooses `API_TRINO_URL`, then `TRINO_URL`, then `https://trino:8443`. A host process should use `https://localhost:8084` (or its configured port), a trusted CA file in `TRINO_CA_FILE`, and the read-only `TRINO_READER_USER`/`TRINO_READER_PASSWORD` credentials. Compose mounts its CA and forwards the reader password. TLS verification is enabled; the client uses Basic authentication when that password exists. Default Docker/host URL probing does not provision credentials or trust.

## HTTP observability

The six former business BI reads are retired: GET `/api/v1/admin/overview`, `/api/v1/admin/store/dashboard` and `/api/v1/admin/analytics/{overview,role-metrics,sales-trend,stores}`. The retained analytics router has only the two endpoints below. Historical role dashboards and their types are not a current backend contract. Business sales serving uses the separately configured Superset PoC; see [architecture](../../../../../docs/ARCHITECTURE.md).

### HTTP metrics contract

`GET /api/v1/admin/analytics/http-metrics?days=7` accepts integer `days` from 1 to 30 (default 7). Admin and System Admin only; other roles receive 403, anonymous callers 401, and invalid windows 422. It reads only `mart_hourly_route_metrics`, pinned by `system.logs_analytics_publications` schema 1 (OLTP Gold has no publication; the former reconciliation endpoint is retired). No request triggers a pipeline or queries OLTP for replacement metrics.

The date filter starts at 00:00 UTC `days - 1` dates before the publication's `observed_at` date and includes that observation's UTC date. SQL filters `metric_date`, without a separate intra-day upper cutoff; the pinned publication determines which hourly groups are present. The response includes `window_start`, `observed_at`, publication/cache status, nullable `totals`, observed `hours`, up to 20 aggregated `routes` ordered by error count then average latency, and up to 20 `latency_groups` ordered by p95. Percentiles remain at hour × route × method grain. Error rate is recomputed from summed counts; average latency weights the mart's rounded group means by request count, excluding groups without a mean. Existing ingestion supplies a duration for each accepted event. The legacy `success_2xx_count` groups responses below 400; the API/UI calls this success / 2xx–3xx rather than 2xx only.

An empty published window has null total fields and empty lists; an observed zero remains zero, with null ratios when the denominator is zero. Missing publication/snapshot or Trino failure uses a bounded last-good cache for the same window size, marked degraded, or returns 503. Cached responses retain their original window and observation/publication times. No global percentile or production SLA is inferred.

### HTTP realtime contract

`GET /api/v1/admin/analytics/http-realtime?minutes=15` accepts integers 5–60 (default 15). Admin/System Admin only: other roles 403, anonymous 401, invalid windows 422. Reads `landing.access_logs` through Trino, without a publication or PostgreSQL aggregates. Successful responses use `Cache-Control: no-store`; there is no server last-good cache. Unavailable Landing, expired snapshots or failed queries return a generic 503 without raw error details.

Each read fixes UTC `observed_at` and one current main snapshot from Iceberg `$history`. It includes rows ingested by that cutoff with either event time in `[window_start, observed_at)` or ingestion time since `window_start`. The second branch captures recent invalid/late/future observations. Snapshot metadata and one aggregate query each use an eight-second client timeout. A logical time window does not guarantee physical file pruning on the existing unpartitioned Landing table.

Valid projected envelopes are grouped by event ID. Identical payload hashes count once; IDs with multiple payload hashes are excluded from HTTP metrics. Conflict/replay detection is limited to the scanned scope, not global validation. `quality` distinguishes scanned/invalid/replayed rows from conflicting/late/future IDs. Late IDs have event time before the window; future IDs are at/after the upper boundary. In-window late arrivals are counted when visible and can change earlier minutes.

The response contains `snapshot_id` (string, or null when the table has no snapshot), `snapshot_current_at`, `window_start`, `observed_at`, `queried_at`, latest in-scope ingest/event times, nullable totals, observed minute `series` and at most 20 routes ordered by error count, request count and route/method. Rates and approximate p95 are computed directly over eligible unique requests at each grain; p95 totals are not averaged minute/route percentiles. Duration uses nanoseconds / 1e6; success counts 200–399, errors 400–599, and total requests includes accepted 100–599 statuses. No eligible requests means null total fields and empty series/routes; observed zero errors or zero latency remains zero. No raw payload, event IDs, actor identifiers or individual request records are returned.

The endpoint reports HTTP observations, not business-event realtime or full Silver validation. Captured traffic includes console/analytics requests as well as shopping requests. Polling is a client concern and does not trigger pipeline execution.

## Settings and request conventions

API settings use `API_*`. Analytics also reads Trino settings and `API_ANALYTICS_CACHE_MAX_AGE_SECONDS` directly. Compose injects the variables listed in the service environment. Published HTTP responses use a process-local last-good cache keyed by `days`, with a maximum age of 3600 seconds by default and up to 128 keys. Failed reads with no usable cached response return 503. [API settings](core/config.py)

JWT defaults to the HttpOnly `web_access` cookie; login/register also set readable `web_csrf`. Routes marked Yes below require the cookie value in `X-CSRF-Token`. Cookies are not Secure by default in local configuration. Logout requires CSRF but does not require a valid authenticated customer. Internal order confirmation uses `X-Internal-Secret` instead of a customer cookie.

Checkout requires a nonblank trimmed `Idempotency-Key` of at most 59 characters. Customer cancel/complete, internal/admin confirm, admin cancel, inbound receipt creation, return creation and return inspection require their own nonblank keys of at most 64 characters. Store confirm/cancel require a key and CSRF, then check assigned store and order scope. POS creates an internal key rather than deduplicating arbitrary client retries. Other logistics transitions apply service-level key/state checks; inspect their handlers before replaying uncertain writes. Optional checkout `shipping_city_code` must identify an active city (case-insensitive), otherwise 400. VietQR/refund success is recorded locally; there is no external payment-provider callback route.

`GET /api/v1/admin/customers` is an exact-email scenario lookup: required `email` has length 3–320. It is not an unrestricted customer listing. Admin product search and branch stock reads remain available for scenario IDs. Store-manager stock reads are restricted to the assigned store; branch adjustments permit only admin or the assigned store manager. Adjustments and warehouse-to-store refill run through transactional Unit of Work, updating stock versions and inventory ledger records together.

## Endpoint reference

The following table covers all registered application routes. `/docs`, `/docs/oauth2-redirect` and `/openapi.json` are framework routes; Swagger UI depends on `API_ENABLE_DOCS` (default true), while OpenAPI remains enabled. Runtime `/openapi.json` and source registration are the authoritative contract.

Authentication names: **Customer** means any active cookie-authenticated user; ownership/state checks still apply. **Admin** means exactly `admin`, not `system_admin`. **Staff** includes `admin`, `store_manager`, `sales_manager`, `marketing_manager`, `inventory_manager`, `operations_manager`, `system_admin`. **Inventory** means `admin`/`inventory_manager`; **Marketing** means `admin`/`marketing_manager`. **Bootstrap admin** additionally verifies that the enabled credential belongs to the configured bootstrap email. Provider path values accept only `minio` or `polaris`; invalid values return 422. Public catalog/location/review reads resolve a valid optional login for request attribution.

| Method | Path | Authentication / scope | CSRF |
|---|---|---|---|
| GET | `/health/live` | Public | — |
| GET | `/health/ready` | Public | — |
| POST | `/api/v1/access/login` | Bootstrap admin credentials | — |
| GET | `/api/v1/access/me` | Bootstrap admin | — |
| POST | `/api/v1/access/logout` | Public | Yes |
| GET | `/api/v1/access/audit` | Bootstrap admin | — |
| GET | `/api/v1/access/{provider}` | Bootstrap admin | — |
| POST | `/api/v1/access/{provider}/bindings` | Bootstrap admin | Yes |
| POST | `/api/v1/access/{provider}/resources` | Bootstrap admin | Yes |
| POST | `/api/v1/access/polaris/credentials/{name}` | Bootstrap admin | Yes |
| PATCH | `/api/v1/access/{provider}/resources` | Bootstrap admin | Yes |
| GET | `/api/v1/access/polaris/workload-credentials` | Bootstrap admin | — |
| POST | `/api/v1/access/polaris/workload-credentials` | Bootstrap admin | Yes |
| POST | `/api/v1/auth/register` | Public | — |
| POST | `/api/v1/auth/login` | Public | — |
| POST | `/api/v1/auth/logout` | Public | Yes |
| GET | `/api/v1/auth/me` | Customer | — |
| GET | `/api/v1/admin/products` | Staff | — |
| POST | `/api/v1/admin/products` | Inventory | Yes |
| PATCH | `/api/v1/admin/products/{public_id}` | Inventory | Yes |
| DELETE | `/api/v1/admin/products/{public_id}` | Inventory | Yes |
| PATCH | `/api/v1/admin/variants/{public_id}` | Inventory | Yes |
| GET | `/api/v1/admin/orders/{order_number}` | Admin | — |
| POST | `/api/v1/admin/orders/{order_number}/confirm` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/cancel` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/dispatch` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/deliver` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/failed-delivery` | Admin | Yes |
| GET | `/api/v1/admin/customers` | Admin | — |
| PATCH | `/api/v1/admin/customers/{public_id}` | Admin | Yes |
| POST | `/api/v1/admin/customers/{customer_id}/reinstate-cod` | Admin | Yes |
| GET | `/api/v1/admin/branch-inventory` | Staff; store scope | — |
| PATCH | `/api/v1/admin/branch-inventory` | Admin / assigned store_manager | Yes |
| POST | `/api/v1/admin/inventory/refill` | Admin | Yes |
| GET | `/api/v1/admin/analytics/http-realtime` | Admin / system_admin | — |
| GET | `/api/v1/admin/analytics/http-metrics` | Admin / system_admin | — |
| POST | `/api/v1/admin/inbound/receipts` | Inventory | Yes |
| GET | `/api/v1/admin/inbound/receipts/{receipt_code}` | Inventory | — |
| GET | `/api/v1/admin/store/orders/{order_number}` | Admin / assigned store_manager | — |
| POST | `/api/v1/admin/store/orders/{order_number}/confirm` | Admin / assigned store_manager | Yes |
| POST | `/api/v1/admin/store/orders/{order_number}/cancel` | Admin / assigned store_manager | Yes |
| POST | `/api/v1/admin/coupons` | Marketing | Yes |
| PATCH | `/api/v1/admin/coupons/{public_id}` | Marketing | Yes |
| DELETE | `/api/v1/admin/coupons/{public_id}` | Marketing | Yes |
| PATCH | `/api/v1/admin/reviews/{public_id}` | Marketing | Yes |
| GET | `/api/v1/admin/delivery-staff` | Admin | — |
| POST | `/api/v1/admin/delivery-staff` | Admin | Yes |
| PATCH | `/api/v1/admin/delivery-staff/{staff_id}` | Admin | Yes |
| GET | `/api/v1/coupons/available` | Customer | — |
| GET | `/api/v1/categories` | Public; optional login | — |
| GET | `/api/v1/catalog/facets` | Public; optional login | — |
| GET | `/api/v1/products` | Public; optional login | — |
| GET | `/api/v1/products/{slug}` | Public; optional login | — |
| GET | `/api/v1/cart` | Customer | — |
| PUT | `/api/v1/cart/items/{variant_public_id}` | Customer | Yes |
| DELETE | `/api/v1/cart/items/{variant_public_id}` | Customer | Yes |
| GET | `/api/v1/locations/cities` | Public; optional login | — |
| GET | `/api/v1/locations/cities/{city_code}/stores` | Public; optional login | — |
| GET | `/api/v1/catalog/products/{slug}/availability` | Public; optional login | — |
| POST | `/api/v1/checkout/quote` | Customer | — |
| POST | `/api/v1/checkout` | Customer | Yes |
| GET | `/api/v1/orders` | Customer | — |
| GET | `/api/v1/orders/{order_number}` | Customer | — |
| POST | `/api/v1/orders/{order_number}/cancel` | Customer | Yes |
| POST | `/api/v1/orders/{order_number}/complete` | Customer | Yes |
| GET | `/api/v1/products/{slug}/reviews` | Public; optional login | — |
| POST | `/api/v1/orders/{order_number}/items/{order_item_public_id}/review` | Customer | Yes |
| GET | `/api/v1/pos/products` | Staff; assigned store (admin/system_admin exempt) | — |
| POST | `/api/v1/pos/transactions` | Staff; assigned store (admin/system_admin exempt) | Yes |
| GET | `/api/v1/wishlist` | Customer | — |
| PUT | `/api/v1/wishlist/products/{product_public_id}` | Customer | Yes |
| DELETE | `/api/v1/wishlist/products/{product_public_id}` | Customer | Yes |
| POST | `/api/v1/orders/{order_number}/returns` | Customer | Yes |
| GET | `/api/v1/returns` | Customer | — |
| GET | `/api/v1/returns/{return_code}` | Customer | — |
| POST | `/api/v1/returns/{return_code}/cancel` | Customer | Yes |
| GET | `/api/v1/admin/returns/{return_code}` | Admin | — |
| POST | `/api/v1/admin/returns/{return_code}/review` | Admin | Yes |
| POST | `/api/v1/admin/returns/{return_code}/receive` | Admin | Yes |
| POST | `/api/v1/admin/returns/{return_code}/inspect-and-resolve` | Admin | Yes |
| POST | `/internal/v1/orders/{order_number}/confirm` | Internal secret | — |

## Removed reads and current limits

In addition to the six retired BI routes above, these ten former administrative GET routes are absent: `/api/v1/admin/orders`, `/api/v1/admin/inbound/receipts`, `/api/v1/admin/coupons`, `/api/v1/admin/reviews`, `/api/v1/admin/returns`, `/api/v1/admin/shipments`, `/api/v1/admin/shipments/{order_number}`, `/api/v1/admin/store/orders`, `/api/v1/admin/store/inventory`, `/api/v1/admin/store/staff`. Retained POST/detail routes with related prefixes do not restore their former list APIs. Unsupported requests may return 404 or 405 depending on another registered method/path; they are not supported reads.

- Order, delivery-staff and admin return operations require `admin`. Operations-manager roles do not grant these API write permissions.
- Category CRUD, self-service customer profile editing, a separate shipper login and size/color exchange workflows are absent.
- Access administration supports Polaris principal/role/catalog resources, bindings and explicit workload credential assignment. MinIO is not administered through the API; `minio-init` creates the one storage account Polaris uses. Sensitive credential responses use `Cache-Control: no-store`; access audit data and saved credentials use private persistent paths.
- This guide verifies registrations and source behavior. Successful runtime transactions still require initialized source data, provider permissions and reachable dependencies.

## References

[Setup](../../../../../docs/SETUP.md) · [Architecture and serving](../../../../../docs/ARCHITECTURE.md) · [Schema, access logs and marts](../../../../../docs/DATA.md) · [Operations and recovery](../../../../../docs/OPERATIONS.md)
