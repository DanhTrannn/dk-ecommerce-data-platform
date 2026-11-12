# Ecommerce API Service

FastAPI runs alongside Streamlit in the `platform-app` container. The shared container currently disables Kafka access logging. Analytical dashboards additionally require the data-platform profiles and a completed Lakehouse publication.

FastAPI service for D&K source transactions, structured access logs and analytical serving. [app/models](models) maps 28 OLTP tables; [database/schema.sql](../../../../../database/schema.sql) defines the physical schema. Router registration: [src/platform_app/api/routes/router.py](routes/router.py).

## Structure

| Directory | Responsibility |
|---|---|
| `app/core/` | Settings, JWT/Argon2, errors, middleware and Kafka producer |
| `app/db/` | SQLAlchemy sessions, role/CSRF dependencies and Unit of Work |
| `app/models/` | Transactional model mappings |
| `app/modules/` | Domain routers, request/response schemas and services |
| `app/common/` | Money and pagination helpers |

## Run with Docker

From the repository root, copy `.env.example` to `.env` if it does not exist, configure the values, then run:

```bash
docker compose --profile tools up -d --build platform-app
curl -fsS http://localhost:8000/health/ready
```

Profiles are additive with COMPOSE_PROFILES. The shared container starts Uvicorn and Streamlit without applying schemas or seeds. Compose connects FastAPI to PostgreSQL using psycopg2; the source schema and seed target PostgreSQL; schema bootstrap remains explicit. API readiness checks database connectivity, not schema completeness, Kafka, Trino or Gold processing. See [SETUP](../../../../../docs/setup.md).

## Run on the host

Start PostgreSQL first. Set `API_DATABASE_URL` to the host-reachable PostgreSQL URL using your configured credentials/database; replace the placeholders below. URL-encode credential characters when necessary. Select a local API secret of at least 32 characters.

```bash
docker compose --profile core up -d --wait postgres
export API_DATABASE_URL='postgresql+psycopg2://<POSTGRES_ECOMMERCE_USER>:<POSTGRES_ECOMMERCE_PASSWORD>@127.0.0.1:5432/<POSTGRES_ECOMMERCE_DB>'
export API_SECRET_KEY='local-development-secret-key-at-least-32-characters'
export KAFKA_ENABLED=false
uv run --locked --package platform-app -- python -m database.seeds.seed_catalog
uv run --locked --package platform-app -- uvicorn platform_app.api.main:app --host 0.0.0.0 --port 8000 --reload
```

With Kafka disabled, the current producer path routes access events to the local DLQ. For streaming from a host API, enable Kafka and use its externally advertised listener (default `localhost:9094`), rather than Docker DNS `kafka:9092`. Set the CORS origin for the frontend. A host analytics process also needs a host-reachable Trino URL; see the analytics client configuration below.

For host analytics set `API_TRINO_URL=http://localhost:8084` (or your Trino host port). Docker defaults to `http://trino:8080`; the client also attempts the default host URL when no explicit URL was supplied. Analytical KPI endpoints read a completed `system.analytics_publications` manifest and pin Silver/Gold tables to its Iceberg snapshots. Trino failures can serve a bounded last-good OLAP response; cold or expired caches return 503. These KPI routes do not fall back to PostgreSQL, which remains the transactional source. The dashboard refresh interval is 15 seconds and does not trigger batch or Airflow processing. Admin overview now reads the published admin singleton and order mart, with nullable counters/financial values, explicit total/pending-payment counts and admin_observed_at. System reads the published order mart and observes current PostgreSQL count/value in one statement. Its contract uses source_mode, publication_age_minutes, source_observed_at, reconciliation_status and integer absolute differences. Missing sides remain null; a zero source denominator has an undefined percentage. Bounded cached comparisons are explicitly marked cached and retain their original source observation time. Marketing reads only marts, using independent log (`system.logs_analytics_publications`, schema 1) and OLTP (schema 3) publications. Its response exposes `log_status`/`commerce_status` with per-source availability, observation/publication time and bounded-cache metadata. It has no single combined run ID/timestamp. One unavailable source produces a partial/degraded response while preserving the other; both unavailable without valid cache return 503. Request-event fields replace visitor/purchase proxy names; coupon and review windows are frozen at the commerce observation timestamp. See the Metrics Catalog for the revised contract.

### HTTP metrics contract

`GET /api/v1/admin/analytics/http-metrics?days=7` accepts integer `days` from 1 to 30 (default 7). Admin and System Admin only; other roles receive 403, anonymous callers 401, and invalid windows 422. It reads only `mart_hourly_route_metrics`, pinned by `system.logs_analytics_publications` schema 1, independently of the OLTP reconciliation endpoint. No request triggers a pipeline or queries OLTP for replacement metrics.

The window starts at 00:00 UTC `days - 1` dates before the publication's `observed_at` date and ends at that observation. The response includes `window_start`, `observed_at`, publication/cache status, nullable `totals`, observed `hours`, up to 20 aggregated `routes` ordered by error count then average latency, and up to 20 `latency_groups` ordered by p95. Percentiles remain at hour × route × method grain. Error rate is recomputed from summed counts; average latency weights the mart's rounded group means by request count, excluding groups without a mean. Existing ingestion supplies a duration for each accepted event. The legacy `success_2xx_count` groups responses below 400; the API/UI calls this success / 2xx–3xx rather than 2xx only.

An empty published window has null totals and empty lists; an observed zero remains zero, with null ratios when the denominator is zero. Missing publication/snapshot or Trino failure uses a bounded last-good cache for the same window size, marked degraded, or returns 503. Cached responses retain their original window and observation/publication times. No global percentile or production SLA is inferred.

### HTTP realtime contract

`GET /api/v1/admin/analytics/http-realtime?minutes=15` accepts integers 5–60 (default 15). Admin/System Admin only: other roles 403, anonymous 401, invalid windows 422. Reads `landing.access_logs` through Trino, without a publication or PostgreSQL aggregates. Successful responses use `Cache-Control: no-store`; there is no server last-good cache. Unavailable Landing, expired snapshots or failed queries return a generic 503 without raw error details.

Each read fixes UTC `observed_at` and one current main snapshot from Iceberg `$history`. It includes rows ingested by that cutoff with either event time in `[window_start, observed_at)` or ingestion time since `window_start`. The second branch captures recent invalid/late/future observations. Snapshot metadata and one aggregate query each use an eight-second client timeout. A logical time window does not guarantee physical file pruning on the existing unpartitioned Landing table.

Valid projected envelopes are grouped by event ID. Identical payload hashes count once; IDs with multiple payload hashes are excluded from HTTP metrics. Conflict/replay detection is limited to the scanned scope, not global validation. `quality` distinguishes scanned/invalid/replayed rows from conflicting/late/future IDs. Late IDs have event time before the window; future IDs are at/after the upper boundary. In-window late arrivals are counted when visible and can change earlier minutes.

The response contains `snapshot_id` (string), `snapshot_current_at`, `window_start`, `observed_at`, `queried_at`, latest in-scope ingest/event times, nullable totals, observed minute `series` and at most 20 routes ordered by error count, request count and route/method. Rates and approximate p95 are computed directly over eligible unique requests at each grain; p95 totals are not averaged minute/route percentiles. Duration uses nanoseconds / 1e6; success counts 200–399, errors 400–599, and total requests includes accepted 100–599 statuses. No eligible requests means null totals and empty series/routes; observed zero errors or zero latency remains zero. No raw payload, event IDs, actor identifiers or individual request records are returned.

Realtime polling happens only while its System view is mounted and visible, every 10 seconds with in-flight reuse. A failed refresh retains the previous observation and its timestamp with an error label. Analytical HTTP reporting remains independently published hourly. All recorded API traffic, including dashboard requests, is included; this is not external-user traffic alone. This endpoint is HTTP observation, not business-event realtime or full Silver validation.

## Settings and request conventions

Backend settings are defined in [config.py](core/config.py) and use `API_*`; [Kafka settings](core/kafka_producer.py) use `KAFKA_*`. [Trino settings](modules/analytics/trino_client.py) use their own environment names. Compose only forwards the variables explicitly listed under its API service; adding a new API_* line to the project `.env` does not automatically inject it into that container.

JWT is stored in the HttpOnly `web_access` cookie by default. The readable `web_csrf` cookie is copied into `X-CSRF-Token` for routes that enable double-submit verification. The endpoint table marks those routes. Local cookies are not Secure by default. Internal order confirmation uses `X-Internal-Secret`.

Checkout `Idempotency-Key` must contain 1–59 characters. The optional `shipping_city_code` must name an active city (case-insensitive) or checkout returns 400; it is stored as `orders.shipping_city_id`. Other workflows validate their own key requirements; POS generates an internal key and does not deduplicate client retries in the same way. VietQR and refund success are recorded locally, without an external payment provider callback.

## Endpoint reference

Paths and methods below are registered in source. Authentication columns describe router dependencies plus documented store scope; ownership, state and fine-grained analytics checks still apply in services. `CSRF` identifies an explicit verify_csrf dependency. Swagger UI is `/docs` when API_ENABLE_DOCS is enabled; the OpenAPI document is `/openapi.json`.

| Method | Path | Authentication | CSRF |
|---|---|---|---|
| GET | `/api/v1/admin/overview` | Admin | — |
| GET | `/api/v1/admin/products` | Staff | — |
| POST | `/api/v1/admin/products` | Inventory / admin | Yes |
| PATCH | `/api/v1/admin/products/{public_id}` | Inventory / admin | Yes |
| DELETE | `/api/v1/admin/products/{public_id}` | Inventory / admin | Yes |
| PATCH | `/api/v1/admin/variants/{public_id}` | Inventory / admin | Yes |
| GET | `/api/v1/admin/orders` | Admin | — |
| GET | `/api/v1/admin/orders/{order_number}` | Admin | — |
| POST | `/api/v1/admin/orders/{order_number}/confirm` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/cancel` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/dispatch` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/deliver` | Admin | Yes |
| POST | `/api/v1/admin/orders/{order_number}/failed-delivery` | Admin | Yes |
| GET | `/api/v1/admin/customers` | Admin | — |
| PATCH | `/api/v1/admin/customers/{public_id}` | Admin | Yes |
| POST | `/api/v1/admin/customers/{customer_id}/reinstate-cod` | Admin | Yes |
| GET | `/api/v1/admin/branch-inventory` | Staff | — |
| PATCH | `/api/v1/admin/branch-inventory` | Admin / assigned store manager | Yes |
| POST | `/api/v1/admin/inventory/refill` | Admin | Yes |
| GET | `/api/v1/admin/analytics/stores` | Staff, analytics role/store checks | — |
| GET | `/api/v1/admin/analytics/overview` | Admin | — |
| GET | `/api/v1/admin/analytics/role-metrics` | Staff, analytics role/store checks | — |
| GET | `/api/v1/admin/analytics/sales-trend` | Sales manager / admin | — |
| GET | `/api/v1/admin/analytics/http-metrics` | Admin / System Admin | — |
| GET | `/api/v1/admin/analytics/http-realtime` | Admin / System Admin | — |
| POST | `/api/v1/auth/register` | Public | — |
| POST | `/api/v1/auth/login` | Public | — |
| POST | `/api/v1/auth/logout` | Public | Yes |
| GET | `/api/v1/auth/me` | Authenticated user | — |
| GET | `/api/v1/cart` | Authenticated user | — |
| PUT | `/api/v1/cart/items/{variant_public_id}` | Authenticated user | Yes |
| DELETE | `/api/v1/cart/items/{variant_public_id}` | Authenticated user | Yes |
| GET | `/api/v1/categories` | Public | — |
| GET | `/api/v1/catalog/facets` | Public | — |
| GET | `/api/v1/products` | Public | — |
| GET | `/api/v1/products/{slug}` | Public | — |
| POST | `/api/v1/checkout/quote` | Authenticated user | — |
| POST | `/api/v1/checkout` | Authenticated user | Yes |
| GET | `/api/v1/coupons/available` | Authenticated user | — |
| GET | `/api/v1/admin/coupons` | Marketing / admin | — |
| POST | `/api/v1/admin/coupons` | Marketing / admin | Yes |
| PATCH | `/api/v1/admin/coupons/{public_id}` | Marketing / admin | Yes |
| DELETE | `/api/v1/admin/coupons/{public_id}` | Marketing / admin | Yes |
| POST | `/api/v1/admin/inbound/receipts` | Inventory / admin | Yes |
| GET | `/api/v1/admin/inbound/receipts` | Inventory / admin | — |
| GET | `/api/v1/admin/inbound/receipts/{receipt_code}` | Inventory / admin | — |
| GET | `/api/v1/locations/cities` | Public | — |
| GET | `/api/v1/locations/cities/{city_code}/stores` | Public | — |
| GET | `/api/v1/catalog/products/{slug}/availability` | Public | — |
| GET | `/api/v1/admin/delivery-staff` | Admin | — |
| POST | `/api/v1/admin/delivery-staff` | Admin | Yes |
| PATCH | `/api/v1/admin/delivery-staff/{staff_id}` | Admin | Yes |
| GET | `/api/v1/admin/shipments` | Admin | — |
| GET | `/api/v1/admin/shipments/{order_number}` | Admin | — |
| GET | `/api/v1/orders` | Authenticated user | — |
| GET | `/api/v1/orders/{order_number}` | Authenticated user | — |
| POST | `/api/v1/orders/{order_number}/cancel` | Authenticated user | Yes |
| POST | `/api/v1/orders/{order_number}/complete` | Authenticated user | Yes |
| POST | `/internal/v1/orders/{order_number}/confirm` | Internal secret | — |
| GET | `/api/v1/pos/products` | Staff, POS store scope | — |
| POST | `/api/v1/pos/transactions` | Staff, POS store scope | Yes |
| POST | `/api/v1/orders/{order_number}/returns` | Authenticated user | Yes |
| GET | `/api/v1/returns` | Authenticated user | — |
| GET | `/api/v1/returns/{return_code}` | Authenticated user | — |
| POST | `/api/v1/returns/{return_code}/cancel` | Authenticated user | Yes |
| GET | `/api/v1/admin/returns` | Admin | — |
| GET | `/api/v1/admin/returns/{return_code}` | Admin | — |
| POST | `/api/v1/admin/returns/{return_code}/review` | Admin | Yes |
| POST | `/api/v1/admin/returns/{return_code}/receive` | Admin | Yes |
| POST | `/api/v1/admin/returns/{return_code}/inspect-and-resolve` | Admin | Yes |
| GET | `/api/v1/products/{slug}/reviews` | Public (optional login) | — |
| POST | `/api/v1/orders/{order_number}/items/{order_item_public_id}/review` | Authenticated user | Yes |
| GET | `/api/v1/admin/reviews` | Marketing / admin | — |
| PATCH | `/api/v1/admin/reviews/{public_id}` | Marketing / admin | Yes |
| GET | `/api/v1/admin/store/dashboard` | Store manager / admin, store scope | — |
| GET | `/api/v1/admin/store/orders` | Store manager / admin, store scope | — |
| GET | `/api/v1/admin/store/orders/{order_number}` | Store manager / admin, store scope | — |
| POST | `/api/v1/admin/store/orders/{order_number}/confirm` | Store manager / admin, store scope | — |
| POST | `/api/v1/admin/store/orders/{order_number}/cancel` | Store manager / admin, store scope | — |
| GET | `/api/v1/admin/store/inventory` | Store manager / admin, store scope | — |
| GET | `/api/v1/admin/store/staff` | Store manager / admin, store scope | — |
| GET | `/api/v1/wishlist` | Authenticated user | — |
| PUT | `/api/v1/wishlist/products/{product_public_id}` | Authenticated user | Yes |
| DELETE | `/api/v1/wishlist/products/{product_public_id}` | Authenticated user | Yes |
| GET | `/health/live` | Public | — |
| GET | `/health/ready` | Public | — |

## Current limits

- Admin order, delivery and return APIs require admin. Operations managers have a dashboard/UI menu but do not currently have these API write permissions.
- Store confirm/cancel routes enforce store scope/idempotency, but omit the CSRF dependency.
- Branch inventory PATCH and inventory/refill POST currently flush without committing in their router or get_db dependency. A 204 response is not proof those writes persisted. See [admin router](modules/admin/router.py).
- Category CRUD, customer self-service profile editing, separate shipper login and size/color exchange workflows are not implemented.
- Local HTTP, development defaults and published service ports are not a completed production security setup.

## References

[Business requirements](../../../../../docs/architecture.md#1-phạm-vi-và-hiện-trạng) · [OLTP schema](../../../../../docs/data.md#2-schema-oltp) · [access logs](../../../../../docs/data.md#4-access-log-contract-và-xử-lý) · [metric definitions](../../../../../docs/data.md#5-dimension-fact-và-18-mart)
