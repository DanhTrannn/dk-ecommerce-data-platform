# Structured Access Logs

This document describes the access-event contract and the current Kafka/Flink implementation. Sources: [middleware](../../services/ecommerce-api/app/core/middleware.py), [event builder](../../services/ecommerce-api/app/core/access_logging.py), [producer](../../services/ecommerce-api/app/core/kafka_producer.py), [streaming job](../../pipelines/src/jobs/logs/kafka_to_lakehouse.py).

## 1. Capture boundary

FastAPI middleware assigns a server-generated 32-character hexadecimal request ID and returns it as `X-Request-ID`. Completed requests and handled/unhandled errors produce structured JSON events on stdout and are enqueued asynchronously to Kafka. `GET /health/live` and `GET /health/ready` are excluded. Canonical FastAPI route templates are logged; unmatched paths use `__unmatched__`.

The source is HTTP requests reaching FastAPI, not browser clicks or every Next.js page view. CORS middleware may handle requests before they reach the access middleware. Actor identity is populated by authentication dependencies on the routes that resolve a user; it is not inferred from every request automatically.

## 2. Contract

The [JSON Schema](../contracts/ecommerce-access-v1.schema.json) identifies `ecommerce.access:1.0.0`. It documents the payload shape; the current Kafka job does not run a full JSON Schema validator per event.

| Field | Current meaning |
|---|---|
| `schema.name`, `schema.version` | Contract identity/version |
| `timestamp`, `observed_timestamp` | UTC ISO timestamps for completion and event construction |
| `severity_text`, `severity_number` | INFO/9 below 400, WARN/13 for 4xx, ERROR/17 for 5xx |
| `request.id` | Server-generated request identity; not a trusted caller-supplied ID |
| `trace_id`, `span_id` | Nullable trace fields; no distributed tracing integration is implied |
| `service.*` | Configured name/version/environment; instance from HOSTNAME or service name, up to 128 characters |
| `event.*` | `http.server.request`, category web, kind event, outcome and monotonic duration in ns |
| `http.*` | Uppercase request method, canonical route and response status |
| `actor.type`, `actor.key` | Anonymous, customer, one of seven staff roles, or system; nullable user/system key |
| `client.user_agent` | Optional bounded/sanitized user agent |
| `ecommerce.action` | Action derived from method and route |
| `ecommerce.product_key`, `variant_key` | Allowlisted route identifiers when available |
| `ecommerce.search_query`, `search_redacted` | Normalized/bounded search term or redaction marker |
| `ecommerce.filters` | Allowlisted bounded catalog filters; optional |
| `error.code`, `error.type` | Safe application error metadata; nullable |
| `data_origin` | Contract allows observed/synthetic; runtime builder emits observed |

Optional commerce keys are omitted when unavailable, rather than emitted as JSON null. `actor.key`, trace/span and error values may be null. Service/environment values are configurable, not fixed to a 12-character container ID or a closed list of deployment names. The contract accepts uppercase methods, including HEAD, instead of limiting logs to business endpoint methods.

## 3. Privacy boundary

The event builder does not copy request/response bodies, raw IP, authorization/cookie headers, passwords, JWTs or internal API secrets. Commerce context uses allowlisted fields. Search text is normalized and bounded; email/phone-like values are redacted. User-agent handling removes control characters, limits length and checks PII patterns.

These rules reduce accidental disclosure; pattern sanitization does not prove that every arbitrary input string is free of PII. Actor public IDs remain identifiers. Raw OLTP/CDC privacy is a separate concern and must not be inferred from the access-log schema.

## 4. Producer and failure handling

| Variable | Default in producer |
|---|---|
| `KAFKA_ENABLED` | `true` |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` |
| `KAFKA_ACCESS_LOG_TOPIC` | `ecommerce.access_logs` |
| `KAFKA_DLQ_FILE_PATH` | `data/events/dlq_access_logs.jsonl` |

The producer module defaults to enabled when run without Compose overrides. Compose and `.env.example` default `KAFKA_ENABLED=false` for core-only startup; the launcher sets it to true when `streaming` is selected. An explicitly disabled producer skips events without incrementing the dropped-event counter or writing DLQ records.

`send_access_event` schedules background enqueueing. If the producer is offline, enqueueing raises or the event loop is unavailable, the code increments a process-local counter and attempts a local JSONL DLQ write. The producer awaits `send`, without awaiting each broker-delivery future; it does not establish confirmed delivery for every HTTP event. DLQ writing may also fail and is logged. There is no automatic DLQ replay job, durable outbox, or end-to-end exactly-once guarantee. Compose does not currently mount a dedicated persistent volume for this API-local DLQ path.

Compose forwards the producer topic variable, but the Flink access job reads the same `KAFKA_ACCESS_LOG_TOPIC` variable. Keep the producer and consumer values aligned.

## 5. Landing-first capture and sequential processing

Flink reads Kafka with group `flink-lakehouse-pure-stream`, commits offsets at completed checkpoints and starts new submissions from committed offsets with earliest fallback. Configured checkpointing is 10 seconds in the access job; storage is the mounted Flink checkpoint directory. A new submission does not automatically restore an old checkpoint. The job name stays compatible with existing launch/health checks, with stable source/envelope operator IDs for future controlled restores.

Flink writes **only** `landing.access_logs`. The original decoded payload is retained unchanged; routing/time/status/duration projections are populated when the current parser accepts it. Invalid JSON or an invalid event contract remains in Landing with null projected event fields and an ingestion timestamp. This retains evidence instead of logging/dropping it. It does not make arbitrary raw payloads safe for public exposure; access must remain controlled. A Kafka String deserializer retains decoded text, not the original transport bytes.

| Layer | Writer and behavior |
|---|---|
| Landing | Flink append; raw payload and ingestion envelope, including invalid contracts |
| Bronze | Hourly Spark; nested typed projection plus raw payload, SHA-256 raw identity, validation marker and Landing snapshot lineage |
| Silver | Hourly Spark; valid eligible events flattened/deduplicated by immutable event_id, insert missing IDs |
| Gold fact | Hourly Spark; reads persisted Silver, computes UTC date, duration_ms and HTTP flags, inserts missing IDs |
| Three marts | Hourly Spark; complete aggregation and validated snapshot publication |

Spark recognizes missing raw payloads by hash and missing Silver/fact events by ID, scanning stored inputs rather than using an event-time watermark. Late events can therefore enter a later hourly run. Each layer reads its persisted predecessor independently, so retry catches up after a failure between commits. Invalid contracts remain in Bronze with `_validation_error=invalid_access_event`, without raw content in error messages; no separate quarantine table or automatic invalid-event repair is introduced. Different valid raw payloads sharing one event ID fail processing for investigation. Equivalent JSON text serialized differently is also a different raw payload, so producers must replay immutable event envelopes.

The schema upgrade only adds `raw_payload`, `_raw_hash` and `_validation_error` to Bronze. Legacy typed Bronze rows are kept with null new fields; the first run stores canonical raw observations alongside them. Do not use total Bronze rows as event volume. Existing Silver/fact events and their stored timestamps/lineage are preserved; legacy VN-local values are not silently migrated. New parser/session/date processing uses UTC. `ecommerce.filters` remains NULL in typed projections, matching the previous path; Landing/Bronze raw payload retains it. The parser performs explicit identity/time/status/duration checks, not full JSON Schema validation.

## 6. Hourly publication and historical files

[build_logs_gold](../../pipelines/src/jobs/logs/build_logs_gold.py) now orchestrates the complete persisted sequence before building marts. `lakehouse_logs_marts` runs hourly with one active run and a fixed `--as-of` from the DAG start. Its existing `rollup_logs_gold_marts` task ID is retained. The CLI accepts `--run-id` and optional `--as-of`; without the latter it observes invocation time. Keep standalone writers separate from active DAG runs.

The job pins Landing, Bronze, Silver, then fact snapshots and the published Silver product/variant catalog. It replaces and validates all three mart outputs before appending `system.logs_analytics_publications` schema 1. Source metadata includes all processed layer snapshots. A failure leaves readers on the last completed publication; the underlying layer/mart commits are not one global transaction. Empty fact input clears current mart rows and publishes an empty snapshot set; an empty Landing delta does not erase previously stored facts. Maintenance does not run this pipeline.

Marketing and HTTP Metrics continue to read the log mart publication independently from OLTP publication/cache. Realtime serving from Landing is still pending. Counts, mappings and windows are in [Metrics Catalog](METRICS_CATALOG.md). A checkpoint or refresh interval is not a measured end-to-end latency SLA.

Generator output can be uploaded to this raw prefix:

```text
s3://lakehouse/landing/logs/ingest_date=YYYY-MM-DD/ingest_hour=HH/service=ecommerce-api/<uuid>.jsonl.gz
```

These uploaded historical JSONL files are not consumed automatically by the Kafka job. A configured 30-day generation window is not a storage deletion/retention policy. `data_origin=observed` alone does not prove real user activity: generator exports and simulated API traffic must be identified separately when reporting results.

## 7. Inspect and feed the stream

```bash
# From the repository root, after configuring .env and starting the stack
docker compose --profile streaming logs --tail=200 flink-jobmanager
docker compose exec trino trino --execute "SELECT count(*) FROM lakehouse.gold.fact_web_events;"

# Exercise actual API requests after importing the matching SQL dataset
uv run --locked --package data-generator -- python scripts/simulate_web_traffic.py \
  --config generator/configs/small.yml --duration 300 --concurrency 5

# Historical file upload only; does not populate Kafka/Gold
./scripts/backfill_data.sh --mode logs
```

Use [SETUP](../runbook/SETUP.md) for startup/dependencies and [generator guide](../../generator/README.md) for matched SQL/log datasets.
