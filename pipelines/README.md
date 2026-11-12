# Lakehouse Pipelines

Batch and log processing for the Lakehouse. Engines: **Polars 2.1 + PyIceberg 0.12** for ingestion, quality audit, publication and maintenance; **dbt 1.11 (dbt-trino)** for Gold and marts on Trino; **Flink 1.19** only appends API access logs to Landing. There is no Spark or CDC path.

## Setup

1. Bootstrap the PostgreSQL source schema and run `make source-reader` (SELECT-only role on the 27 analytical tables; `customer_credentials` excluded).
2. Configure `SILVER_PSEUDONYMIZE_SALT`, the MinIO batch workload keys (`MINIO_BATCH_*`), the bucket and the Polaris credential file produced by `make provision-lakehouse`.
3. Build the images: `infrastructure/batch/Dockerfile` (CLI client, profile `batch`) and `infrastructure/airflow/Dockerfile` (Airflow with the same environment at `/opt/pipeline-venv`, exposed as `PIPELINE_PYTHON`).

## DAGs

| DAG | Schedule | Tasks |
|---|---|---|
| `init_lakehouse_oltp` | manual | `ingest_source` (all rows) → `build_validate_publish` |
| `lakehouse_oltp_pipeline` | `0 3 * * *`, one active run | `ingest_source` (interval `[start, end)`) → `build_validate_publish` |
| `lakehouse_logs_marts` | hourly | `rollup_logs_gold_marts` |
| `lakehouse_oltp_maintenance` | `0 4 * * *` | Iceberg compaction/snapshot expiry for OLTP tables |
| `lakehouse_streaming_maintenance` | every 2 hours | Flink health check, then maintenance for log tables |

Do not overlap `init_lakehouse_oltp` and `lakehouse_oltp_pipeline`; there is no cross-DAG lock. Airflow passes its fixed run start as `--as-of`, reused on retries.

## OLTP batch

1. `jobs.oltp.run_polars_batch` reads each of the 27 tables from PostgreSQL in primary-key order, at most 10,000 rows per page. Mutable tables use `updated_at`, append-only tables `created_at`; timestamps are UTC and there is no lookback.
2. Bronze keeps every source version (primary key + change time), so replays do not duplicate. Silver validates, deduplicates, pseudonymizes PII (SHA-256 with the salt) and merges by primary key; invalid rows go to quarantine.
3. `jobs.oltp.build_oltp_gold` pins the Silver snapshots, runs the dbt project (`pipelines/dbt/models/oltp`: 6 dimensions, 6 facts, 15 marts) on Trino, audits the results (including the dbt data tests in `pipelines/dbt/tests`) and publishes a snapshot manifest to `system.analytics_publications`. A failed gate leaves the previous publication readable.

Source tables, keys, cursors and PII mappings are declared once in `SOURCE_TABLES` (`src/lakehouse/utils/config.py`). Source hard deletes are not synchronized: rows deleted in PostgreSQL remain in Silver. `run_reconciliation_gate.py` remains an explicit diagnostic for a quiescent source.

## Access logs

Flink (`kafka_to_lakehouse_pure_streaming`, submitted with `flink-submit`) appends every decoded Kafka payload to `landing.access_logs`, including invalid contracts. `jobs.logs.build_logs_gold` then pins Landing, inserts missing raw hashes into Bronze, deduplicates valid events into Silver and `fact_web_events`, runs the dbt log models (`pipelines/dbt/models/logs`) for the three marts against the published OLTP catalog, audits them and publishes to `system.logs_analytics_publications`. Conflicting valid payloads sharing an event ID fail the run instead of being chosen arbitrarily.

## Layout

```text
src/lakehouse/
├── analytics/     # dbt runner and OLTP Gold build/publication
├── oltp/          # bronze, silver, polars pipeline, gold schema, quality, publication
├── logs/          # Landing adapter, parser, Polars layers, marts and publication
├── streaming/     # Flink access-log runtime and sink setup
├── utils/         # config, Iceberg/Polaris catalog, PostgreSQL, SQL loader, maintenance
└── sql_templates/ # contracts (Silver/Gold/log schemas), flink, postgres
src/jobs/          # CLIs: oltp, logs, maintenance
dbt/               # dbt project: models/oltp, models/logs, data tests
```

## Tests

Tests under `pipelines/tests/` are kept local only: every `tests/` directory is gitignored except the dbt data tests in `pipelines/dbt/tests/`.

```bash
PYTHONPATH=pipelines/src:pipelines/tests uv run --locked --all-packages -- \
  python -m unittest discover -s pipelines/tests -v
```

The opt-in stack fixture (`pipelines/tests/stack/runtime.py`) still seeds its source with the removed `data-generator` package and cannot run until the new OLTP generator exists.

## Acceptance history

The records below were captured before the Spark-to-Polars/dbt migration and the CDC removal. Commands and engine details in them are historical.

### Verification evidence — 07/11/2026

The disposable integration check passed with 27 PostgreSQL source tables, all 30 source-to-Silver comparisons at zero tolerance, all 27 OLTP Gold tables, 55 Gold quality checks and an Iceberg publication. It also verified paged extraction, empty intervals, replay, interval boundaries and legacy-source rejection. The check exposed and fixed Silver rejecting the valid `order_cancellation` inventory movement; unknown movement types remain quarantined. Eight pure PostgreSQL SQL/config checks also passed. These observations apply to the small synthetic fixture and local test catalog, not an existing deployment or all scenario distributions.

### Full-stack verification evidence — 08/11/2026

The isolated Compose acceptance passed with real PostgreSQL, MinIO/Polaris, Airflow 3.3.2 scheduler/LocalExecutor and Trino 483. Both manually triggered OLTP DAGs completed 109/109 tasks. Initial publication contained 30 orders worth 8,800,880 VND. Two API-runner COD checkouts added 498,000 VND: the incremental publication contained 32 orders worth 9,298,880 VND in both fact and order mart. Net recognized merchandise revenue remained 5,303,400 VND because the new COD orders were unpaid. Both source-to-Silver diagnostics passed 30/30 comparisons at zero tolerance.

Trino SELECT/time travel worked; INSERT/DROP were denied. Revoking the Spark role denied a new Spark job without changing the completed reader publication; restoring it recovered access. Replaying order Bronze/Silver kept 32 rows in each layer. Increasing every fact order total by 1 VND caused both source-to-Gold and additive order-mart parity checks to reject publication, while the pinned reader retained correct values. Rebuilding the fact passed 55/55 checks and republishing the same run kept two publication rows in total. Test containers, volumes, network and private configuration were removed.

The check exposed the manual initial-run date-template issue and missing Trino connector read-only enforcement, both corrected here. It validates this small fixture and the manually triggered batch path only; it does not establish a production latency SLA, cron creation, CDC, log streaming or Superset readiness. Generated report: `data/pipeline-runs/tlcn-stack-2385ac19c1/report.json` (ignored, local).

### Rewritten generator acceptance — 09/11/2026

Historical record: generator 0.10, its importer and `small.yml` have since been removed.

After the EDA merge and application build repair, the full `small.yml` scale passed against actual PostgreSQL, Airflow/Spark, MinIO/Polaris and Trino: 500 configured customers, 60 products, 240 variants and 3,000 orders. The fixture imported SQL using the production transactional importer and recorded generator 0.10.0, dataset identity, configured scale and SQL SHA-256. The full-preset SQL fingerprint was reproducible across isolated attempts.

Both OLTP DAGs completed 109/109 tasks. Published fact/order-mart values read through Trino were:

| Metric | Initial load | After two API COD checkouts |
|---|---:|---:|
| Orders | 3,000 | 3,002 |
| Placed order value (VND) | 1,902,898,430 | 1,903,396,430 |
| Net recognized merchandise revenue (VND) | 1,358,389,307 | 1,358,389,307 |

Both source-to-Silver diagnostics passed 30/30 comparisons at zero tolerance. Trino denied INSERT/DROP; revoked Spark access denied a new job and role restoration recovered access. Order replay retained 3,002 Bronze and Silver rows. Intentional total corruption failed both source-to-Gold and additive order-mart parity checks, leaving the pinned publication readable. Recovery passed 55/55 checks; republishing the same run retained two publication rows. Private containers, volumes, network and configuration were cleaned up.

The separate generator suite passed 33 tests, including product-weight normalization, first-product selection independent of variant counts and exhaustion without zero-weight fallback. PostgreSQL-backed platform discovery passed 111 tests with the two separate provider live tests skipped (`RUN_POSTGRES_SCHEMA_TESTS=1 POSTGRES_TEST_IMAGE=postgres:16.8`). That suite also covers multiple full-preset seeds, duplicate import rejection, transaction/sequence rollback and persisted API purchases.

Evidence is local and ignored: `data/pipeline-runs/tlcn-stack-25aa75d70a/report.json`, `generator-stack-check.log`, `generator-unit-check.log` and `generator-postgres-check.log` under `data/pipeline-runs/`. This is post-merge technical acceptance, not proof of market representativeness, cron timing, CDC/log delivery, Superset readiness or a performance SLA. PostgreSQL CDC and actual API log capture remain the next separate phase.

### PostgreSQL CDC acceptance — 09/11/2026

The opt-in actual-stack check passed with Debezium 3.0.8, Kafka 3.8, Flink 1.19 and Iceberg/MinIO/Polaris. Its managed publication/FULL replica identity covered 27 tables and all 594 snapshot rows matched source counts per table, including zero-row tables. Source setup rejected an incomplete schema, preserved isolation, reran successfully, removed old owner membership and denied credential reads/business inserts. Kafka and Bronze excluded customer_credentials.

The pre-PostgreSQL audit fixture row and binlog fields survived additive schema evolution. Live INSERT/UPDATE/DELETE produced full previous/new values, positive PostgreSQL LSN/txId, stable transport IDs, delete envelopes and NULL-value tombstones; transaction BEGIN/END were retained. Detached submission returned with the remote job RUNNING. A TaskManager restart recovered from a completed checkpoint and captured another insert; a Connect restart retained its pgoutput slot, did not repeat snapshot/prior row operations and captured a later update. No duplicate event IDs or Silver/Gold tables were observed in the fixture.

Five pure CDC contract checks passed. The normal pipeline suite passed 16 tests with three integration cases skipped; CDC integration was enabled separately with `RUN_POSTGRES_CDC_TESTS=1`. The private resources were cleaned and the local report is `data/pipeline-runs/tlcn-stack-085770b323/report.json` (ignored). Full JobManager disaster recovery, arbitrary resubmission/offset resets, load/latency SLAs and API access-log delivery are not established by this check. See [CDC operations](../docs/operations.md#cdcflinkkafka) for setup and recovery boundaries.


### Real access-log acceptance (opt-in)

```bash
RUN_ACCESS_LOG_STACK_TESTS=1 PYTHONPATH=pipelines/src:pipelines/tests \
  uv run --package batch-pipeline python -m unittest discover -s pipelines/tests -p test_logs_stack.py -v
```

The private Compose fixture uses the actual platform image, Kafka, Flink, MinIO, Polaris, Airflow and Trino, with random credentials, an internal network, no published ports and isolated named volumes. Build `tlcn-platform-stack-check:local` from `services/platform-app/Dockerfile` after producer changes. `STACK_PLATFORM_IMAGE`, `STACK_AIRFLOW_IMAGE` and `LOGS_FLINK_IMAGE` select cached equivalent images. The fixture starts only the components needed for logs; it does not start Debezium.

The check exercises API availability and local DLQ when Kafka starts late, automatic producer connection recovery, real catalog/product HTTP traffic and two API cart/checkout purchases, a custom shared topic, detached Flink submission, raw duplicate/invalid-message retention, production source-to-catalog builders and quality gates, canonical 16-byte Silver UUIDs and compatibility with old textual UUID bytes, resolved cart additions, the scheduler's one-task log DAG, three published marts through Trino, replay idempotence, TaskManager checkpoint recovery and retention of the last publication after an event-ID conflict. Catalog preparation invokes production batch builders in one Spark process; this check does not revalidate the 109-task OLTP scheduler DAG. JSON evidence and redacted commands are written under ignored `data/pipeline-runs/<project>/`; resources are removed after success or failure.

Run normal tests without the opt-in flag for parser/CDC/source contracts. Producer acknowledgement, bounded delivery, shutdown drain, disabled mode, startup recovery and DLQ fallback have independent tests in `services/platform-app/tests/api/core/test_kafka_producer.py`. See [setup](../docs/setup.md#log-api-thực) for operational limits, including manual DLQ recovery and the absence of a transactional outbox.


### API access-log acceptance — 09/11/2026

The isolated actual-stack check passed in 411.4 seconds: API → a custom Kafka topic → detached Flink → Landing → scheduler-run Spark log DAG → Bronze/Silver/fact → three published marts → Trino and authorized HTTP Metrics. It retained three offline requests in the local DLQ and recovered the producer connection when Kafka started. The published fact contained 21 observed requests, including two real runner checkouts; route/funnel totals matched, both cart additions resolved to products, raw replay did not duplicate valid events, and one malformed payload remained flagged in Bronze.

This acceptance exposed a PostgreSQL UUID encoding defect: casting JDBC UUID strings to Binary stored UTF-8 text instead of UUID bytes. Silver now decodes UUIDs, and log identity mapping accepts both canonical and earlier textual encodings. All 18 binary UUID columns passed the length check; four product/variant mapping checks covered both encodings. Catalog preparation passed 30/30 source reconciliation and 55/55 Gold checks.

Two retries of one logical log run retained one publication row and unchanged fact counts. TaskManager recovery advanced checkpoint 25 → 26 and captured three new requests. A conflicting valid payload sharing an event ID rejected publication; the prior funnel stayed unchanged. HTTP Metrics returned 200 with `data_source=lakehouse`, `source_run_id=logs-replay`, 21 requests and no cache fallback; anonymous access returned 401. The OLTP publication remained independent.

Local evidence: `data/pipeline-runs/tlcn-stack-0ba785099b/report.json` and `data/pipeline-runs/access-log-stack-check.log` (ignored). Private resources from this completed run were removed. The pipeline unit suite passed 18 tests with four opt-in cases skipped; platform tests passed 71 with 49 opt-in cases skipped. Full-repository Ruff, Compose validation and lock checks passed. This does not establish production latency, JobManager HA, automatic DLQ replay, cron timing or Superset readiness.

### Polars + PyIceberg migration pilot

The production DAGs still use Spark. The optional pilot only processes `categories` in an explicitly named `polars_pilot_*` namespace; it never writes the serving Bronze/Silver/Gold namespaces or publication manifests. It reuses the existing source reader and Spark-named workload credentials during migration, without reprovisioning accounts or grants.

```bash
uv sync --locked --all-packages --extra polars
PYTHONPATH=pipelines/src uv run --locked --all-packages --extra polars -- \
  python -m unittest discover -s pipelines/tests -p test_polars_pilot.py -v

docker build -f infrastructure/batch/Dockerfile -t tlcn-polars-pilot:local .
RUN_POLARS_STACK_TESTS=1 PYTHONPATH=pipelines/src \
  uv run --locked --all-packages --extra polars -- \
  python -m unittest discover -s pipelines/tests -p test_polars_stack.py -v
```

The opt-in fixture starts private PostgreSQL/MinIO/Polaris/Trino resources and one-off Python/Spark clients, then Kafka/Flink for a real API-log read. It reuses the existing cached platform, Spark and Flink images; see the image overrides in `tests/stack/fixture.py`, `tests/logs/fixture.py` and `tests/polars_checks/fixture.py`. No host ports or existing deployment volumes are used. Reports are written under ignored `data/pipeline-runs/` and private resources are cleaned afterward.

`jobs/oltp/run_polars_pilot.py` is also an explicit CLI for a configured isolated environment:

```bash
PYTHONPATH=pipelines/src uv run --locked --package batch-pipeline --extra polars -- \
  python -m jobs.oltp.run_polars_pilot \
  --namespace polars_pilot_local --run-id initial
```

This command requires the existing `POSTGRES_SOURCE_*`, source reader/database, MinIO endpoint/bucket/Spark workload keys, Polaris URI/catalog/realm and `POLARIS_CREDENTIAL_FILE` to be exported for that environment. It does not load `.env`, publish analytics or initialize production tables. Use both `--interval-start` and `--interval-end` for a bounded UTC interval. A PostgreSQL session advisory lock serializes pilot invocations sharing the namespace.

The Python image has no Spark/Java runtime and is not an always-running Compose service. Runtime dependencies are opt-in `batch-pipeline[polars]`: Polars 2.1.0, PyIceberg 0.12.0 (Arrow/native transform extras for partitioned writes) and psycopg2 2.9.10. Shared PyArrow is pinned to 18.1.0 because PyIceberg's Arrow extra requires at least 18; host consumers use the same workspace lock. Production Spark/Airflow images and their separate dependencies remain unchanged.

This pilot is not the production replacement: the extensions below cover 27-table Bronze/Silver and Gold with isolated publication; native log ingestion, Airflow/BI cutover and maintenance parity remain subsequent work. Its history scans are bounded only by the pilot table size; do not interpret this implementation as a general large-table merge strategy.

The clean compatibility run on 10/11/2026 passed in 167.890 seconds, including service startup/cleanup. It verified paged category history/current state, replay and interval boundaries, old snapshot reads, Spark positional deletes, Python partitioned writes, Trino/Spark readback, actual catalog permission denial/recovery and three real API requests captured by Flink. This is compatibility acceptance on a small private fixture, not a performance or full-pipeline claim. Evidence and remaining boundaries are in [operations](../docs/operations.md#nghiệm-thu-pilot-ngày-10112026).

#### All-source Bronze/Silver migration

The optional runtime also provides `jobs.oltp.run_polars_batch`, with all 27 configured tables by default and repeatable `--table` selection. It derives source types from PostgreSQL cursor metadata and reads existing Silver DDL contracts through a deliberately limited Arrow adapter. It does not execute Spark SQL or import PySpark. PostgreSQL JSON text is retained unchanged.

```bash
PYTHONPATH=pipelines/src uv run --locked --package batch-pipeline --extra polars -- \
  python -m jobs.oltp.run_polars_batch \
  --namespace-prefix polars_batch_local --run-id initial

RUN_POLARS_BATCH_STACK_TESTS=1 PYTHONPATH=pipelines/src \
  uv run --locked --all-packages --extra polars -- \
  python -m unittest discover -s pipelines/tests -p test_polars_batch_stack.py -v
```

Rebuild `tlcn-polars-pilot:local` before the new integration check. It reuses the private source/Trino fixture and runs actual Spark extraction/Silver as the reference. No Kafka/Flink/Airflow service is needed for this batch comparison. `STACK_USE_PRESET_SCALE=1` selects the existing full generator preset; default fixtures remain small.

All output is fenced under `<prefix>_bronze`, `<prefix>_silver` and `<prefix>_quarantine`, with staging under `landing/polars-batch/<prefix>/<run>/`. One PostgreSQL advisory lock serializes native invocations sharing the prefix. Every selected source stages successfully before Bronze/Silver data commits begin. Quarantine uses the existing business rules, salted SHA-256 PII normalization and replay identity; older source versions cannot replace newer current state.

Use both interval flags for incremental extraction. Empty intervals preserve typed staging and existing rows. Native merges retain source history and reject legacy/unclassified Bronze. Source paging is bounded, but Iceberg upserts and matching-file scans are not claimed to have bounded memory for arbitrary workloads. Source reads are not a cross-table transactional snapshot while writes are occurring. This source CLI does not build Gold or publish analytics; the separate Gold CLI below handles those steps in migration namespaces. Neither replaces maintenance or scheduled DAGs.

The clean all-source run `tlcn-stack-7e0f251bbd` passed on 10/11/2026 in 381.475 seconds including both engines and service lifecycle. All 27 tables contained data (600 initial source rows), 54 Bronze/Silver business-column fingerprints matched Spark, two API COD purchases moved current orders from 30 to 32, and all 27 fault cases matched quarantine codes/semantic JSON. Replay, empty intervals, older backfill, old snapshots and legacy-source rejection passed. See [acceptance evidence](../docs/operations.md#nghiệm-thu-27-nguồn-ngày-10112026); this is not a performance or serving cutover claim.


#### Native Gold, quality gate and publication

`jobs.oltp.run_polars_gold` builds all 27 existing Gold contracts using Polars/PyIceberg. Run it after all-source Bronze/Silver, with the same configured isolated environment:

```bash
PYTHONPATH=pipelines/src uv run --locked --package batch-pipeline --extra polars -- \
  python -m jobs.oltp.run_polars_gold \
  --namespace-prefix polars_batch_local --run-id gold_initial \
  --as-of 2026-11-10T10:00:00Z

RUN_POLARS_GOLD_STACK_TESTS=1 PYTHONPATH=pipelines/src \
  uv run --locked --all-packages --extra polars -- \
  python -m unittest discover -s pipelines/tests -p test_polars_gold_stack.py -v
```

Choose the observation timestamp for the actual data; `--as-of` must include a timezone. `--batch-date` optionally labels the audit's logical date. Processing timestamps remain actual ingestion time. `--verify-only` audits existing tables without rebuilding or publishing. Rebuild the Python image before the stack check.

One prefix lock serializes native source/Gold jobs. Gold reads pinned Silver snapshots, reads back its own Gold commits, persists all 55 checks, and publishes only on success. Schema-v3 manifests pin 54 physical Silver/Gold names under the migration prefix, including explicit null snapshot IDs for empty tables. They do not redirect existing Superset/serving readers. Failed gates retain the previous publication; rerunning a logical run updates one publication row. Inventory history retains prior day partitions, while current summaries select the observation day. Integer VND allocation uses Int128 intermediates; parity fingerprints compare DOUBLE fields at six decimal places and integer money exactly.

The stack check uses the real Spark implementation as a reference, private PostgreSQL/MinIO/Polaris/Trino, two actual API COD purchases, a deliberately corrupted mart, gate recovery and pinned old-manifest reads. It does not establish scale or production latency. The native log rollup extension below covers a separate boundary; log ingestion, hard-delete handling, maintenance, Airflow scheduling and BI serving cutover remain pending.


#### Native access-log marts

`jobs.logs.run_polars_marts` builds the three log marts from a pinned snapshot of the existing serving `gold.fact_web_events`. It selects a published product/variant catalog from either the native OLTP publication (default) or serving OLTP publication. It does not run Landing/Bronze/Silver/fact ingestion.

```bash
PYTHONPATH=pipelines/src uv run --locked --package batch-pipeline --extra polars -- \
  python -m jobs.logs.run_polars_marts \
  --namespace-prefix polars_batch_local --run-id logs_initial \
  --as-of 2026-11-11T10:00:00Z --catalog-source native

RUN_POLARS_LOGS_STACK_TESTS=1 PYTHONPATH=pipelines/src \
  uv run --locked --all-packages --extra polars -- \
  python -m unittest discover -s pipelines/tests -p test_polars_logs_stack.py -v
```

Use an appropriate event cutoff, optionally `--catalog-run-id` to select an explicit batch publication, and `--catalog-source serving` for the production catalog namespace. `--verify-only` audits without publishing. Destinations remain fenced under the migration prefix. The shared namespace lock serializes native jobs; outputs are read back from their own commit IDs. Fifteen checks must pass before an independent schema-v1 log publication pins the three output snapshots and four input snapshots. Failed checks preserve the prior publication. Audits append to `<prefix>_system.logs_quality_results`; this feature does not modify OLTP publications or existing BI readers.

Native latency quantiles use exact nearest-rank (`equiprobable`) with null durations excluded, while Spark uses `percentile_approx`; large-data equivalence is not claimed. Counts retain existing request-event semantics, UTC grouping and zero-denominator NULLs. Product identity supports canonical/legacy UUIDs and unresolved raw keys, and rejects joins that multiply events. Fact loading and aggregation are in memory; large-table resource limits are not yet established.

The opt-in check uses real API envelopes captured in the private local DLQ, a test-only handoff to actual Spark medallion transforms, and direct Spark/native mart comparisons. It does not start Kafka/Flink/Airflow or prove automatic DLQ replay. Remaining engine work includes native log ingestion through fact, hard-delete/maintenance, scheduler and serving cutover.
