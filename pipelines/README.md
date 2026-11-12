# Lakehouse Pipelines

## PostgreSQL batch source — 07/11/2026

Batch now reads the 27 analytical source tables from PostgreSQL `public` through `org.postgresql.Driver`. The existing Spark/Airflow Dockerfiles already pin and install pgJDBC; rebuild older images that still contain only the MySQL connector. Native UUIDs, booleans and UTC timestamps pass through JDBC into staging; Silver keeps its existing explicit analytical types and PII rules. Credentials are JDBC properties, not part of the JDBC URL.

Before running jobs:

1. Bootstrap the PostgreSQL business schema and load data using the [database guide](../database/README.md).
2. Set `POSTGRES_ECOMMERCE_READER_USER` and a nonempty `POSTGRES_ECOMMERCE_READER_PASSWORD` in `.env`. Recreate the PostgreSQL service to pick up these environment variables and the script mount, then run `make source-reader` explicitly.
3. Configure a stable nonempty `SILVER_PSEUDONYMIZE_SALT`, MinIO Spark credentials/bucket and Polaris catalog/credential files. Run `make provision-lakehouse` after storage/catalog services are ready; startup alone does not configure them. See the [explicit provisioning guide](../docs/setup.md#2-build-và-chọn-profile).
4. Rebuild/recreate Spark and Airflow services before using the init or interval DAGs below. Compose supplies a local `spark_default` connection from `SPARK_MASTER_URL` so SparkSubmit does not fall back to YARN, and injects `postgres:5432`; host-side invocations use `POSTGRES_SOURCE_HOST`/`POSTGRES_SOURCE_PORT` and must export the configured environment (pipeline helpers do not load `.env` themselves).

The reader script runs transactionally, requires all 27 source tables, refuses privileged/owner/member roles, grants SELECT on an explicit whitelist and excludes `customer_credentials`. It revokes that dedicated role's direct table/sequence permissions before regranting SELECT; it does not change PUBLIC permissions or other accounts. Future tables are not automatically granted. Re-running applies the configured password again. Do not point the reader settings at an application or administrative role. Database-level read-only defaults and JDBC read-only settings supplement the actual privilege restrictions.

PostgreSQL source SQL has its own double-quoted identifiers in `sql_templates/postgres/`; Spark SQL retains backticks. Interval bounds are explicit UTC `TIMESTAMPTZ` values and remain half-open. Full init still has no timestamp predicate. Lineage is `_source_system=postgresql_ecommerce`, `_source_schema=public`; this is not a migration of older MySQL analytical history. Bronze rejects existing non-PostgreSQL/unclassified source rows before merging configured targets. Provision a separate catalog/warehouse for the PostgreSQL source or plan an explicit migration; do not reset old volumes or reuse historical PKs implicitly.

### Isolated checks

Tests under `pipelines/tests/` are kept local only: every `tests/` directory is gitignored except the dbt data tests in `pipelines/dbt/tests/`, which `dbt build` runs as quality gates.

```bash
# Pure source SQL/config tests; no services or credentials
PYTHONPATH=pipelines/src uv run --locked --all-packages -- \
  python -m unittest discover -s pipelines/tests -p test_postgres.py -v
```

The former Spark batch integration test (`test_postgres_batch.py`) has been removed.

### Full-stack acceptance (opt-in)

The separate stack check uses the repository's Compose services and real Airflow scheduler/LocalExecutor, PostgreSQL, MinIO, Polaris and Trino. It creates an internal network, random credentials, temporary data and project-prefixed volumes, publishes no host ports, and removes only its own resources. It never uses the existing source database or Lakehouse volumes.

Build the current application and Airflow runtimes, and cache the infrastructure images declared in Compose before starting (the fixture uses `--pull never`):

```bash
docker build -f services/platform-app/Dockerfile -t tlcn-platform-stack-check:local .
docker build -f infrastructure/airflow/Dockerfile -t tlcn-airflow-stack-check:local .
RUN_LAKEHOUSE_STACK_TESTS=1 PYTHONPATH=pipelines/src \
  uv run --locked --all-packages -- \
  python -m unittest discover -s pipelines/tests -p test_stack.py -v
```

**Currently not runnable:** the stack fixture (`pipelines/tests/stack/runtime.py`) seeds its source with the removed `data-generator` package and `generator/configs/small.yml`; it needs the new OLTP generator. The fixture reduced the preset to 30 orders by default; `STACK_USE_PRESET_SCALE=1` used the full former `small.yml` scale (500 customers, 60 products, 240 variants and 3,000 orders). This flag affects only the isolated test source, not production settings. The full-preset mode allows up to 60 minutes per DAG; the default reduced fixture retains its 25-minute limit. A failed/retrying task still fails the check immediately. The local report records generator version, configured scale, logical dataset identity and SQL SHA-256 alongside the import receipt.

`STACK_PLATFORM_IMAGE` and `STACK_AIRFLOW_IMAGE` override these test image tags. The test runs both 109-task OLTP DAGs: initial load, then a bounded interval containing two API-runner COD purchases. It compares publication-pinned fact/mart results through Trino, checks source-to-Silver parity, reader write denials, writer permission failure, order replay, quality rejection and same-run recovery. Reports/logs with known credentials redacted are kept under ignored `data/pipeline-runs/tlcn-stack-*/`. This is a small synthetic acceptance fixture, not a load test.

The Trino catalog explicitly sets `iceberg.security=READ_ONLY`, rejecting data/metadata writes at SQL authorization before reaching MinIO. This complements the scoped MinIO policy and Polaris reader grants; see [Trino 483 Iceberg authorization](https://github.com/trinodb/trino/blob/483/docs/src/main/sphinx/connector/iceberg.md#authorization).

Automatic cron creation is disabled only in the fixture; DAG runs are triggered through the Airflow API and executed by the scheduler. The fixture limits scheduler/Trino/Polaris memory and reduces Spark shuffle partitions. It does not validate cron timing, CDC, access-log streaming, Superset, public deployment or production-scale performance. Normal unit discovery skips this test unless explicitly enabled.

Reference behavior: [Spark 3.5.9 JDBC options](https://spark.apache.org/docs/3.5.9/sql-data-sources-jdbc.html), [pgJDBC connection properties](https://jdbc.postgresql.org/documentation/use/).

## OLTP batch

`lakehouse_oltp_pipeline` is the interval-only DAG, catchup disabled, one active run. Its schedule is configured in the DAG file; it is independent of the manual init DAG. Airflow supplies its own run ID and fixed `data_interval_start`/`data_interval_end` to Spark local jobs.

1. Extract source rows where `change_time >= interval_start AND change_time < interval_end`. Timestamps are normalized to UTC. Mutable tables use `updated_at`, append-only tables use `created_at`; there is no lookback or persistent cursor.
2. Each table task uses Spark JDBC without a thread pool. Read that table in primary-key order, at most 10,000 rows per query (`pk > last_pk ORDER BY pk LIMIT 10000`). Write one Parquet file per chunk at `landing/oltp/staging/<table>`; the first chunk overwrites previous staging. An empty interval writes an empty dataset with schema. Extraction must finish before Bronze starts.
3. Merge the raw delta into Iceberg Bronze one file at a time. A source version is identified by primary key and change timestamp, so replaying staging does not append duplicates and older versions remain available. Staging has no `_run_id`/`_chunk_id`; no checkpoints or completed-chunk tracking are used. Missing/unreadable staging fails.
4. Silver selects Bronze versions matching the keys/change timestamps in each staging file, validates, deduplicates by PK/change time, pseudonymizes configured PII and applies Iceberg `MERGE` by primary key. Older backfills cannot overwrite newer source versions.
5. Build Gold dimensions, facts and 15 marts from Silver. Validate Gold and publish its snapshot IDs for the existing dashboard reader. A failed publication leaves the last completed publication available. Airflow passes the fixed DAG start time as `--as-of` so inventory observation dates and time-based mart measures remain stable during retries.

There are no Landing manifests, MD5 scans, high-watermark tasks, cursor files, completion markers or live-source reconciliation gate in the scheduled path. `run_reconciliation_gate.py` remains an explicit diagnostic for a quiescent source.

The 27 source tables, primary keys, change-time columns and Silver/PII mappings are declared once in `SOURCE_TABLES` in `src/lakehouse/utils/config.py`. Extraction uses those fixed names directly; YAML keeps Landing paths only.

Both OLTP DAGs contain five TaskGroups, with one task per target table:

| TaskGroup | Tables | Task example |
|---|---:|---|
| `staging` | 27 source tables | `staging.orders` |
| `bronze` | 27 raw source tables | `bronze.orders` |
| `silver` | 27 transformed tables | `silver.silver_orders` |
| `gold` | 6 dimensions + 6 facts | `gold.fact_order` |
| `mart` | 15 marts | `mart.mart_sales_daily` |

Including validate/publish, each DAG has 109 tasks and allows up to three active tasks. The interval DAG uses its interval start for the batch date; the initial-load DAG uses its fixed run start because Airflow 3 manual runs may have no logical date or interval. Both convert to the DAG timezone (Asia/Ho_Chi_Minh); no context task or XCom is needed. No shared writer lock serializes independent table tasks. Stages finish in order: staging → Bronze → Silver → Gold → mart → validate/publish. Within a stage, `fact_order` waits for `fact_order_item`, `fact_inventory_current` waits for its daily snapshot, and inventory summary/store marts wait for `mart_inventory_current`.

Each Spark CLI accepts `--table` to process one configured table and prepare only its target schema. Without `--table`, the existing all-table CLI behavior remains available. Retry/clear a table task to reread its input from the beginning; extract task retries fetch only that source table again. Silver reads Bronze data matching that table's staging keys. Final quality/publication waits for every mart. Admin overview is a singleton mart built from independent catalog, inventory, customer, review, coupon and order aggregates; its counts/windows use the same fixed DAG as_of. System reconciliation reuses the additive order mart. TaskGroup `mart` writes the existing `lakehouse.gold.mart_*` tables.


## Initial load and recovery

Start `core,batch` and load source data. Initial full loading uses a separate manual DAG, `init_lakehouse_oltp` (`schedule=None`). Its extraction is `SELECT *` without any interval or timestamp filter:

```bash
docker compose exec -T airflow-scheduler airflow dags pause lakehouse_oltp_pipeline
docker compose exec -T airflow-scheduler airflow dags unpause init_lakehouse_oltp
docker compose exec -T airflow-scheduler airflow dags trigger init_lakehouse_oltp
```

Wait for init to finish before running the interval DAG. Both DAGs share staging/Silver/Gold; there is no cross-DAG concurrency check. Init upserts existing analytical data and retains Bronze/history; it is not a database reset.

There is no `full_extract` option on the interval DAG or extraction CLI. The init DAG uses `jobs/oltp/init_oltp.py`, then the existing Bronze, Silver, Gold and validate/publish jobs.

Ordinary scheduled or manual runs use their Airflow interval. An extract retry starts again at the first source key and replaces staging. Downstream task retries read their existing inputs from the beginning; they do not fetch source again or resume from a checkpoint. Business rows are merged without duplicates; quarantine records and same-run publication retries are also deduplicated. Airflow run IDs remain for quality/publication audit only. If historical intervals were missed while the local stack was off, run those intervals deliberately or run the init DAG again; catchup is not automatic. Init does not propagate source hard deletes.

Staging is shared and overwritten, so run jobs through the DAG's single-active-run boundary. Do not run standalone extraction concurrently. There is no global transaction across PostgreSQL tables or Gold tables. Fixed intervals intentionally do not cover late commits with older timestamps; the project has opted out of lookback.

## CDC and access logs

`streaming` starts Debezium/Kafka/Flink. Run `make source-cdc` after schema bootstrap to configure the dedicated source role, 27-table publication and FULL replica identity; Debezium uses the existing publication with auto-creation disabled. `postgres_cdc_to_bronze.py` writes immutable audit events to `bronze.oltp_cdc_events`, including before/after, PostgreSQL LSN/transaction metadata and transport coordinates. Existing MySQL audit fields/history are preserved with additive columns. See [CDC operations](../docs/operations.md#cdcflinkkafka) for startup, migration and recovery. It never writes Silver/Gold. The former `spark-cdc` service and transport-checkpoint materializer are removed; existing audit/publication/history tables are preserved.

`manual_lakehouse_hard_delete` (`schedule=None`) synchronizes source hard deletes from CDC. It never deletes PostgreSQL source rows. Source purge remains an explicit, separately approved operation, performed while CDC is capturing changes. Normal source operations remain soft deletes.

The DAG takes no user parameters and always processes both `cart_items` and `wishlist_items`. Triggering the DAG executes delete synchronization directly; there is no preview mode or apply flag. Hard-delete synchronization supports only removed cart and wishlist items:

| Source table | Soft-delete evidence in the CDC `before` image |
|---|---|
| `cart_items`, `wishlist_items` | `is_present = false` (or legacy `0`) and `removed_at IS NOT NULL` |

Only actual delete events (`operation='d'`) whose source deletion time `source_ts <= DAG start time - 30 days` qualify. `source_ts` comes from the CDC event’s `source.ts_ms`, not the row’s `updated_at`, the soft-delete `removed_at` or the capture/ingestion time. Events without a source deletion timestamp, tombstones, active records, recent deletions and unsupported tables are excluded. `is_active=false` alone is not a deletion marker; all other tables, including `products` and `coupons`, are excluded. Products and coupons retain their archive lifecycle.

Trigger delete synchronization:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger manual_lakehouse_hard_delete
```

Run delete synchronization while batch/init are idle; coordination between these separate DAGs is manual. The single task reads eligible CDC delete keys and deletes matching Silver rows with one Spark MERGE per supported table. It does not recheck PostgreSQL or Silver update timestamps, maintain a deletion ledger, track processed events or split keys into chunks. Repeating the delete is safe for rows that are already absent; every run reads the eligible CDC events again. After each successful MERGE, an INFO log reports the Silver table, deleted row count and cutoff; repeated runs with no matching rows report zero.

Bronze source versions, CDC audit and historical snapshots are retained. No Kafka offsets are reset. Batch/backfill is not filtered against deleted keys and can restore rows from old staging/Bronze; run this manual DAG again when deletion must be reapplied. Existing deletion-ledger tables from earlier deployments are no longer used; this code change does not drop them. Current Gold/mart builders and dashboards do not read cart/wishlist items, so this DAG does not rebuild Gold/marts or create a new analytics publication. The next successful batch publishes the current Silver snapshots.

### Landing-first access logs

Flink appends only `landing.access_logs`, preserving every decoded Kafka payload. Invalid contracts remain raw envelopes with nullable projected fields rather than being discarded. It uses the existing consumer group, checkpoint offset commits and committed startup offsets with earliest fallback when no committed position exists. A new submission is not an automatic savepoint restore; see the [upgrade procedure](../docs/operations.md#cdcflinkkafka).

The independent hourly `lakehouse_logs_marts` DAG uses one Spark task for the sequential path **Landing → Bronze → Silver → Gold fact → three marts → validation/publication**. Its existing task ID and CLI remain compatible. Airflow passes its fixed run start as `--as-of`, reused on retry; the CLI defaults to invocation time when run without it. Do not overlap standalone log writers with this DAG (`max_active_runs=1`).

1. Pin the Landing snapshot and select landed payloads with `ingest_ts <= as_of`. Group duplicate payloads and insert raw hashes absent from Bronze; no event-time cursor drops late arrivals. This discovers missing keys by scanning stored inputs, not an incremental file checkpoint. It suits the local dataset, not a measured large-scale optimization.
2. Bronze adds `raw_payload`, `_raw_hash` (SHA-256) and `_validation_error`. The existing parser supplies typed projections. Invalid payloads remain with a generic error marker; they are excluded from Silver, not silently counted as valid events. Existing typed Bronze rows remain unchanged, with null added fields; the first upgrade adds canonical raw rows alongside them. Bronze row count is not a unique event count.
3. Read persisted Bronze, deduplicate immutable `event_id`, and insert missing eligible events into Silver. Conflicting valid raw payloads sharing an ID fail the job for inspection rather than selecting arbitrarily. Future event timestamps stay in Bronze until a later observation; late historical events remain eligible.
4. Read persisted Silver independently and insert missing events into `fact_web_events`. Each layer catches up on retry even if the previous attempt failed after committing its predecessor. Existing Silver/fact rows, including their historical timestamps/lineage, are not rewritten.
5. Build all three marts from one pinned fact snapshot and the published OLTP catalog. Validate complete outputs, then publish them together using log publication schema 1. Source snapshot metadata now includes Landing/Bronze/Silver. Dashboards continue to read the previous completed publication if processing fails; this is not a transaction across every layer/table.

The hourly pipeline reads the Iceberg Landing table, not the separately uploaded JSONL object prefix. No bucket, Kafka offset, old checkpoint, source transaction or Iceberg history is deleted by this change. Realtime Landing API/dashboard refresh every 10 seconds is a later phase; existing analytical views still read hourly marts.

## Runtime and SQL

Spark runs with `SPARK_MASTER_URL=local[2]` inside the Airflow task container; ad hoc SQL uses `spark-client`. `core` starts the web source, `batch` starts Airflow and analytical infrastructure, `streaming` adds audit/access capture, and `tools` adds management interfaces.

OLTP extraction lives in `src/lakehouse/oltp/extract.py`; staging/Bronze/Silver coordination in `ingestion.py`.

```text
src/lakehouse/
├── utils/         # shared config, Spark session, SQL loader and maintenance
├── oltp/
│   ├── extract.py, ingestion.py, hard_delete.py, publication.py
│   ├── bronze/    # writer.py: source version MERGE and schema evolution
│   ├── silver/    # schema, writer, validation, PII, quarantine
│   ├── gold/      # schema, per-table I/O, builders by domain and *_marts.py
│   └── quality/   # gate, inventory/operations/marketing checks, manual reconciliation
├── logs/          # raw Landing adapter, parser, medallion builders/ingestion, marts/publication
├── streaming/     # shared Flink runtime, catalog and sink setup
└── sql_templates/ # shared SQL resources for Spark, Flink and PostgreSQL
```

`jobs/` retains thin executable CLIs grouped into `oltp`, `logs`, `cdc` and `maintenance`. DAGs import `gold/schema.py` without loading Spark builders. Flink jobs import the shared `streaming` modules directly; no forwarding `flink.py` or single-file `orchestration` package remains. Maintenance healthcheck lives with its DAG and no longer creates unused context/XCom tasks.

Retired parallel Flink Bronze/Silver/fact writers and their SQL resources have been removed; Spark now owns those access-log layers. The generic `ingest_bronze.py`, obsolete fact builder and format fallbacks remain removed. Current source ingestion uses `merge_source_versions`; Silver writes Iceberg only, with deduplication/region metadata inline in its writer. Sales and inventory mart builders live with their respective `*_marts.py` modules. Short operational queries remain inline. Existing large SQL/DDL resources remain under `src/lakehouse/sql_templates/`; moving these to shared repo-level SQL is outside this batch change.

The PostgreSQL batch and full-stack acceptance fixtures are versioned under `pipelines/tests`; their generated datasets, credentials and reports stay outside Git. Compile/lint and Compose configuration checks remain in CI. Maintenance schedules and retention are unchanged in this change; conversion to manual maintenance is still tracked in the refactor note.

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

## Polars + PyIceberg migration pilot

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

### All-source Bronze/Silver migration

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


### Native Gold, quality gate and publication

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


### Native access-log marts

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
