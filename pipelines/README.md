# Lakehouse Pipelines

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

Including validate/publish, each DAG has 109 tasks and allows up to three active tasks. The batch date comes directly from the Airflow interval template; no context task or XCom is needed. No shared writer lock serializes independent table tasks. Stages finish in order: staging → Bronze → Silver → Gold → mart → validate/publish. Within a stage, `fact_order` waits for `fact_order_item`, `fact_inventory_current` waits for its daily snapshot, and inventory summary/store marts wait for `mart_inventory_current`.

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

Staging is shared and overwritten, so run jobs through the DAG's single-active-run boundary. Do not run standalone extraction concurrently. There is no global transaction across MySQL tables or Gold tables. Fixed intervals intentionally do not cover late commits with older timestamps; the project has opted out of lookback.

## CDC and access logs

`streaming` starts Debezium/Kafka/Flink. `mysql_cdc_to_bronze.py` writes immutable audit events to `bronze.oltp_cdc_events`, including before/after, operation and transport coordinates. It never writes Silver/Gold. The former `spark-cdc` service and transport-checkpoint materializer are removed; existing audit/publication/history tables are preserved.

`manual_lakehouse_hard_delete` (`schedule=None`) synchronizes source hard deletes from CDC. It never deletes MySQL rows. Source purge remains an explicit, separately approved operation, performed while CDC is capturing changes. Normal source operations remain soft deletes.

The DAG takes no user parameters and always processes both `cart_items` and `wishlist_items`. Triggering the DAG executes delete synchronization directly; there is no preview mode or apply flag. Hard-delete synchronization supports only removed cart and wishlist items:

| Source table | Soft-delete evidence in the CDC `before` image |
|---|---|
| `cart_items`, `wishlist_items` | `is_present = 0` and `removed_at IS NOT NULL` |

Only actual delete events (`operation='d'`) whose source deletion time `source_ts <= DAG start time - 30 days` qualify. `source_ts` comes from the CDC event’s `source.ts_ms`, not the row’s `updated_at`, the soft-delete `removed_at` or the capture/ingestion time. Events without a source deletion timestamp, tombstones, active records, recent deletions and unsupported tables are excluded. `is_active=false` alone is not a deletion marker; all other tables, including `products` and `coupons`, are excluded. Products and coupons retain their archive lifecycle.

Trigger delete synchronization:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger manual_lakehouse_hard_delete
```

Run delete synchronization while batch/init are idle; coordination between these separate DAGs is manual. The single task reads eligible CDC delete keys and deletes matching Silver rows with one Spark MERGE per supported table. It does not recheck MySQL or Silver update timestamps, maintain a deletion ledger, track processed events or split keys into chunks. Repeating the delete is safe for rows that are already absent; every run reads the eligible CDC events again. After each successful MERGE, an INFO log reports the Silver table, deleted row count and cutoff; repeated runs with no matching rows report zero.

Bronze source versions, CDC audit and historical snapshots are retained. No Kafka offsets are reset. Batch/backfill is not filtered against deleted keys and can restore rows from old staging/Bronze; run this manual DAG again when deletion must be reapplied. Existing deletion-ledger tables from earlier deployments are no longer used; this code change does not drop them. Current Gold/mart builders and dashboards do not read cart/wishlist items, so this DAG does not rebuild Gold/marts or create a new analytics publication. The next successful batch publishes the current Silver snapshots.

### Landing-first access logs

Flink appends only `landing.access_logs`, preserving every decoded Kafka payload. Invalid contracts remain raw envelopes with nullable projected fields rather than being discarded. It uses the existing consumer group, checkpoint offset commits and committed startup offsets with earliest fallback when no committed position exists. A new submission is not an automatic savepoint restore; see the [upgrade procedure](../docs/runbook/SETUP.md#landing-first-access-log-upgrade).

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
└── sql_templates/ # shared SQL resources for Spark, Flink and MySQL
```

`jobs/` retains thin executable CLIs grouped into `oltp`, `logs`, `cdc` and `maintenance`. DAGs import `gold/schema.py` without loading Spark builders. Flink jobs import the shared `streaming` modules directly; no forwarding `flink.py` or single-file `orchestration` package remains. Maintenance healthcheck lives with its DAG and no longer creates unused context/XCom tasks.

Retired parallel Flink Bronze/Silver/fact writers and their SQL resources have been removed; Spark now owns those access-log layers. The generic `ingest_bronze.py`, obsolete fact builder and format fallbacks remain removed. Current source ingestion uses `merge_source_versions`; Silver writes Iceberg only, with deduplication/region metadata inline in its writer. Sales and inventory mart builders live with their respective `*_marts.py` modules. Short operational queries remain inline. Existing large SQL/DDL resources remain under `src/lakehouse/sql_templates/`; moving these to shared repo-level SQL is outside this batch change.

Local verification scripts and Spark/Iceberg fixtures are kept outside Git. Compile/lint and Compose configuration checks remain in CI. Maintenance schedules and retention are unchanged in this change; conversion to manual maintenance is still tracked in the refactor note.
