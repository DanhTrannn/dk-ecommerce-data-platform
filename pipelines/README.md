# Lakehouse Pipelines

## OLTP batch

`lakehouse_oltp_pipeline` is the interval-only DAG, catchup disabled, one active run. Its schedule is configured in the DAG file; it is independent of the manual init DAG. Airflow supplies its own run ID and fixed `data_interval_start`/`data_interval_end` to Spark local jobs.

1. Extract source rows where `change_time >= interval_start AND change_time < interval_end`. Timestamps are normalized to UTC. Mutable tables use `updated_at`, append-only tables use `created_at`; there is no lookback or persistent cursor.
2. Each table task uses Spark JDBC without a thread pool. Read that table in primary-key order, at most 10,000 rows per query (`pk > last_pk ORDER BY pk LIMIT 10000`). Write one Parquet file per chunk at `landing/oltp/staging/<table>`; the first chunk overwrites previous staging. An empty interval writes an empty dataset with schema. Extraction must finish before Bronze starts.
3. Merge the raw delta into Iceberg Bronze one file at a time. A source version is identified by primary key and change timestamp, so replaying staging does not append duplicates and older versions remain available. Staging has no `_run_id`/`_chunk_id`; no checkpoints or completed-chunk tracking are used. Missing/unreadable staging fails.
4. Silver selects Bronze versions matching the keys/change timestamps in each staging file, validates, deduplicates by PK/change time, pseudonymizes configured PII and applies Iceberg `MERGE` by primary key. Older backfills cannot overwrite newer source versions.
5. Build Gold dimensions, facts and 14 marts from Silver. Validate Gold and publish its snapshot IDs for the existing dashboard reader. A failed publication leaves the last completed publication available. Airflow passes the fixed DAG start time as `--as-of` so inventory observation dates and time-based mart measures remain stable during retries.

There are no Landing manifests, MD5 scans, high-watermark tasks, cursor files, completion markers or live-source reconciliation gate in the scheduled path. `run_reconciliation_gate.py` remains an explicit diagnostic for a quiescent source.

The 27 source tables, primary keys, change-time columns and Silver/PII mappings are declared once in `SOURCE_TABLES` in `src/lakehouse/config.py`. Extraction uses those fixed names directly; YAML keeps Landing paths only.

Both OLTP DAGs contain five TaskGroups, with one task per target table:

| TaskGroup | Tables | Task example |
|---|---:|---|
| `staging` | 27 source tables | `staging.orders` |
| `bronze` | 27 raw source tables | `bronze.orders` |
| `silver` | 27 transformed tables | `silver.silver_orders` |
| `gold` | 6 dimensions + 6 facts | `gold.fact_order` |
| `mart` | 14 marts | `mart.mart_sales_daily` |

Including validate/publish, each DAG has 108 tasks and allows up to three active tasks. The batch date comes directly from the Airflow interval template; no context task or XCom is needed. No shared writer lock serializes independent table tasks. Stages finish in order: staging → Bronze → Silver → Gold → mart → validate/publish. Within a stage, `fact_order` waits for `fact_order_item`, `fact_inventory_current` waits for its daily snapshot, and inventory summary/store marts wait for `mart_inventory_current`.

Each Spark CLI accepts `--table` to process one configured table and prepare only its target schema. Without `--table`, the existing all-table CLI behavior remains available. Retry/clear a table task to reread its input from the beginning; extract task retries fetch only that source table again. Silver reads Bronze data matching that table's staging keys. Final quality/publication waits for every mart. TaskGroup `mart` writes the existing `lakehouse.gold.mart_*` tables.


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

Select source tables explicitly when triggering this DAG. Its default is preview only (`apply=false`); no Silver, Gold, ledger or publication is written. The current schema supports:

| Source table | Soft-delete evidence in the CDC `before` image |
|---|---|
| `cart_items`, `wishlist_items` | `is_present = 0` and `removed_at IS NOT NULL` |
| `products`, `coupons` | `archived_at IS NOT NULL` |

Only actual delete events (`operation='d'`) whose source `updated_at <= DAG start time - 30 days` qualify. Tombstones, active records, recent updates and unsupported tables are excluded. Debezium microsecond timestamps and ISO timestamp strings are accepted. `is_active=false` alone is not a deletion marker; `carts` and `categories` are not eligible in the current schema.

Preview an explicitly selected table:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger manual_lakehouse_hard_delete \
  --conf '{"tables":["cart_items"],"apply":false}'
```

After reviewing the table/count output, the operator can trigger the same DAG with `apply=true`. Run delete synchronization while batch/init are idle; coordination between these separate DAGs is manual. The job checks that keys are already absent from MySQL, rejects newer Silver rows and refuses products/coupons still referenced by current data or historical transactions. Each page contains at most 10,000 keys. A failure stops the job; an apply run may have completed earlier pages, which remain safe to replay.

The sync task records approved keys in `lakehouse.system.oltp_hard_deletes`, deletes only matching Silver rows, then the DAG rebuilds Gold/marts in per-table TaskGroups, runs the quality gate and publishes. Bronze source versions, CDC audit, historical inventory dates and previously published snapshots are retained. This table records deletion decisions, not an extraction checkpoint; Silver batch/backfill excludes its keys so old Bronze or staging cannot resurrect a hard-deleted record. No Kafka offsets are reset. The `apply_confirmed` gate skips Gold/marts/publication in preview mode. Apply runs continue to Gold even when all deletes already completed; failed downstream tasks can be retried without repeating source checks. Initial load and ordinary batch reads remain independent from CDC updates.

Access-log processing is unchanged: Flink still writes its four sinks in parallel. `lakehouse_logs_marts` runs hourly, independent of maintenance, validates three marts and publishes their snapshots. The planned Landing-first realtime design is a separate change.

## Runtime and SQL

Spark runs with `SPARK_MASTER_URL=local[2]` inside the Airflow task container; ad hoc SQL uses `spark-client`. `core` starts the web source, `batch` starts Airflow and analytical infrastructure, `streaming` adds audit/access capture, and `tools` adds management interfaces.

OLTP extraction lives in `src/lakehouse/oltp/extract.py`; staging/Bronze/Silver coordination in `ingestion.py`.

```text
src/lakehouse/
├── config.py, spark.py, sql.py
├── oltp/
│   ├── extract.py, bronze.py, ingestion.py, hard_delete.py, publication.py
│   ├── silver/    # schema, writer, validation, PII, quarantine
│   ├── gold/      # schema, per-table I/O, builders by domain and *_marts.py
│   └── quality/   # gate, inventory/operations/marketing checks, manual reconciliation
├── logs/          # parser, Flink adapter, Spark mart rollup and publication
├── streaming/     # shared Flink runtime, catalog and sink setup
└── maintenance.py
```

`jobs/` retains thin executable CLIs grouped into `oltp`, `logs`, `cdc` and `maintenance`. DAGs import `gold/schema.py` without loading Spark builders. Flink jobs import the shared `streaming` modules directly; no forwarding `flink.py` or single-file `orchestration` package remains. Maintenance healthcheck lives with its DAG and no longer creates unused context/XCom tasks.

The unused Spark log Bronze/Silver path, generic `ingest_bronze.py`, obsolete fact builder and format fallbacks have been removed. Current source ingestion uses `merge_source_versions`; Silver writes Iceberg only, with deduplication/region metadata inline in its writer. Sales and inventory mart builders live with their respective `*_marts.py` modules. Short operational queries remain inline. Existing large SQL/DDL resources remain under `src/lakehouse/sql_templates/`; moving these to shared repo-level SQL is outside this batch change.

Local verification scripts and Spark/Iceberg fixtures are kept outside Git. Compile/lint and Compose configuration checks remain in CI. Maintenance schedules and retention are unchanged in this change; conversion to manual maintenance is still tracked in the refactor note.
