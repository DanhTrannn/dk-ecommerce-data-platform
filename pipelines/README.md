# Lakehouse Pipelines

Batch and log processing for the Lakehouse. Engines: **Polars 2.1 + PyIceberg 0.12** for ingestion, pre-merge checks and maintenance; **dbt 1.11 (dbt-trino)** for Gold on Trino; **Flink 1.19** only appends API access logs to Landing.

## Setup

1. Bootstrap the PostgreSQL source schema and run `make source-reader` (SELECT-only role on the 27 analytical tables; `customer_credentials` excluded). The batch loads 19 of them and reads `inventory` and `store_inventory` only for the stock reconciliation check.
2. Configure the MinIO batch workload keys (`MINIO_BATCH_*`), the bucket and MinIO policies/users through Streamlit. Create the Polaris principal/roles, then use **Cấp cho workload** to assign credentials to `batch-airflow` and the Trino consumers; the exporter writes their credential volumes. MinIO key/secret values must match the workload environment; restart consumers after changing credentials. See [setup](../docs/setup.md#2-build-và-chọn-profile).
3. Configure `DBT_TRINO_PASSWORD` and the Trino CA/credential volumes as described in setup. dbt connects over TLS on port 8443 as `dbt_writer` through `lakehouse_etl`; Gold writes are restricted to `gold` and isolated `polars_batch_*_gold` schemas. The separate `analytics_reader` reads the `lakehouse` catalog, whose connector is read-only. Trino is therefore both a restricted Gold writer and an analytical reader.
4. Build the Airflow image (`infrastructure/airflow/Dockerfile`). It carries the pipeline runtime as a separate venv at `/opt/pipeline-venv`, exposed as `PIPELINE_PYTHON`; run jobs by hand with `docker compose exec airflow-scheduler`.

## DAGs

| DAG | Schedule | Tasks |
|---|---|---|
| `lakehouse_oltp` | `0 3 * * *`, one active run | groups `bronze` → `checks` → `silver` → `gold` → `maintenance`, one task per table / dbt node |
| `lakehouse_logs_marts` | hourly | `rollup_logs_gold_marts` |
| `lakehouse_streaming_maintenance` | `0 */2 * * *` | Flink health check → `compact` → `expire` → `orphan` report for log tables |

Schedules use `Asia/Ho_Chi_Minh`, with `catchup=False`. OLTP tasks retry twice; log and maintenance tasks retry once, all after two minutes.

## OLTP batch

`lakehouse_oltp` takes two params: `mode` (`incremental` by default, or `full`) and `namespace_prefix` (`serving` by default). Scheduled runs are incremental over the run's data interval `[start, end)`; a full load is a manual trigger:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp --conf '{"mode": "full"}'
```

Each group holds one task per source table (19), so a failure shows the table and a retry (or **Clear** in the UI) reruns only that table. Every task is a short CLI call with the same run arguments: `--namespace-prefix`, `--run-id` (Airflow's, reused on retries), `--mode`, `--run-started-at` (the DAG run's start) and, in incremental mode only, `--interval-start`/`--interval-end`. Tasks share the Airflow pool `lakehouse_oltp` (4 slots, created by `airflow-init`); a full run on the 1m dataset peaked at about 4.8 GiB in the scheduler container.

1. **`bronze.<table>`** (`jobs.oltp.bronze --table`). Reads one source table in primary-key order (at most 10,000 rows per page, configured columns only) inside its own repeatable-read transaction and appends to `bronze.<table>` in batches of 200,000 rows with `_ingested_at` and `_run_id`, partitioned by day of `_ingested_at`. Incremental runs select the cursor (`updated_at` for mutable tables, `created_at` for append-only ones) in `[start, end)`, with no lookback. Full runs select every row **created before the run start**, so all 19 tables of one run stop at the same moment although each is read at a different time; rows created while the run is going wait for the next run. A retry with the same run ID first deletes that table's Bronze rows of the run.
2. **`checks.<table>`** (`jobs.oltp.check --table`). Row rules on the run's Bronze rows (required fields, allowed statuses, non-negative amounts, order and line arithmetic). Full runs also check duplicate primary keys and compare with the source up to the same cutoff: row count, money totals of `orders`, `payments` and `refunds`, and, for `inventory_transactions`, ledger stock against `on_hand − opening_on_hand` minus the movements recorded after the cutoff (one source query). **`checks.foreign_keys`** (`--foreign-keys`) runs after the 19 table checks and only in full runs: an order created inside an incremental interval but updated after it is read by the next run, so its lines can briefly point at a missing parent. A failure raises `CheckFailed` naming the table, the check and the row count; no Silver task starts.
3. **`silver.<table>`** (`jobs.oltp.silver --table`). `silver.<table>` keeps the current row per primary key. A full run replaces the table in one commit (rows deleted in the source disappear); an incremental run upserts in batches and never overwrites a row with an older cursor.
4. **`gold.<node>`**. One task per dbt model in `pipelines/dbt/models/oltp` and per seed (`delivery_sla`, `calendar_events`): 7 dimensions, 6 facts, 14 marts, rebuilt from current Silver with `CREATE OR REPLACE`. Task order follows each model's `ref()` calls, read from the SQL when the DAG is parsed. The task command is plain dbt, e.g. `dbt build --select dim_city --exclude path:tests/oltp` (seeds use `dbt seed`). Its context comes from the task environment: `DBT_GOLD_SCHEMA`, `DBT_SILVER_SCHEMA`, `DBT_AS_OF` (the run start, Gold's current day: it ends the dense daily series and the 28-day windows), plus project, profile, target and log paths under `/tmp/dbt/<run_id>/<node>`. The first log line prints the three context variables; to repeat a task by hand, export them and run the same command in the scheduler container (the other `DBT_*` paths are visible in the task's **Rendered Template**). Generic key tests run with their model; **`gold.tests`** runs the cross-model tests in `pipelines/dbt/tests/oltp` last. dbt data tests run with severity `warn`: they log, they do not block.
5. **`maintenance`**. `compact` → `expire` → `orphan` report on the OLTP tables (`jobs.maintenance.iceberg_table_maintenance --profile oltp`), after Gold.

Batch tasks take no lock: the DAG's `max_active_runs=1` and its task order keep writers apart. OLTP maintenance holds the advisory lock `<prefix>:oltp`, so it does not collide with the log and streaming jobs, which hold `<prefix>`.

There is no run state, quarantine, quality-result table or publication for OLTP. Readers (Superset, the API) read the current Gold tables. Source tables, keys, cursors, columns and foreign keys are declared once in `SOURCE_TABLES` and `FOREIGN_KEYS` (`src/lakehouse/oltp/sources.py`).

Gold rules worth knowing:

- Revenue is recognized on the later of `completed_at` and `paid_at`. Refunds of recognized orders reduce revenue on the refund day; refunds of orders never recognized (paid, then cancelled) are not revenue.
- Gross profit = net revenue − cost of goods. Goods a refunded return puts back on the shelf (accepted at inspection) take their sold cost back out on the refund day, so a fully returned order nets to zero profit.
- A shipment is on time when its local delivery date is at most `sla_days` after the local order date. `sla_days` per city (seed `delivery_sla`) is the upper bound of the GHTK Express range of the city's zone plus one day of shop handling and one day of waiting for pickup: TP.HCM 4, Bình Dương, Cần Thơ and Hà Nội 6, Đà Nẵng and Hải Phòng 8.

`namespace_prefix=serving` resolves to the plain `bronze`, `silver`, `gold` and `system` namespaces. Only `serving` and isolated prefixes matching `polars_batch_[a-z0-9_]{1,40}` are accepted, e.g. `--conf '{"mode": "full", "namespace_prefix": "polars_batch_try"}'` for a trial run.

## Access logs

Flink (`kafka_to_lakehouse_pure_streaming`, submitted with `flink-submit`) appends every decoded Kafka payload to `landing.access_logs`, including invalid contracts. `jobs.logs.build_logs_gold` then pins Landing, inserts missing raw hashes into Bronze, deduplicates valid events into Silver and `fact_web_events`, runs the dbt log models (`pipelines/dbt/models/logs`) for the three marts against the current Silver product catalog, audits them and publishes to `system.logs_analytics_publications`. Conflicting valid payloads sharing an event ID fail the run instead of being chosen arbitrarily. Landing rows are selected by ingestion time up to `--as-of`, while typed events/facts are bounded by event time. Invalid events remain in Bronze with a validation error. The log CLI defaults to `serving` and requires `--run-id`; `--as-of` defaults to the current UTC time when omitted, so provide it explicitly for controlled retries.

Runtime API capture emits `data_origin=observed`. The separate `datagen logs` implementation writes synthetic daily Parquet (`raw_payload`, `event_at`); no checked-in loader sends those files to Kafka or Iceberg Landing. Generating files alone therefore does not populate these log tables. Generated payloads carry `session.id` and, for applicable actions, `ecommerce.order_number`; the current pipeline parser and typed Bronze/Silver/fact projections do not expose either field. Raw payloads retain them, but the existing marketing mart measures action/actor counts rather than a session-level, order-linked conversion funnel. See [generator](../generator/README.md).

The submitted Flink job explicitly configures 10-second exactly-once checkpoints, a five-second minimum pause and a 60-second timeout at `file:///opt/flink/checkpoints`, overriding the 30-second interval in `flink-conf.yaml`. These are code settings, not evidence of delivery or recovery performance.

Maintenance uses explicit table whitelists. Snapshot expiry protects references in retained log publications and Iceberg refs. Scheduled orphan actions report candidates only; physical deletion additionally requires `--delete-orphans --writers-stopped`.

## Layout

```text
src/lakehouse/
├── analytics/     # dbt runner
├── oltp/          # sources, Bronze, pre-merge checks, Silver merge, DAG plan
├── logs/          # Landing adapter, parser, Polars layers, marts and publication
├── streaming/     # Flink access-log runtime and sink setup
├── utils/         # contracts, run arguments, Iceberg/Polaris catalog, PostgreSQL, SQL loader, maintenance
└── sql_templates/ # contracts (Silver/Gold/log schemas), flink
src/jobs/          # CLIs: oltp, logs, maintenance
dbt/               # dbt project: models/oltp, models/logs, seeds, data tests
```

## Tests

Tests under `pipelines/tests/` are kept local only: every `tests/` directory is gitignored except the dbt data tests in `pipelines/dbt/tests/`.

```bash
PYTHONPATH=pipelines/src:pipelines/tests uv run --locked --all-packages -- \
  python -m unittest discover -s pipelines/tests -v
```

The local suite covers Bronze batching and retries, the pre-merge checks, the Silver merge and the per-table CLIs against a SQLite-backed PyIceberg catalog (`pipelines/tests/iceberg_fixture.py`), plus the log pipeline, the OLTP DAG commands and Gold graph, and PostgreSQL source SQL. It does not run Trino, dbt or Kafka/Flink. dbt artifacts of OLTP Gold tasks stay in `/tmp/dbt/<run_id>/<node>` of the container.
