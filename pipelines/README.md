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
| `init_lakehouse_oltp` | manual | `run_batch_pipeline --mode full` |
| `lakehouse_oltp_pipeline` | `0 3 * * *`, one active run | `run_batch_pipeline --mode incremental` (interval `[start, end)`) |
| `lakehouse_logs_marts` | hourly | `rollup_logs_gold_marts` |
| `lakehouse_oltp_maintenance` | `0 4 * * *` | `compact` → `expire` → `orphan` report |
| `lakehouse_streaming_maintenance` | `0 */2 * * *` | Flink health check → `compact` → `expire` → `orphan` report for log tables |

Schedules use `Asia/Ho_Chi_Minh`, with `catchup=False`. OLTP DAG tasks retry twice; log and maintenance tasks retry once, all after two minutes. A PostgreSQL advisory lock per namespace serializes batch, log and maintenance jobs; a job that finds the lock taken fails instead of waiting, so do not overlap the full and incremental DAGs. Airflow passes its run ID and fixed run start (`--as-of`) to the job and reuses both on retries.

## OLTP batch

`jobs.oltp.run_batch_pipeline` runs one batch end to end:

1. **Bronze.** Inside one repeatable-read PostgreSQL transaction, each of the 19 source tables is read in primary-key order (at most 10,000 rows per page, configured columns only) and appended to `bronze.<table>` in batches of 200,000 rows with `_ingested_at` and `_run_id`, partitioned by day of `_ingested_at`. Mutable tables use the `updated_at` cursor, append-only tables `created_at`. `--mode full` reads every row; `--mode incremental` requires `--interval-start` and `--interval-end` and selects `[start, end)` with no lookback. A retry with the same run ID first deletes that run's Bronze rows.
2. **Checks.** Before Silver changes, the run's Bronze rows are checked: row rules (required fields, allowed statuses, non-negative amounts, order and line arithmetic). Full runs also check duplicate primary keys, foreign keys within the run, source row counts of all 19 tables, money totals of `orders`, `payments` and `refunds`, and ledger stock against `inventory`/`store_inventory`. Incremental runs skip those: an order created inside the interval but updated after it is read by the next run, so its lines can briefly point at a missing parent. Any failure raises `CheckFailed` naming the check and the row count; Silver and Gold stay as they were.
3. **Silver.** `silver.<table>` keeps the current row per primary key. A full run replaces each table in one commit (rows deleted in the source disappear); an incremental run upserts in batches and never overwrites a row with an older cursor.
4. **Gold.** dbt (`pipelines/dbt/models/oltp`, tag `oltp`) rebuilds every Gold table from current Silver with `CREATE OR REPLACE`: 7 dimensions, 6 facts, 14 marts and 2 seeds (`delivery_sla`, `calendar_events`). `--as-of` is Gold's current day: it ends the dense daily series (inventory, backlog) and the 28-day windows. dbt data tests (`pipelines/dbt/tests/oltp` and the key tests in `gold.yml`) run with severity `warn`: they log, they do not block.

There is no run state, quarantine, quality-result table or publication for OLTP. Readers (Superset, the API) read the current Gold tables. Source tables, keys, cursors, columns and foreign keys are declared once in `SOURCE_TABLES` and `FOREIGN_KEYS` (`src/lakehouse/utils/config.py`).

Gold rules worth knowing:

- Revenue is recognized on the later of `completed_at` and `paid_at`. Refunds of recognized orders reduce revenue on the refund day; refunds of orders never recognized (paid, then cancelled) are not revenue.
- Gross profit = net revenue − cost of goods. Goods a refunded return puts back on the shelf (accepted at inspection) take their sold cost back out on the refund day, so a fully returned order nets to zero profit.
- A shipment is on time when its local delivery date is at most `sla_days` after the local order date. `sla_days` per city (seed `delivery_sla`) is the upper bound of the GHTK Express range of the city's zone plus one day of shop handling and one day of waiting for pickup: TP.HCM 4, Bình Dương, Cần Thơ and Hà Nội 6, Đà Nẵng and Hải Phòng 8.

The Airflow DAGs use `--namespace-prefix serving`, which resolves to the plain `bronze`, `silver`, `gold` and `system` namespaces. Only `serving` and isolated prefixes matching `polars_batch_[a-z0-9_]{1,40}` are accepted. The CLI takes `--namespace-prefix`, `--run-id`, `--mode`, a timezone-aware `--as-of`, the two interval bounds in incremental mode, and `--page-size` (1–10,000). The result line `BATCH_PIPELINE_RESULT` lists Bronze and Silver row counts and dbt statuses.

## Access logs

Flink (`kafka_to_lakehouse_pure_streaming`, submitted with `flink-submit`) appends every decoded Kafka payload to `landing.access_logs`, including invalid contracts. `jobs.logs.build_logs_gold` then pins Landing, inserts missing raw hashes into Bronze, deduplicates valid events into Silver and `fact_web_events`, runs the dbt log models (`pipelines/dbt/models/logs`) for the three marts against the current Silver product catalog, audits them and publishes to `system.logs_analytics_publications`. Conflicting valid payloads sharing an event ID fail the run instead of being chosen arbitrarily. Landing rows are selected by ingestion time up to `--as-of`, while typed events/facts are bounded by event time. Invalid events remain in Bronze with a validation error. The log CLI defaults to `serving` and requires `--run-id`; `--as-of` defaults to the current UTC time when omitted, so provide it explicitly for controlled retries.

Runtime API capture emits `data_origin=observed`. The separate `datagen logs` implementation writes synthetic daily Parquet (`raw_payload`, `event_at`); no checked-in loader sends those files to Kafka or Iceberg Landing. Generating files alone therefore does not populate these log tables. Generated payloads carry `session.id` and, for applicable actions, `ecommerce.order_number`; the current pipeline parser and typed Bronze/Silver/fact projections do not expose either field. Raw payloads retain them, but the existing marketing mart measures action/actor counts rather than a session-level, order-linked conversion funnel. See [generator](../generator/README.md).

The submitted Flink job explicitly configures 10-second exactly-once checkpoints, a five-second minimum pause and a 60-second timeout at `file:///opt/flink/checkpoints`, overriding the 30-second interval in `flink-conf.yaml`. These are code settings, not evidence of delivery or recovery performance.

Maintenance uses explicit table whitelists. Snapshot expiry protects references in retained log publications and Iceberg refs. Scheduled orphan actions report candidates only; physical deletion additionally requires `--delete-orphans --writers-stopped`.

## Layout

```text
src/lakehouse/
├── analytics/     # dbt runner
├── oltp/          # batch run, Polars Bronze, pre-merge checks, Silver merge
├── logs/          # Landing adapter, parser, Polars layers, marts and publication
├── streaming/     # Flink access-log runtime and sink setup
├── utils/         # config, Iceberg/Polaris catalog, PostgreSQL, SQL loader, maintenance
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

The local suite covers Bronze batching and retries, the pre-merge checks, the Silver merge and the batch orchestration against a SQLite-backed PyIceberg catalog (`pipelines/tests/iceberg_fixture.py`), plus the log pipeline, DAG templates and PostgreSQL source SQL. It does not run Trino, dbt or Kafka/Flink. A full run against a configured stack prints the dbt statuses in its result line; dbt artifacts are temporary.
