# Lakehouse Pipelines

Batch and log processing for the Lakehouse. Engines: **Polars 2.1 + PyIceberg 0.12** for ingestion, quality audit, publication and maintenance; **dbt 1.11 (dbt-trino)** for Gold and marts on Trino; **Flink 1.19** only appends API access logs to Landing.

## Setup

1. Bootstrap the PostgreSQL source schema and run `make source-reader` (SELECT-only role on the 27 analytical tables; `customer_credentials` excluded).
2. Configure `SILVER_PSEUDONYMIZE_SALT`, the MinIO batch workload keys (`MINIO_BATCH_*`), the bucket and the Polaris credential file produced by `make provision-lakehouse`.
3. Build the Airflow image (`infrastructure/airflow/Dockerfile`). It carries the pipeline runtime as a separate venv at `/opt/pipeline-venv`, exposed as `PIPELINE_PYTHON`; run jobs by hand with `docker compose exec airflow-scheduler`.

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

Source tables, keys, cursors and PII mappings are declared once in `SOURCE_TABLES` (`src/lakehouse/utils/config.py`). Source hard deletes are not synchronized: rows deleted in PostgreSQL remain in Silver.

## Access logs

Flink (`kafka_to_lakehouse_pure_streaming`, submitted with `flink-submit`) appends every decoded Kafka payload to `landing.access_logs`, including invalid contracts. `jobs.logs.build_logs_gold` then pins Landing, inserts missing raw hashes into Bronze, deduplicates valid events into Silver and `fact_web_events`, runs the dbt log models (`pipelines/dbt/models/logs`) for the three marts against the published OLTP catalog, audits them and publishes to `system.logs_analytics_publications`. Conflicting valid payloads sharing an event ID fail the run instead of being chosen arbitrarily.

## Layout

```text
src/lakehouse/
├── analytics/     # dbt runner and OLTP Gold build/publication
├── oltp/          # Polars Bronze/Silver pipeline, Gold schema, quality, publication
├── logs/          # Landing adapter, parser, Polars layers, marts and publication
├── streaming/     # Flink access-log runtime and sink setup
├── utils/         # config, Iceberg/Polaris catalog, PostgreSQL, SQL loader, maintenance
└── sql_templates/ # contracts (Silver/Gold/log schemas), flink
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
