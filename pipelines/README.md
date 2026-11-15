# Lakehouse Pipelines

Batch and log processing for the Lakehouse. Engines: **Polars 2.1 + PyIceberg 0.12** for ingestion, quality audit, publication and maintenance; **dbt 1.11 (dbt-trino)** for Gold and marts on Trino; **Flink 1.19** only appends API access logs to Landing.

## Setup

1. Bootstrap the PostgreSQL source schema and run `make source-reader` (SELECT-only role on the 27 analytical tables; `customer_credentials` excluded).
2. Configure `SILVER_PSEUDONYMIZE_SALT`, the MinIO batch workload keys (`MINIO_BATCH_*`), the bucket and MinIO policies/users through Streamlit. Create the Polaris principal/roles, then use **Cấp cho workload** to assign credentials to `batch-airflow` and the Trino consumers; the exporter writes their credential volumes. MinIO key/secret values must match the workload environment; restart consumers after changing credentials. See [setup](../docs/setup.md#2-build-và-chọn-profile).
3. Configure `DBT_TRINO_PASSWORD` and the Trino CA/credential volumes as described in setup. dbt connects over TLS on port 8443 as `dbt_writer` through `lakehouse_etl`; Gold writes are restricted to `gold` and isolated `polars_batch_*_gold` schemas. The separate `analytics_reader` reads the `lakehouse` catalog, whose connector is read-only. Trino is therefore both a restricted Gold writer and an analytical reader.
4. Build the Airflow image (`infrastructure/airflow/Dockerfile`). It carries the pipeline runtime as a separate venv at `/opt/pipeline-venv`, exposed as `PIPELINE_PYTHON`; run jobs by hand with `docker compose exec airflow-scheduler`.

## DAGs

| DAG | Schedule | Tasks |
|---|---|---|
| `init_lakehouse_oltp` | manual | `ingest_source` (all rows) → `build_validate_publish` |
| `lakehouse_oltp_pipeline` | `0 3 * * *`, one active run | `ingest_source` (interval `[start, end)`) → `build_validate_publish` |
| `lakehouse_logs_marts` | hourly | `rollup_logs_gold_marts` |
| `lakehouse_oltp_maintenance` | `0 4 * * *` | `compact` → `expire` → `orphan` report → `quarantine` cleanup |
| `lakehouse_streaming_maintenance` | `0 */2 * * *` | Flink health check → `compact` → `expire` → `orphan` report for log tables |

Schedules use `Asia/Ho_Chi_Minh`, with `catchup=False`. OLTP DAG tasks retry twice; log and maintenance tasks retry once, all after two minutes. Do not overlap initialization and incremental runs: a PostgreSQL advisory lock serializes individual native jobs sharing a namespace, but does not lock the whole two-task DAG sequence. Overlapping jobs fail lock acquisition rather than wait. Airflow passes its fixed run start as `--as-of`, reused on retries; this freezes the calculation clock, not the PostgreSQL source contents or the input snapshot set across attempts.

## OLTP batch

1. `jobs.oltp.run_polars_batch` reads each of the 27 tables from PostgreSQL in primary-key order, at most 10,000 rows per page. Mutable tables use `updated_at`, append-only tables `created_at`; timestamps are UTC and there is no lookback. Both timezone-aware interval bounds are required together and select `[start, end)`; omitting both performs a full read. All selected sources are staged to Parquet before Bronze/Silver data commits begin. Subsequent commits are per table/page, with no transaction covering all tables.
2. Bronze upserts by primary key + cursor timestamp, retaining versions observed by batch reads; it does not capture every intervening source update. Replays reuse that version identity. Silver validates, deduplicates, pseudonymizes PII (SHA-256 with the salt) and merges by primary key; invalid rows go to quarantine.
3. `jobs.oltp.build_oltp_gold` pins the Silver snapshots, runs the dbt project (`pipelines/dbt/models/oltp`: 6 dimensions, 6 facts, 15 marts) on Trino, audits the results (including the dbt data tests in `pipelines/dbt/tests`) and publishes a snapshot manifest to `system.analytics_publications`. The OLTP publication requires all 27 Silver and 27 Gold snapshot references and the complete 55-check native gate. Publication records are written with PyIceberg upsert. A failed gate leaves the previous manifest unchanged, but may leave newer physical Gold snapshots behind; readers must use publication-pinned snapshots rather than the current tables to retain that guarantee.

Source tables, keys, cursors and PII mappings are declared once in `SOURCE_TABLES` (`src/lakehouse/utils/config.py`). Source hard deletes are not synchronized: rows deleted in PostgreSQL remain in Silver. Late or backdated changes outside the selected cursor interval are not recovered automatically. Retrying an interval reads the source again; replay-safe keys do not make a retry an identical historical source read. Keep the PII salt stable across runs.

The Airflow DAGs use `--namespace-prefix serving`, which resolves to the plain `bronze`, `silver`, `gold`, `quarantine` and `system` namespaces. For example, manifests are read as `lakehouse.system.analytics_publications`, not a `serving_system` schema. Only `serving` and isolated prefixes matching `polars_batch_[a-z0-9_]{1,40}` are accepted. The batch CLI requires `--namespace-prefix` and `--run-id`; optional `--table` may be repeated for a unique subset and `--page-size` is 1–10,000. Gold defaults to `serving`, requires `--run-id` and a timezone-aware `--as-of`, and accepts `--batch-date` and `--verify-only`. Verification audits current tables without running dbt or publishing; the wrapper still ensures target schemas/tables exist.

## Access logs

Flink (`kafka_to_lakehouse_pure_streaming`, submitted with `flink-submit`) appends every decoded Kafka payload to `landing.access_logs`, including invalid contracts. `jobs.logs.build_logs_gold` then pins Landing, inserts missing raw hashes into Bronze, deduplicates valid events into Silver and `fact_web_events`, runs the dbt log models (`pipelines/dbt/models/logs`) for the three marts against the published OLTP catalog, audits them and publishes to `system.logs_analytics_publications`. Conflicting valid payloads sharing an event ID fail the run instead of being chosen arbitrarily. Landing rows are selected by ingestion time up to `--as-of`, while typed events/facts are bounded by event time. Invalid events remain in Bronze with a validation error. The log CLI defaults to `serving` and requires `--run-id`; `--as-of` defaults to the current UTC time when omitted, so provide it explicitly for controlled retries.

Runtime API capture emits `data_origin=observed`. The separate `datagen logs` implementation writes synthetic daily Parquet (`raw_payload`, `event_at`); no checked-in loader sends those files to Kafka or Iceberg Landing. Generating files alone therefore does not populate these log tables. Generated payloads carry `session.id` and, for applicable actions, `ecommerce.order_number`; the current pipeline parser and typed Bronze/Silver/fact projections do not expose either field. Raw payloads retain them, but the existing marketing mart measures action/actor counts rather than a session-level, order-linked conversion funnel. See [generator](../generator/README.md).

The submitted Flink job explicitly configures 10-second exactly-once checkpoints, a five-second minimum pause and a 60-second timeout at `file:///opt/flink/checkpoints`, overriding the 30-second interval in `flink-conf.yaml`. These are code settings, not evidence of delivery or recovery performance.

Maintenance uses explicit table whitelists. Snapshot expiry protects references in retained OLTP/log publications and Iceberg refs. Scheduled orphan actions report candidates only; physical deletion additionally requires `--delete-orphans --writers-stopped`. Quarantine cleanup belongs to the OLTP schedule.

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

The current local checkout contains `pipelines/tests/test_exported_credentials.py`, a mocked credential-file parsing test. `pipelines/tests/stack/runtime.py` and the former generator-backed stack fixture are absent. The command above discovers whichever local tests are present; it does not establish PostgreSQL ingestion, dbt execution, publication, or Kafka/Flink recovery. No current end-to-end acceptance result is asserted here. To retain dbt build evidence from a configured runtime, set `DBT_ARTIFACTS_DIR`; the wrapper copies `manifest.json` and `run_results.json` under `<directory>/<run-id>/<oltp|logs>/`. Without that setting, dbt artifacts are temporary.
