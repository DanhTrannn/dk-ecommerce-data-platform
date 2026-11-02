# Lakehouse Pipelines

The `batch-pipeline` workspace package contains Spark OLTP extraction/transformation/quality jobs, PyFlink access-log and CDC jobs, and Iceberg maintenance helpers. Spark/Flink execution runs in the corresponding Docker images; the Python workspace alone is not their complete runtime.

## Source map

| Path | Responsibility |
|---|---|
| [config/default.yml](config/default.yml) | 27 source tables, PK/cursor, mutability, PII fields and lookback |
| [src/jobs/oltp](src/jobs/oltp) | Extract, Landing-to-Bronze, Silver, Gold and gates |
| [src/jobs/logs/kafka_to_lakehouse.py](src/jobs/logs/kafka_to_lakehouse.py) | Kafka access stream to four Iceberg sinks |
| [src/jobs/logs/build_logs_gold.py](src/jobs/logs/build_logs_gold.py) | Three log marts from fact_web_events |
| [src/jobs/cdc/mysql_cdc_to_bronze.py](src/jobs/cdc/mysql_cdc_to_bronze.py) | Debezium table topics to append-only Bronze events |
| [src/jobs/maintenance](src/jobs/maintenance) | Compaction, manifest rewrite, expiry, orphan and quarantine cleanup |
| [src/lakehouse/oltp](src/lakehouse/oltp) | SQL predicates, extract, cursor, Bronze/Silver/Gold DDL/transform, reconciliation/quality |
| [src/lakehouse/logs](src/lakehouse/logs) | Log schemas and Spark rollup transformations |
| [src/lakehouse/flink.py](src/lakehouse/flink.py) | Active Flink table DDL, upsert/checkpoint/catalog configuration |
| [src/lakehouse/spark.py](src/lakehouse/spark.py) | Spark session, UTC timezone, S3 and REST catalog configuration |
| [Airflow DAGs](../airflow/dags) | Batch and separate maintenance orchestration |

## Batch OLTP

The `lakehouse_oltp_pipeline` DAG runs daily at 02:00 Asia/Ho_Chi_Minh, with catchup disabled and one active run. Compose creates DAGs paused. In the default `LAKEHOUSE_OLTP_MODE=cdc`, it runs a read-only source-to-Silver reconciliation task. Set mode to `batch` only for a controlled handoff: stop the CDC consumer first, then run the batch chain.

1. Capture per-table high watermarks; Spark reads composite cursor windows with a default 10-minute lookback.
2. Write Landing Parquet/manifests; validate listed objects and MD5 integrity metadata. MD5 here is an integrity check, not authentication of an untrusted source.
3. Append source data/lineage to Bronze; **commit cursors after Bronze succeeds**.
4. Type, deduplicate, pseudonymize configured PII and MERGE Silver; quarantine violations.
5. Reconcile MySQL against Silver at a 0.05% tolerance before Gold.
6. Build Gold dimensions and facts in parallel, then marts.
7. Run post-Gold quality checks and write evidence to `lakehouse.system.data_quality_results`.

Gold OLTP defines 6 dimensions, 6 facts (including current inventory) and 4 marts. Counts/keys/financial checks are separate from the dashboard's display thresholds. Each table commits independently; a post-Gold failure does not roll back every previously written table. Watermarks are not a single consistent snapshot across all source tables.

Batch does not propagate all hard deletes. Payments use created_at even though COD completion updates status, so old updates outside lookback can be missed. In CDC mode these updates and hard deletes flow into typed Silver and dependent Gold. CDC publication gates transaction completeness, validation, source parity at initial bootstrap, data quality, and a shared local writer lock; a failed batch leaves the published Kafka offsets unchanged for retry. The local `flock` does not coordinate writers across hosts.

## Access-log streaming and CDC

| Job | Source | Sinks / boundary |
|---|---|---|
| `kafka_to_lakehouse_pure_streaming` | Kafka ecommerce.access_logs | One StatementSet inserts Landing access_logs, Bronze web_events, Silver silver_logs and Gold fact_web_events |
| `oltp_cdc_to_bronze` | Debezium table and transaction metadata topics | Bronze oltp_cdc_events, including transaction BEGIN/END, raw row images and Kafka coordinates |

Flink jobs configure 10-second checkpoints on the mounted checkpoint directory; this is not a measured end-to-end latency or exactly-once SLA. `spark-cdc` polls Bronze at `CDC_APPLY_INTERVAL_SECONDS` (default 30), merges row changes into 27 typed Silver tables, refreshes dependent Gold tables, and appends one publication manifest with the completed offsets and Iceberg snapshot IDs. Reader APIs pin related tables to that publication. Silver row watermarks make replayed events/deletes idempotent. The publication records both Kafka offsets and a MySQL GTID/binlog cursor, so a reset Kafka topic does not make lower offsets silently invisible. A changed GTID UUID or backwards binlog position fails closed. A local file lock serializes Spark batch and CDC writers.

The access job uses request identity/upsert configuration for Silver/Gold. Invalid JSON is logged and dropped; a full schema/quarantine path is not enforced for every message. Access-log timestamps are normalized to UTC before storage and event-date derivation. Historical JSONL uploaded under landing/logs is not read by this Kafka job. See [access logs](../docs/architecture/ACCESS_LOG_DESIGN.md) and [CDC](../docs/architecture/CDC_DASHBOARD_DATA_SPEC.md).

## Maintenance and mart rollups

| DAG | Schedule (VN) | Current scope |
|---|---|---|
| `lakehouse_oltp_maintenance` | 03:00 daily | 16 Gold + 8 listed Silver tables; separate quarantine cleanup |
| `lakehouse_streaming_maintenance` | Every 2 hours | Four log tables, then three log mart rollups for the current VN date |

Maintenance does not yet cover all 27 Silver tables, batch Bronze or Bronze CDC. Snapshot expiry must retain enough history for pinned publications still served by the API cache. Refer to TABLE_PROFILES in the maintenance job. OLTP compaction targets 128 MiB, snapshot expiry retains at least the configured last 10 snapshots (20 for logs), orphan cleanup defaults to a 3-day cutoff. Quarantine retention deletes rows older than 30 days in quarantine.silver_oltp_violations; it is not a bucket-wide retention policy.

The streaming health task checks for a RUNNING job, not independently for both required streams. Maintenance DAGs have no cross-DAG dependency waiting for batch completion. The log rollup CLI can omit --ingest-date to process all available fact dates; the scheduled DAG supplies today's VN date.

## Run and inspect

After configuring and starting the stack, load the source history and use:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_pipeline
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_pipeline
docker compose exec trino trino --execute "SHOW TABLES FROM lakehouse.gold;"
docker compose exec trino trino --execute "SELECT * FROM lakehouse.system.data_quality_results LIMIT 20;"
```

A successful schema bootstrap only creates tables; it does not populate analytical results. Full startup, other DAGs and recovery checks are in [SETUP](../docs/runbook/SETUP.md). Model grains and KPI formulas are in [Lakehouse Design](../docs/project/LAKEHOUSE_DESIGN_PLAN.md) and [Metrics Catalog](../docs/architecture/METRICS_CATALOG.md).
