# PostgreSQL CDC Architecture and Operations

## 1. Implemented path and boundary

PostgreSQL WAL (`pgoutput`) is captured by Debezium/Kafka Connect into `ecommerce.public.<table>` topics. The PyFlink `oltp_cdc_to_bronze` job appends raw audit records to `lakehouse.bronze.oltp_cdc_events`, stored as Iceberg on MinIO and cataloged by Polaris. Transaction metadata uses `ecommerce.transaction`.

CDC does not merge typed Silver, build Gold or publish analytics snapshots. Airflow batch remains the OLTP analytical writer. Access-log capture is an independent path; enabling this CDC job does not enable the API's Kafka log producer. No end-to-end latency SLA or global exactly-once guarantee is claimed.

## 2. Source configuration

After the complete business schema is available, run `make source-cdc` explicitly. The command uses the existing PostgreSQL administrator to:

- Configure a dedicated LOGIN/REPLICATION role from `POSTGRES_CDC_USER` and `POSTGRES_CDC_PASSWORD`.
- Remove the former source-owner membership, replace its direct table/sequence grants with SELECT on the 27 analytical tables, and grant schema USAGE/database CONNECT. No default privileges are added for future tables.
- Set `REPLICA IDENTITY FULL` on those 27 tables so UPDATE/DELETE retain previous column values.
- Create or reconcile `ecommerce_publication` to exactly those tables. `customer_credentials` is excluded from grants, publication, connector allowlist and the Flink topic pattern.

The script fails before changing privileges when the source schema is incomplete or the requested CDC role is an owner/admin or has unrelated memberships. It is transactional and rerunnable; it applies the explicitly configured password and does not reset any replication slot, connector offset, Kafka topic or business row. ALTER TABLE/publication operations can take locks; schedule configuration as an explicit operational step.

Debezium uses `publication.autocreate.mode=disabled`, because an administrator prepares the publication. The connector no longer needs membership of the application owner. PostgreSQL's [replica identity and publication requirements](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html) explain the source-side contract. Fresh-volume initialization creates the CDC login; retained volumes also require the explicit post-schema setup.

## 3. Audit contract

The table retains one appended row per consumed Kafka record:

| Field | Meaning |
|---|---|
| `operation` | Envelope `r`, `c`, `u`, `d`; transaction `BEGIN`/`END`; NULL Kafka value maps to the local tombstone convention `t` |
| `event_id` | `<topic>:<partition>:<offset>` transport identity |
| `topic`, `kafka_partition`, `kafka_offset` | Kafka transport coordinates |
| `database_name`, `table_name` | Source database/table; transaction rows use `__transaction__` |
| `source_connector`, `source_schema` | PostgreSQL connector/schema metadata; database/schema fallbacks retain context for tombstones |
| `postgres_lsn`, `postgres_tx_id` | Source WAL/transaction coordinates, nullable for snapshots/control records where absent |
| `transaction_id` | Debezium transaction identity, retained independently of source txId |
| `source_ts`, `event_ts`, `ingested_at` | Source-change, connector/Kafka and sink timestamps, interpreted in UTC |
| `record_key`, `before_payload`, `after_payload`, `raw_payload` | Raw UTF-8 key/envelope and projected row images |
| `event_date` | UTC partition date from event time, with Kafka/ingestion fallback |

LSN alone is not a globally unique row-change identity; the raw envelope retains transaction ordering metadata. `event_id` is not a physically enforced UNIQUE constraint. A deliberate transport replay can append the same event identity again; consumers must account for this rather than treating raw audit row counts as unique business transactions.

A tombstone is identified by `raw_payload IS NULL`, not solely by operation `t`, which can also label a non-null Debezium truncate envelope. The raw Kafka format preserves nullable values and keys, allowing delete envelopes and their separate tombstones to remain visible. Topic discovery runs every 10 seconds so later-created analytical table topics are discovered. Startup uses committed group offsets with earliest fallback; see the [Flink 1.19 Kafka source contract](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/kafka/).

## 4. Existing audit history

The job keeps the same Iceberg table name. It adds missing `source_connector`, `source_schema`, `postgres_lsn` and `postgres_tx_id` columns, without dropping or rewriting historical rows. Legacy `mysql_server_id`, `binlog_file`, `binlog_position` and `binlog_row` remain available for old history and are NULL for new PostgreSQL events. New PostgreSQL columns are NULL on old rows.

The Python entrypoint is now `jobs/cdc/postgres_cdc_to_bronze.py`; Compose supplies `POSTGRES_ECOMMERCE_DB`. Stop an old writer before changing its job/schema. Preserve old checkpoints/savepoints and broker state; do not assume a MySQL job's savepoint is compatible with the changed PostgreSQL execution graph or source topics. This change does not translate old binlog positions into WAL positions or reconstruct missing historical before images.

## 5. Start and inspect

Configure `.env` from the example, bootstrap the business schema and initialize storage/catalog permissions first. If PostgreSQL already exists, `up` applies the new script mount while retaining its named volume:

```bash
docker compose --profile core up -d postgres minio polaris
make provision-lakehouse
make source-cdc
docker compose --profile core --profile streaming up -d --build \
  kafka debezium-connect flink-jobmanager flink-taskmanager
docker compose --profile core --profile streaming run --rm debezium-init
docker compose --profile core --profile streaming run --rm flink-cdc-submit
```

The last two services are manual (`scale: 0`). The submit command returns after detached submission; the Flink job keeps running. It skips submission when a matching job is already RUNNING, rather than launching a second writer.

Inspect Debezium through the configured host port (8086 by default):

```bash
curl -fsS http://127.0.0.1:8086/connectors/postgres-ecommerce-cdc/status
```

Read audit results using Trino:

```sql
SELECT source_connector, table_name, operation, count(*) AS records
FROM lakehouse.bronze.oltp_cdc_events
GROUP BY source_connector, table_name, operation;
```

## 6. Recovery and downstream behavior

The job checkpoints every 10 seconds and commits Iceberg on completed checkpoints; state is stored under the existing `flink-checkpoints` volume. TaskManager recovery uses the running JobManager's checkpoint state. Connect stores connector configuration and source offsets in its Kafka internal topics; the PostgreSQL slot `ecommerce_cdc` and publication must be retained. A fresh job submission from broker group offsets is not the same as restoring a Flink savepoint. Full JobManager HA/disaster recovery and arbitrary manual offset resets are outside this change.

Airflow still controls Silver/Gold updates independently from CDC transport positions. `manual_lakehouse_hard_delete` remains an explicit synchronization for only cart/wishlist delete events with soft-delete evidence and a source deletion timestamp at least 30 days old. Its before-image check accepts both PostgreSQL `false` and legacy `0`. Source hard deletion itself is not performed by that DAG, and this CDC change does not invoke it.

## 7. Isolated acceptance

Build the existing application, Spark, Flink and Debezium images and cache the infrastructure images declared in Compose. The opt-in fixture uses the repository's services on a unique internal network, random credentials and project-prefixed volumes, with no host ports or existing data mounts:

```bash
RUN_POSTGRES_CDC_TESTS=1 PYTHONPATH=pipelines/src \
  uv run --locked --all-packages -- \
  python -m unittest discover -s pipelines/tests -p test_cdc_stack.py -v
```

The fixture checks source setup/rerun and denials, snapshot row counts for every configured table, unchanged legacy audit fields, live create/update/delete/tombstone/transaction records, credentials exclusion, PostgreSQL coordinates, TaskManager checkpoint recovery and Connect restart. It verifies that CDC creates no Silver/Gold tables. Known credentials are redacted in local ignored reports under `data/pipeline-runs/tlcn-stack-*/`; private resources are removed afterward. This is a bounded synthetic acceptance case, not a throughput benchmark or proof of end-to-end exactly-once behavior.

Verified 09/11/2026: 594 snapshot records matched every source-table count across 27 configured tables; legacy history and live C/U/D/tombstone/transaction checks passed. TaskManager checkpoint recovery and graceful Connect restart captured subsequent writes without repeating the initial snapshot or observed prior row operations. Report: `data/pipeline-runs/tlcn-stack-085770b323/report.json` (local/ignored). The fixture created no Silver/Gold tables and removed its private resources.
