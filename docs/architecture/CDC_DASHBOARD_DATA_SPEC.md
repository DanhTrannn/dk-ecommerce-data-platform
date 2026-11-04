# MySQL CDC Architecture and Operations

## 1. Implemented data path

The transactional CDC path is deployed independently from access-log streaming:

```text
MySQL 8.4 row binlog
  -> Debezium MySQL Connector / Kafka Connect
  -> ecommerce.ecommerce.<table> Kafka topics
  -> PyFlink oltp_cdc_to_bronze
  -> lakehouse.bronze.oltp_cdc_events (Iceberg v2)
```

This path retains immutable audit events. It does not write typed Silver, Gold or analytical publications. The daily Airflow batch owns analytical updates; no mode switch or persistent Spark CDC consumer is required. No end-to-end latency SLA is claimed.

Access logs continue through their separate
`ecommerce.access_logs -> Flink -> Landing/Bronze/Silver/Gold` pipeline.

## 2. Capture contract

Debezium captures the 27 analytical OLTP tables. `customer_credentials` is
intentionally excluded because it contains authentication secrets and is not an
analytical source.

The immutable `lakehouse.bronze.oltp_cdc_events` table stores one row per Kafka
record with:

- `operation`: source envelope `op`, normally `r` (snapshot), `c` (create),
  `u` (update), or `d` (delete). The local sink maps a NULL Kafka payload to
  `t`; this is its tombstone convention, not a general definition of Debezium
  `t` (which can also represent a truncate event).
- `before_payload` and `after_payload`: row images from the Debezium envelope.
- `binlog_file`, `binlog_position`, and `binlog_row`: source replay ordering.
- `topic`, `kafka_partition`, and `kafka_offset`: transport replay coordinates.
- `event_id`: deterministic `<topic>:<partition>:<offset>` identity.
- `source_ts`, `event_ts`, and `ingested_at`: source, connector, and sink timing.
- `record_key` and `raw_payload`: the original Kafka key and Debezium envelope.

The table is append-only and partitioned by `event_date`. `event_id` is a replay
identity, not a physically enforced UNIQUE constraint; consumers must account
for possible replay duplicates. The table deliberately does
not overwrite deletes or collapse updates, so audits can reconstruct state at a
specific offset. Consumers must use the latest event per primary key when they
need a current-state projection.

## 3. MySQL and connector guarantees

MySQL runs with row-based binlog, full row images, GTID, and seven-day binlog
retention. A dedicated `debezium` account receives `SELECT`, `RELOAD`,
`SHOW DATABASES`, `REPLICATION SLAVE`, and `REPLICATION CLIENT` privileges
**on `*.*`**. The connector's table allowlist restricts capture, not this
account's SQL visibility to just those 27 tables.

The connector is registered idempotently by `debezium-init` with:

- initial consistent snapshot followed by continuous binlog capture;
- one topic per captured table;
- transaction metadata and schema history topics;
- delete events plus Kafka tombstones;
- JSON envelopes without embedded Connect schemas;
- fail-fast conversion/error handling.

The job configures 10-second exactly-once checkpoints and checkpoint-driven
Iceberg commits. Checkpoints use `file:///opt/flink/checkpoints` on a Docker
volume. These settings do not establish exactly-once delivery across MySQL,
Connect, Kafka, manual job resubmission and all downstream consumers. Kafka
coordinates and deterministic event IDs support replay inspection.

## 4. Run and inspect

Start the complete stack:

```bash
docker compose --profile core --profile batch --profile streaming up -d --build
```

Prepare `.env` using [SETUP](../runbook/SETUP.md) first. The connector endpoint
below uses the default host port; adjust for `CDC_CONNECT_PORT` overrides.

Inspect the connector:

```bash
curl -fsS http://127.0.0.1:8086/connectors/mysql-ecommerce-cdc/status
```

Query captured operations:

```sql
SELECT table_name, operation, count(*) AS events
FROM lakehouse.bronze.oltp_cdc_events
GROUP BY table_name, operation
ORDER BY table_name, operation;
```

## 5. Audit boundary and recovery

CDC stores insert/update/delete envelopes and transaction metadata in Bronze. It does not apply row images to Silver or rebuild/publish Gold. The former Spark CDC materializer, typed-row state and automatic delete propagation are no longer part of the runtime.

Do not reset Kafka Connect offsets/history or remove Bronze audit data during normal operation. Flink restore/replay still needs its documented checkpoint procedure; submitting a fresh job is not the same as restoring state.

Airflow batch is the analytical writer: source interval → staging → Bronze → Silver MERGE → Gold/marts → validated publication. Reader snapshot pinning is retained; CDC transport offsets do not determine batch progress. See [pipeline guide](../../pipelines/README.md) and [migration runbook](../runbook/SETUP.md#71-move-an-existing-cdc-written-installation-to-batch).

Normal operations use source soft-delete flags. `manual_lakehouse_hard_delete` explicitly selects supported tables and previews by default. It reads only CDC delete events whose before image has the configured soft-delete marker and source `updated_at` at least 30 days old. Setting `apply=true` synchronizes Silver, rebuilds/publishes Gold, and retains Bronze/audit/history. The DAG does not purge MySQL. Recorded deletion decisions prevent batch replay from resurrecting approved keys. See the [pipeline guide](../../pipelines/README.md#cdc-and-access-logs) for table eligibility and execution.
