# MySQL CDC Architecture and Operations

## 1. Implemented data path

The transactional CDC path is deployed independently from access-log streaming:

```text
MySQL 8.4 row binlog
  -> Debezium MySQL Connector / Kafka Connect
  -> ecommerce.ecommerce.<table> Kafka topics
  -> PyFlink oltp_cdc_to_bronze
  -> lakehouse.bronze.oltp_cdc_events (Iceberg v2)
  -> Spark CDC materializer -> typed Silver -> affected Gold/Data Marts
  -> system.analytics_publications (exact table snapshots + committed Kafka offsets)
  -> Trino -> FastAPI role dashboards
```

The CDC pipeline has a persistent Spark consumer that reads Bronze offsets, applies complete MySQL transactions to typed Silver, refreshes dependent Gold tables, validates results, and then commits an Iceberg publication manifest. The OLTP Airflow DAG is blocked in default CDC mode to prevent competing batch writers. Set `LAKEHOUSE_OLTP_MODE=batch` for a controlled batch handoff only after stopping the CDC consumer. No end-to-end latency SLA is claimed.

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

## 5. Recovery and operational rules

- Never delete the Kafka Connect offset or schema-history topics during normal
  operation.
- A connector normally resumes from its stored binlog offset while that offset
  and the binlogs remain available. The CDC Kafka source uses group offsets,
  with earliest fallback. The submission script does not supply a checkpoint
  or savepoint restore argument; a newly submitted job must not be assumed to
  restore the prior job's state automatically. Verify the actual restore point
  and committed Kafka offsets before accepting a recovery as complete.
- If binlogs have expired before the connector resumes, stop the connector,
  perform a controlled new snapshot, and reconcile with the Spark batch path.
- Changing `CDC_TOPIC_PREFIX` or `CDC_FLINK_GROUP_ID` creates a new logical
  stream; treat that as a migration, not an ordinary restart.
- Keep Bronze CDC access restricted because raw row images may contain PII.
  Pseudonymization remains mandatory before exposing customer data downstream.
- Use the Spark reconciliation and Gold quality gates after recovery or replay.

## 6. Remaining recovery limits

CDC capture and CDC-driven typed Silver/Gold publication are implemented. Store, inventory, checkout, POS and other transaction endpoints still use MySQL because they perform operational reads/writes. KPI dashboard APIs use Trino publications. Flink replacement jobs must be checked for their actual restore point; Bronze `event_id` is deterministic but the sink does not impose a physical uniqueness constraint.

Implementation references: [connector configuration](../../infrastructure/debezium/register-connector.sh),
[MySQL account](../../infrastructure/mysql/create-cdc-user.sh),
[Flink job](../../pipelines/src/jobs/cdc/mysql_cdc_to_bronze.py),
[sink DDL/checkpoint factory](../../pipelines/src/lakehouse/flink.py),
[submission script](../../infrastructure/flink/bin/submit-job.sh).


## 7. CDC materialization and dashboard publication

`spark-cdc` is a persistent Spark micro-batch consumer (default poll interval 30 seconds). It deduplicates records by topic coordinates plus source transaction/binlog identity (because Kafka offsets can be reused after a topic reset), waits for Debezium transaction END and its `event_count`, applies inserts/updates/deletes to the 27 typed Silver tables, and tracks per-key binlog ordering in `system.cdc_row_state`. PII fields use the same pseudonymization path as batch Silver; coarse shipping region is derived before address pseudonymization. Invalid rows are quarantined and block checkpoint advancement.

For initial bootstrap, legacy Bronze history can contain transactions whose END record predates retained Kafka metadata. The first publication therefore requires symmetric all-column parity between all 27 curated Silver tables and a live read of their MySQL source tables. After bootstrap, missing or incomplete transaction END metadata blocks publication. A source write during the one-time all-table parity check can cause a fail-closed retry.

After applying Silver, the consumer rebuilds only dependent Gold tables (whole-table replacement currently removes groups lost after deletes), runs reference/order/inventory checks, and appends a publication row with exact Iceberg snapshot IDs and Kafka offsets. This is table-level dependency refresh, not row/partition-level incremental Gold processing. The API pins Silver/Gold queries with `FOR VERSION AS OF`; web-event Gold remains on its separate access-log streaming lifecycle. It uses a bounded last-good OLAP response during Trino errors, then returns HTTP 503 when no unexpired response exists. It never substitutes MySQL analytics.

`system.cdc_pipeline_state` reports consumer heartbeat, status, pending event count and oldest pending event. The System dashboard compares published Gold with current MySQL source counts/revenue, so its reconciliation can temporarily reflect source changes waiting in CDC. Dashboard browser refresh is 15 seconds; that is a polling interval, not a CDC propagation SLA.

Operational boundaries: the shared POSIX `flock` serializes writers only on this Docker host; it is not a distributed lock. Kafka transaction completeness depends on the transaction metadata topic and the configured topic prefix. The source cursor uses MySQL GTID UUID and binlog file/position ordering; changed source identity fails closed until an operator performs a controlled rebootstrap. Kafka offsets alone are not treated as a globally unique source checkpoint. The daily batch and CDC writer must not run concurrently.

## Publication schema 3 and compatibility repair

Batch and CDC now use the same merchandise transformations and snapshot publication helper. `schema_version=3` and `source_mode` distinguish the metric contract and writer. The API rejects older contracts until rebuilt; cached results never trigger MySQL fallback.

`fact_inventory_current` is provisioned explicitly and refreshed by both writers. Daily snapshots remain separate. Orders expose the UTC creation timestamp, Vietnam order date and coarse shipping region required by the API. Payment extraction uses `updated_at`; existing databases need the explicit timestamp upgrade and current-payment refresh in [SETUP](../runbook/SETUP.md#71-upgrade-payment-timestamps-and-analytical-publication-schema).

Merchandise revenue excludes shipping, recognizes fulfilled orders, allocates discounts in integer VND and deducts successful refunds. Completed accepted returns reverse historical item cost. Fact/item/mart financial totals are checked before publication; see [metrics](METRICS_CATALOG.md). An upgrade with no new CDC events still rebuilds and publishes the changed contract.

The Flink CDC subscription includes the transaction topic and maps Debezium BEGIN/END records into Bronze. Spark disables the Iceberg catalog cache and explicitly refreshes Bronze table metadata before each read because Flink commits snapshots externally. Source checkpoints include snapshot rows with binlog coordinates, so an initial snapshot does not replay indefinitely when Kafka offsets are reused.

Controlled batch publications retain the last CDC transport/source checkpoint. Returning to CDC rebuilds and republishes the current contract even without new source changes. Inventory snapshots use their actual Vietnam observation date; an Airflow logical interval does not reconstruct historical stock. Publication validation checks the inventory observation date and successful refund money against Silver as well as fact/mart parity.

Kafka broker logs and internal Connect offset/schema-history topics reside in the named `kafka-data` volume via `KAFKA_LOG_DIRS=/var/lib/kafka/data`. Existing data under the old image-default directory must be migrated before broker recreation; source-binlog ordering remains a separate replay safeguard.
