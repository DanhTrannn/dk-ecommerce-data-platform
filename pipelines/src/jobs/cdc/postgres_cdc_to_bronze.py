"""Capture Debezium PostgreSQL change events from Kafka into Iceberg Bronze.

The sink is an immutable audit log: inserts, updates, deletes, initial snapshot
records and Debezium tombstones are retained with Kafka and WAL coordinates.
The existing Spark batch path remains the authoritative backfill/reconciliation
path; downstream consumers can deterministically order and replay this table.
"""

import logging
import os

from lakehouse.streaming.runtime import create_stream_environment, create_table_environment
from lakehouse.streaming.catalog import ensure_medallion_namespaces, register_polaris_catalog
from lakehouse.streaming.tables import ensure_cdc_tables
from lakehouse.streaming.cdc import kafka_source_ddl, insert_sql, topic_pattern

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("flink.postgres_cdc_to_bronze")

CHECKPOINT_INTERVAL_MS = 10_000


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic_prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce")
    database_name = os.environ.get("POSTGRES_ECOMMERCE_DB", "ecommerce")
    group_id = os.environ.get("CDC_FLINK_GROUP_ID", "flink-oltp-cdc-bronze-v1")

    env = create_stream_environment(checkpoint_interval_ms=CHECKPOINT_INTERVAL_MS)
    table_env = create_table_environment(env, pipeline_name="oltp_cdc_to_bronze")

    register_polaris_catalog(table_env, catalog_name="lakehouse")
    ensure_medallion_namespaces(table_env, catalog_name="lakehouse")
    ensure_cdc_tables(table_env)
    table_env.execute_sql(kafka_source_ddl(bootstrap_servers, topic_prefix, group_id))

    log.info(
        "Starting CDC stream Kafka[%s] -> lakehouse.bronze.oltp_cdc_events",
        topic_pattern(topic_prefix),
    )
    # The remote Flink CLI submits this unbounded query in detached mode.
    # Waiting for completion would keep the one-shot submit container alive forever.
    table_env.execute_sql(insert_sql(database_name))


if __name__ == "__main__":
    main()
