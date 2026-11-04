"""Append raw access-log envelopes to Landing; Spark owns downstream layers."""

import logging
import os

from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaOffsetResetStrategy, KafkaSource
from pyflink.datastream.functions import MapFunction
from pyflink.table import Schema
from pyflink.table.types import DataTypes

from lakehouse.streaming.runtime import create_stream_environment, create_table_environment
from lakehouse.streaming.catalog import ensure_medallion_namespaces, register_polaris_catalog
from lakehouse.streaming.tables import ensure_merge_on_read, ensure_streaming_tables
from lakehouse.logs.landing import LANDING_FIELDS, landing_envelope
from lakehouse.utils.sql import load_sql

log = logging.getLogger(__name__)
CHECKPOINT_INTERVAL_MS = 10_000


class LandingAccessLog(MapFunction):
    def map(self, raw):
        return Row(*landing_envelope(raw))


def run_access_stream():
    topic = os.environ.get("KAFKA_ACCESS_LOG_TOPIC", "ecommerce.access_logs")
    # 1. Environments
    env = create_stream_environment(checkpoint_interval_ms=CHECKPOINT_INTERVAL_MS)
    t_env = create_table_environment(env, pipeline_name="kafka_to_lakehouse_pure_streaming")

    # 2. Polaris Iceberg REST Catalog
    register_polaris_catalog(t_env, catalog_name="lakehouse")
    ensure_medallion_namespaces(t_env, catalog_name="lakehouse")
    ensure_streaming_tables(t_env)
    ensure_merge_on_read(t_env, ["lakehouse.landing.access_logs"])

    # 3. Kafka Source

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .set_topics(topic)
        .set_group_id(os.environ.get("KAFKA_ACCESS_LOG_GROUP_ID", "flink-lakehouse-pure-stream"))
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))
        .set_property("commit.offsets.on.checkpoint", "true")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-access-logs-source",
    ).uid("access-log-kafka-source")

    # 4. Map & Parse JSON
    field_names = [name for name, _ in LANDING_FIELDS]
    field_types = [getattr(Types, kind)() for _, kind in LANDING_FIELDS]
    parsed_type = Types.ROW_NAMED(field_names, field_types)

    parsed_stream = raw_stream.map(LandingAccessLog(), output_type=parsed_type).uid("access-log-landing-envelope")

    # 5. Convert DataStream to Table
    schema = Schema.new_builder()
    for name, kind in LANDING_FIELDS:
        table_kind = {"LONG": "BIGINT"}.get(kind, kind)
        schema.column(name, getattr(DataTypes, table_kind)())
    parsed_schema = schema.build()

    tbl = t_env.from_data_stream(parsed_stream, parsed_schema)
    t_env.create_temporary_view("raw_events_view", tbl)

    # Keep the existing job name so launch/health checks recognize this job.
    log.info("Submitting access capture: Kafka[%s] -> Landing only", topic)
    t_env.execute_sql(load_sql("flink/inserts/lakehouse_landing_access_logs.sql")).wait()
