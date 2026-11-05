"""DataFrame transformations for raw Landing, typed Bronze, Silver and facts."""

from dataclasses import astuple

from pyspark.sql import Window, functions as F, types as T

from lakehouse.logs.parser import InvalidAccessEvent, parse_access_event
from lakehouse.logs.stream_schema import FIELD_SPECS

_TYPES = {
    "STRING": T.StringType(),
    "LONG": T.LongType(),
    "INT": T.IntegerType(),
    "DOUBLE": T.DoubleType(),
    "BOOLEAN": T.BooleanType(),
}
_PARSED_SCHEMA = T.StructType([T.StructField(name, _TYPES[kind]) for name, kind in FIELD_SPECS])


def _parse(raw):
    try:
        return astuple(parse_access_event(raw))
    except InvalidAccessEvent:
        return None


def _nullable(name):
    return F.when(F.col(name) != "", F.col(name))


def build_bronze_delta(landing, bronze, run_id, *, as_of, source_ref):
    """Retain each distinct raw payload once, including rejected contracts."""
    raw = (
        landing.filter(F.col("ingest_ts") <= F.lit(as_of))
        .groupBy("raw_payload")
        .agg(F.min("ingest_ts").alias("_landed_at"))
    )
    raw = raw.withColumn("_raw_hash", F.sha2("raw_payload", 256)).join(
        bronze.select("_raw_hash").where("_raw_hash IS NOT NULL"), "_raw_hash", "left_anti"
    )
    parsed = raw.withColumn("p", F.udf(_parse, _PARSED_SCHEMA)("raw_payload"))
    return parsed.select(
        F.col("p.event_id").alias("event_id"),
        F.to_timestamp("p.event_ts").alias("event_ts"),
        F.to_timestamp("p.observed_timestamp").alias("observed_timestamp"),
        F.struct(F.col("p.schema_name").alias("name"), F.col("p.schema_version").alias("version")).alias("schema"),
        F.struct(
            F.col("p.service_name").alias("name"),
            F.col("p.service_version").alias("version"),
            F.col("p.service_env").alias("environment"),
            F.col("p.service_instance_id").alias("instance_id"),
        ).alias("service"),
        F.col("p.severity_number").alias("severity_number"),
        F.col("p.severity_text").alias("severity_text"),
        _nullable("p.trace_id").alias("trace_id"),
        _nullable("p.span_id").alias("span_id"),
        F.struct(
            *[F.col(f"p.event_{c}").alias(c) for c in ["name", "category", "kind", "outcome"]],
            F.col("p.duration_ns").alias("duration_ns"),
        ).alias("event"),
        F.struct(
            F.col("p.http_method").alias("request_method"),
            F.col("p.http_route").alias("route"),
            F.col("p.http_status").alias("status_code"),
        ).alias("http"),
        F.struct(F.col("p.event_id").alias("id")).alias("request"),
        F.struct(F.col("p.actor_type").alias("type"), _nullable("p.actor_key").alias("key")).alias("actor"),
        F.struct(F.col("p.client_user_agent").alias("user_agent")).alias("client"),
        F.struct(
            F.col("p.action").alias("action"),
            _nullable("p.product_key").alias("product_key"),
            _nullable("p.variant_key").alias("variant_key"),
            _nullable("p.search_query").alias("search_query"),
            F.col("p.search_redacted").alias("search_redacted"),
            F.lit(None).cast("map<string,string>").alias("filters"),
        ).alias("ecommerce"),
        F.struct(_nullable("p.error_code").alias("code"), _nullable("p.error_type").alias("type")).alias("error"),
        F.col("p.data_origin").alias("data_origin"),
        F.lit(run_id).alias("_run_id"),
        F.lit("access-log-landing").alias("_source_system"),
        F.lit(source_ref).alias("_source_file"),
        F.col("_landed_at").cast("timestamp").alias("_ingested_at"),
        "raw_payload",
        "_raw_hash",
        F.when(F.col("p").isNull(), F.lit("invalid_access_event")).alias("_validation_error"),
    )


def build_silver_delta(bronze, silver, *, as_of):
    valid = bronze.filter(
        F.col("_validation_error").isNull() & F.col("event_id").isNotNull() & (F.col("event_ts") <= F.lit(as_of))
    )
    # Legacy typed Bronze rows have no raw hash and are retained. Pick one
    # deterministic observation; never rewrite an event already in Silver.
    window = Window.partitionBy("event_id").orderBy(
        F.col("_ingested_at").asc_nulls_last(), F.col("_raw_hash").asc_nulls_last()
    )
    valid = (
        valid.withColumn("_row", F.row_number().over(window))
        .filter("_row=1")
        .join(silver.select("event_id"), "event_id", "left_anti")
    )
    fields = {
        "schema_name": "schema.name",
        "schema_version": "schema.version",
        "service_name": "service.name",
        "service_version": "service.version",
        "service_environment": "service.environment",
        "service_instance_id": "service.instance_id",
        "event_name": "event.name",
        "event_category": "event.category",
        "event_kind": "event.kind",
        "event_outcome": "event.outcome",
        "event_duration_ns": "event.duration_ns",
        "http_request_method": "http.request_method",
        "http_route": "http.route",
        "http_status_code": "http.status_code",
        "request_id": "request.id",
        "actor_type": "actor.type",
        "actor_key": "actor.key",
        "client_user_agent": "client.user_agent",
        "ecommerce_action": "ecommerce.action",
        "ecommerce_product_key": "ecommerce.product_key",
        "ecommerce_variant_key": "ecommerce.variant_key",
        "ecommerce_search_query": "ecommerce.search_query",
        "ecommerce_search_redacted": "ecommerce.search_redacted",
        "ecommerce_filters": "ecommerce.filters",
        "error_code": "error.code",
        "error_type": "error.type",
    }
    return valid.select(
        "event_id",
        "event_ts",
        "observed_timestamp",
        "severity_number",
        "severity_text",
        "trace_id",
        "span_id",
        "data_origin",
        *[F.col(source).alias(target) for target, source in fields.items()],
        F.col("_ingested_at").alias("_silver_ingested_at"),
        F.col("_run_id").alias("_source_bronze_run_id"),
    )


def _duration_ms(value):
    # Preserve the parser's Python rounding at binary half-way boundaries.
    return round(value / 1_000_000.0, 3) if value is not None else None


def build_fact_delta(silver, facts, run_id, *, as_of):
    fresh = silver.filter(F.col("event_ts") <= F.lit(as_of)).join(facts.select("event_id"), "event_id", "left_anti")
    duration = F.udf(_duration_ms, T.DoubleType())("event_duration_ns")
    return fresh.select(
        "event_id",
        "event_ts",
        F.to_date("event_ts").alias("event_date"),
        "actor_key",
        "actor_type",
        "http_request_method",
        "http_route",
        "http_status_code",
        "ecommerce_action",
        F.col("ecommerce_product_key").alias("product_key"),
        F.col("ecommerce_variant_key").alias("variant_key"),
        duration.alias("duration_ms"),
        (F.col("http_status_code") < 400).alias("is_success"),
        F.col("http_status_code").between(400, 499).alias("is_client_error"),
        (F.col("http_status_code") >= 500).alias("is_server_error"),
        (duration >= 1000).alias("is_slow_request"),
        F.lit(as_of).cast("timestamp").alias("_gold_ingested_at"),
        F.lit(run_id).alias("_source_run_id"),
    )
