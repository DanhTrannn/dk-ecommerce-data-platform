"""Replay-safe native Landing to typed Bronze, Silver and fact processing."""

import hashlib
from datetime import UTC, datetime

import polars as pl
import pyarrow as pa
from pyiceberg.transforms import DayTransform

from lakehouse.logs.parser import InvalidAccessEvent, parse_access_event
from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.oltp.polars.schema import align_arrow, ddl_schema
from lakehouse.oltp.polars.writer import PROPERTIES
from lakehouse.utils.namespaces import namespace as ns

FLAT_FIELDS = {
    "schema_name": "schema.name",
    "schema_version": "schema.version",
    "service_name": "service.name",
    "service_version": "service.version",
    "service_environment": "service.environment",
    "service_instance_id": "service.instance_id",
    **{"event_" + name: "event." + name for name in ("name", "category", "kind", "outcome", "duration_ns")},
    "http_request_method": "http.request_method",
    "http_route": "http.route",
    "http_status_code": "http.status_code",
    "request_id": "request.id",
    "actor_type": "actor.type",
    "actor_key": "actor.key",
    "client_user_agent": "client.user_agent",
    **{
        "ecommerce_" + name: "ecommerce." + name
        for name in ("action", "product_key", "variant_key", "search_query", "search_redacted", "filters")
    },
    "error_code": "error.code",
    "error_type": "error.type",
}


def timestamp(value):
    return datetime.fromisoformat(value).replace(tzinfo=UTC) if value else None


def bronze_row(raw, landed, run_id, source_ref):
    try:
        parsed = parse_access_event(raw)
    except InvalidAccessEvent:
        parsed = None

    def get(name, nullable=False):
        value = getattr(parsed, name, None)
        return None if nullable and value == "" else value

    row = {
        name: get(name, name in {"trace_id", "span_id"})
        for name in ("event_id", "severity_number", "severity_text", "trace_id", "span_id", "data_origin")
    }
    row.update(event_ts=timestamp(get("event_ts")), observed_timestamp=timestamp(get("observed_timestamp")))
    groups = {
        "schema": {"name": "schema_name", "version": "schema_version"},
        "service": {
            "name": "service_name",
            "version": "service_version",
            "environment": "service_env",
            "instance_id": "service_instance_id",
        },
        "event": {
            **{name: "event_" + name for name in ("name", "category", "kind", "outcome")},
            "duration_ns": "duration_ns",
        },
        "http": {"request_method": "http_method", "route": "http_route", "status_code": "http_status"},
        "request": {"id": "event_id"},
        "actor": {"type": "actor_type", "key": "actor_key"},
        "client": {"user_agent": "client_user_agent"},
        "ecommerce": {
            name: name for name in ("action", "product_key", "variant_key", "search_query", "search_redacted")
        },
        "error": {"code": "error_code", "type": "error_type"},
    }
    nullable = {"actor_key", "product_key", "variant_key", "search_query", "error_code", "error_type"}
    for group, names in groups.items():
        row[group] = {key: get(value, value in nullable) for key, value in names.items()}
    row["ecommerce"]["filters"] = None
    row.update(
        _run_id=run_id,
        _source_system="access-log-landing",
        _source_file=source_ref,
        _ingested_at=landed,
        raw_payload=raw,
        _raw_hash=hashlib.sha256(raw.encode()).hexdigest(),
        _validation_error=None if parsed else "invalid_access_event",
    )
    return row


def bronze_delta(landing, bronze, run_id, *, observed, source_ref):
    if landing.filter(pl.col("raw_payload").is_null() | pl.col("ingest_ts").is_null()).height:
        raise ValueError("Landing payload and ingestion timestamp must be present")
    raw = landing.filter(pl.col("ingest_ts") <= observed).group_by("raw_payload").agg(pl.col("ingest_ts").min())
    hashes = set(bronze["_raw_hash"].drop_nulls().to_list())
    rows = [
        bronze_row(r["raw_payload"], r["ingest_ts"], run_id, source_ref)
        for r in raw.to_dicts()
        if hashlib.sha256(r["raw_payload"].encode()).hexdigest() not in hashes
    ]
    return pl.from_arrow(pa.Table.from_pylist(rows, schema=ddl_schema("contracts/logs/bronze_web_events.sql")))


def silver_delta(bronze, silver, *, observed):
    valid = bronze.filter(
        pl.col("_validation_error").is_null() & pl.col("event_id").is_not_null() & (pl.col("event_ts") <= observed)
    )
    conflicts = (
        valid.filter(pl.col("_raw_hash").is_not_null())
        .group_by("event_id")
        .agg(pl.col("_raw_hash").n_unique().alias("versions"))
        .filter(pl.col("versions") > 1)
    )
    if conflicts.height:
        raise ValueError("Conflicting valid raw payloads share an access event ID")
    # Conflicts are checked across all valid Bronze, including future events, before selection.
    valid = (
        valid.sort(["event_id", "_ingested_at", "_raw_hash"], nulls_last=True)
        .unique(subset=["event_id"], keep="first", maintain_order=True)
        .join(silver.select("event_id"), on="event_id", how="anti")
    )
    return valid.select(
        "event_id",
        "event_ts",
        "observed_timestamp",
        "severity_number",
        "severity_text",
        "trace_id",
        "span_id",
        "data_origin",
        *[
            pl.col(path.split(".")[0]).struct.field(path.split(".")[1]).alias(name)
            for name, path in FLAT_FIELDS.items()
        ],
        pl.col("_ingested_at").alias("_silver_ingested_at"),
        pl.col("_run_id").alias("_source_bronze_run_id"),
    )


def fact_delta(silver, facts, run_id, *, observed):
    fresh = silver.filter(pl.col("event_ts") <= observed).join(facts.select("event_id"), on="event_id", how="anti")
    status = pl.col("http_status_code")
    duration = pl.col("event_duration_ns").map_elements(
        lambda value: round(value / 1_000_000.0, 3), return_dtype=pl.Float64
    )
    return fresh.select(
        "event_id",
        "event_ts",
        pl.col("event_ts").dt.date().alias("event_date"),
        "actor_key",
        "actor_type",
        "http_request_method",
        "http_route",
        "http_status_code",
        "ecommerce_action",
        pl.col("ecommerce_product_key").alias("product_key"),
        pl.col("ecommerce_variant_key").alias("variant_key"),
        duration.alias("duration_ms"),
        (status < 400).alias("is_success"),
        status.is_between(400, 499).alias("is_client_error"),
        (status >= 500).alias("is_server_error"),
        (duration >= 1000).alias("is_slow_request"),
        pl.lit(observed).alias("_gold_ingested_at"),
        pl.lit(run_id).alias("_source_run_id"),
    )


def ensure_table(catalog, prefix, layer, name, resource):
    catalog.create_namespace_if_not_exists(ns(prefix, layer))
    schema = ddl_schema(resource)
    table = catalog.create_table_if_not_exists((ns(prefix, layer), name), schema=schema, properties=PROPERTIES)
    if set(schema.names) - set(table.schema().column_names):
        with table.update_schema() as update:
            update.union_by_name(schema)
    if name == "fact_web_events" and not table.spec().fields:
        with table.update_spec() as update:
            update.add_field("event_ts", DayTransform(), "event_day")
    return table


def insert_missing(table, frame, key):
    if frame.height:
        table.upsert(align_arrow(frame.to_arrow(), table.schema()), join_cols=[key], when_matched_update_all=False)
    return capture(table)


def ingest(catalog, prefix, run_id, *, observed):
    landing = catalog.load_table((ns(prefix, "landing"), "access_logs"))
    source = capture(landing)
    bronze = ensure_table(catalog, prefix, "bronze", "web_events", "contracts/logs/bronze_web_events.sql")
    silver = ensure_table(catalog, prefix, "silver", "silver_logs", "contracts/logs/silver_logs.sql")
    fact = ensure_table(catalog, prefix, "gold", "fact_web_events", "contracts/logs/fact_web_events.sql")
    old = read_snapshot(bronze, capture(bronze))
    source_ref = f"lakehouse.{ns(prefix, 'landing')}.access_logs@{source}"
    delta = bronze_delta(read_snapshot(landing, source), old, run_id, observed=observed, source_ref=source_ref)
    bronze_id = insert_missing(bronze, delta, "_raw_hash")
    persisted = read_snapshot(bronze, bronze_id)
    all_valid = persisted.filter(pl.col("_validation_error").is_null() & pl.col("_raw_hash").is_not_null())
    if (
        all_valid.group_by("event_id")
        .agg(pl.col("_raw_hash").n_unique().alias("versions"))
        .filter(pl.col("versions") > 1)
        .height
    ):
        raise ValueError("Conflicting valid raw payloads share an access event ID")
    delta = silver_delta(persisted, read_snapshot(silver, capture(silver)), observed=observed)
    silver_id = insert_missing(silver, delta, "event_id")
    delta = fact_delta(read_snapshot(silver, silver_id), read_snapshot(fact, capture(fact)), run_id, observed=observed)
    fact_id = insert_missing(fact, delta, "event_id")
    return {
        f"lakehouse.{ns(prefix, layer)}.{name}": snapshot
        for layer, name, snapshot in (
            ("landing", "access_logs", source),
            ("bronze", "web_events", bronze_id),
            ("silver", "silver_logs", silver_id),
            ("gold", "fact_web_events", fact_id),
        )
    }
