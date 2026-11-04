"""Single-writer, retryable Landing -> Bronze -> Silver -> fact ingestion."""

import logging

from pyspark.sql import functions as F

from lakehouse.logs.gold import FACT_WEB_EVENTS_TABLE
from lakehouse.logs.medallion import build_bronze_delta, build_silver_delta, build_fact_delta
from lakehouse.utils.sql import load_sql

log = logging.getLogger(__name__)
LANDING = "lakehouse.landing.access_logs"
BRONZE = "lakehouse.bronze.web_events"
SILVER = "lakehouse.silver.silver_logs"


def ensure_log_layers(spark):
    for name in ["bronze", "silver"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{name}")
    for resource in ["bronze_web_events", "silver_logs"]:
        spark.sql(load_sql(f"spark/logs/{resource}.sql"))
    existing = set(spark.table(BRONZE).columns)
    for name in ["raw_payload", "_raw_hash", "_validation_error"]:
        if name not in existing:
            spark.sql(f"ALTER TABLE {BRONZE} ADD COLUMN {name} STRING")


def snapshot_id(spark, table):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'").collect()
    return rows[0].snapshot_id if rows else None


def read_snapshot(spark, table, snapshot):
    return (
        spark.read.option("snapshot-id", str(snapshot)).table(table)
        if snapshot is not None
        else spark.table(table).where("false")
    )


def _insert_missing(spark, frame, target, key):
    delta = frame.select(*spark.table(target).columns).cache()
    try:
        count = delta.count()
        if count:
            delta.createOrReplaceTempView("log_layer_delta")
            spark.sql(f"""MERGE INTO {target} t USING log_layer_delta s ON t.{key}=s.{key}
                WHEN NOT MATCHED THEN INSERT *""")
            if delta.join(spark.table(target).select(key), key, "left_anti").limit(1).count():
                raise ValueError(f"Access-log layer did not retain every delta key: {target}")
        log.info("Access-log layer %s: %s new rows", target, count)
        return count
    finally:
        delta.unpersist()


def ingest_log_layers(spark, run_id, *, as_of):
    """Replay is based on missing raw hashes/IDs, never event-time watermarks.

    Each step reads its persisted predecessor independently, so a retry still
    catches up after a crash between commits. Run only under the log DAG's
    single-active-run boundary; standalone jobs must not overlap it.
    """
    ensure_log_layers(spark)
    source = snapshot_id(spark, LANDING)
    landing = read_snapshot(spark, LANDING, source)
    if landing.filter(F.col("raw_payload").isNull() | F.col("ingest_ts").isNull()).limit(1).count():
        raise ValueError("Landing payload and ingestion timestamp must be present")
    delta = build_bronze_delta(landing, spark.table(BRONZE), run_id, as_of=as_of, source_ref=f"{LANDING}@{source}")
    _insert_missing(spark, delta, BRONZE, "_raw_hash")
    bronze_snapshot = snapshot_id(spark, BRONZE)
    bronze = read_snapshot(spark, BRONZE, bronze_snapshot)
    # An event ID is immutable. Do not silently pick a payload when the
    # producer has reused an ID for two different valid events.
    conflicts = (
        bronze.filter("_raw_hash IS NOT NULL AND _validation_error IS NULL")
        .groupBy("event_id")
        .agg(F.countDistinct("_raw_hash").alias("versions"))
        .filter("versions>1")
    )
    if conflicts.limit(1).count():
        raise ValueError("Conflicting valid raw payloads share an access event ID")
    _insert_missing(spark, build_silver_delta(bronze, spark.table(SILVER), as_of=as_of), SILVER, "event_id")
    silver_snapshot = snapshot_id(spark, SILVER)
    silver = read_snapshot(spark, SILVER, silver_snapshot)
    _insert_missing(
        spark,
        build_fact_delta(silver, spark.table(FACT_WEB_EVENTS_TABLE), run_id, as_of=as_of),
        FACT_WEB_EVENTS_TABLE,
        "event_id",
    )
    rejected = bronze.filter("_validation_error IS NOT NULL").count()
    log.info(
        "Access logs processed from Landing snapshot %s; rejected raw payloads retained in Bronze: %s", source, rejected
    )
    return {LANDING: source, BRONZE: bronze_snapshot, SILVER: silver_snapshot}
