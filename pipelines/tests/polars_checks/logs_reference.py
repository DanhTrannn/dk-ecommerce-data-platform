"""Actual Spark medallion/rollup reference, using a private DLQ transport fixture."""

import json
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

from gold_contracts import gold_fingerprint
from lakehouse.logs.gold import (
    FACT_WEB_EVENTS_TABLE,
    build_mart_daily_product_demand,
    build_mart_hourly_route_metrics,
    build_mart_marketing_funnel_daily,
    ensure_logs_gold_tables,
)
from lakehouse.logs.ingestion import BRONZE, SILVER, ensure_log_layers
from lakehouse.logs.medallion import build_bronze_delta, build_silver_delta, build_fact_delta
from lakehouse.utils.spark import spark_session

PREFIX = "polars_batch_acceptance"
mode, run_id, as_of = sys.argv[1:4]
raws = json.loads(Path("/native-log-events.json").read_text()) if mode == "initial" else []
observed = datetime.fromisoformat(as_of).replace(tzinfo=None)
spark = spark_session("native_logs_reference")
spark.sparkContext.setLogLevel("ERROR")
try:
    ensure_logs_gold_tables(spark)
    if mode == "initial":
        ensure_log_layers(spark)
        # Raw replay and one invalid envelope test the existing Spark predecessor.
        landing = spark.createDataFrame(
            [(raw,) for raw in [*raws, raws[0], "invalid-native-log-fixture"]], ["raw_payload"]
        ).withColumn("ingest_ts", F.lit(observed))
        build_bronze_delta(
            landing, spark.table(BRONZE), run_id, as_of=observed, source_ref="fixture-dlq-handoff"
        ).writeTo(BRONZE).append()
        build_silver_delta(spark.table(BRONZE), spark.table(SILVER), as_of=observed).writeTo(SILVER).append()
        build_fact_delta(spark.table(SILVER), spark.table(FACT_WEB_EVENTS_TABLE), run_id, as_of=observed).writeTo(
            FACT_WEB_EVENTS_TABLE
        ).append()
    publication = (
        spark.table(f"lakehouse.{PREFIX}_system.analytics_publications")
        .orderBy("published_at", ascending=False)
        .first()
    )
    manifest = json.loads(publication.snapshots_json)

    def pinned(name):
        physical = f"lakehouse.{PREFIX}_silver.{name}"
        return spark.read.option("snapshot-id", str(manifest[physical])).table(physical)

    products, variants = pinned("silver_products"), pinned("silver_product_variants")
    events = spark.table(FACT_WEB_EVENTS_TABLE).filter(f"event_ts <= TIMESTAMP '{observed}'")
    frames = [
        ("mart_hourly_route_metrics", build_mart_hourly_route_metrics(events, run_id)),
        (
            "mart_daily_product_demand",
            build_mart_daily_product_demand(events, run_id, products=products, variants=variants),
        ),
        ("mart_marketing_funnel_daily", build_mart_marketing_funnel_daily(events, run_id)),
    ]
    output = {}
    for name, frame in frames:
        frame.select(*spark.table("lakehouse.gold." + name).columns).writeTo("lakehouse.gold." + name).overwrite(
            F.lit(True)
        )
        frame = spark.table("lakehouse.gold." + name)
        output[name] = gold_fingerprint(frame.collect(), frame.columns)
    rejected = spark.table(BRONZE).filter("_validation_error IS NOT NULL").count()
    print("STACK_RESULT " + json.dumps(dict(fingerprints=output, events=events.count(), rejected=rejected)), flush=True)
finally:
    spark.stop()
