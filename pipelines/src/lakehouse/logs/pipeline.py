"""Build a complete, validated log-mart snapshot set from pinned inputs."""

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

from lakehouse.logs.gold import (
    FACT_WEB_EVENTS_TABLE,
    MART_DAILY_PRODUCT_DEMAND_TABLE,
    MART_HOURLY_ROUTE_METRICS_TABLE,
    MART_MARKETING_FUNNEL_DAILY_TABLE,
    build_mart_daily_product_demand,
    build_mart_hourly_route_metrics,
    build_mart_marketing_funnel_daily,
    ensure_logs_gold_tables,
)
from lakehouse.logs.product_identity import resolve_product_events
from lakehouse.logs.publication import publish_logs


def run_logs_marts(spark, run_id):
    ensure_logs_gold_tables(spark)
    observed = datetime.now(timezone.utc).replace(tzinfo=None)
    refs = spark.sql(f"SELECT snapshot_id FROM {FACT_WEB_EVENTS_TABLE}.refs WHERE name='main'").collect()
    if refs:
        events = spark.read.option("snapshot-id", str(refs[0].snapshot_id)).table(FACT_WEB_EVENTS_TABLE)
    else:
        events = spark.table(FACT_WEB_EVENTS_TABLE).where("false")
    events = events.filter(F.col("event_ts") <= F.lit(observed)).cache()
    try:
        shape = events.agg(F.count("*").alias("n"), F.countDistinct("event_id").alias("ids")).first()
        if shape.n != shape.ids or events.filter(F.col("event_date").isNull()).limit(1).count():
            raise ValueError("Log event IDs must be unique/non-null, with a valid event date")
        publication = spark.sql("""SELECT snapshots_json FROM lakehouse.system.analytics_publications
            ORDER BY published_at DESC LIMIT 1""").first()
        if not publication:
            raise ValueError("Publish the OLTP catalog before resolving product identities")
        source_snapshots = json.loads(publication.snapshots_json)

        def catalog(table):
            target = f"lakehouse.silver.silver_{table}"
            snapshot = source_snapshots[target]
            return (
                spark.read.option("snapshot-id", str(snapshot)).table(target)
                if snapshot
                else spark.table(target).where("false")
            )

        products, variants = catalog("products"), catalog("product_variants")
        frames = [
            (MART_HOURLY_ROUTE_METRICS_TABLE, build_mart_hourly_route_metrics(events, run_id)),
            (
                MART_DAILY_PRODUCT_DEMAND_TABLE,
                build_mart_daily_product_demand(events, run_id, products=products, variants=variants),
            ),
            (MART_MARKETING_FUNNEL_DAILY_TABLE, build_mart_marketing_funnel_daily(events, run_id)),
        ]
        for target, frame in frames:
            frame.select(*spark.table(target).columns).writeTo(target).overwrite(F.lit(True))
        # Full replacement also clears stale partitions on an empty source.
        # Publish only after validating every output against these same inputs.
        for target, expected in frames:
            columns = [c for c in expected.columns if c not in {"_gold_ingested_at", "_source_run_id"}]
            actual = spark.table(target).select(*columns)
            expected = expected.select(*columns)
            if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
                raise ValueError(f"Log mart parity failed: {target}")
        resolved = resolve_product_events(events, products, variants)
        if resolved.count() != shape.n:
            raise ValueError("Catalog identity mapping multiplied log events")
        summary = events.agg(F.max("event_ts").alias("latest")).first()
        inputs = {FACT_WEB_EVENTS_TABLE: refs[0].snapshot_id if refs else None}
        inputs.update(
            {
                k: source_snapshots[k]
                for k in ["lakehouse.silver.silver_products", "lakehouse.silver.silver_product_variants"]
            }
        )
        publish_logs(
            spark,
            run_id,
            [target for target, _ in frames],
            observed_at=observed,
            source_at=summary.latest,
            source_snapshots=inputs,
        )
        print(
            f"[{run_id}] Published three log marts from {shape.n} events; OLTP publication/offsets retained",
            flush=True,
        )
    finally:
        events.unpersist()
