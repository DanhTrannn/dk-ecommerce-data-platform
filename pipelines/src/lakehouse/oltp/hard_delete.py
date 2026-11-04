"""Synchronize explicitly selected CDC deletes without removing source or history."""

from datetime import timedelta, timezone
from urllib.parse import urlsplit

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lakehouse.config import HARD_DELETE_TABLES
from lakehouse.sql import load_sql

DELETIONS = "lakehouse.system.oltp_hard_deletes"


def sync_hard_deletes(spark, cfg, tables, run_id, as_of, *, apply=False):
    if not tables or len(tables) != len(set(tables)) or set(tables) - set(HARD_DELETE_TABLES):
        raise ValueError(f"Select distinct tables explicitly from {HARD_DELETE_TABLES}")
    if as_of.tzinfo is None:
        raise ValueError("Observation time must include a timezone")
    cutoff = as_of.astimezone(timezone.utc) - timedelta(days=30)
    jdbc_url = cfg.jdbc_url
    database = urlsplit(jdbc_url.removeprefix("jdbc:")).path.strip("/")
    audit = spark.table("lakehouse.bronze.oltp_cdc_events")
    if apply:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.system")
        spark.sql(load_sql("spark/system/oltp_hard_deletes.sql"))

    total = 0
    for name in HARD_DELETE_TABLES:
        if name not in tables:
            continue
        table = cfg.table(name)
        events = audit.filter(
            (F.col("operation") == "d") & (F.col("database_name") == database) & (F.col("table_name") == name)
        )
        if name in ("cart_items", "wishlist_items"):
            soft_deleted = (
                F.lower(F.get_json_object("before_payload", "$.is_present")).isin("0", "false")
                & F.get_json_object("before_payload", "$.removed_at").isNotNull()
            )
        else:
            soft_deleted = F.get_json_object("before_payload", "$.archived_at").isNotNull()
        events = (
            events.filter(soft_deleted)
            .withColumn(
                "primary_key",
                F.coalesce(
                    F.get_json_object("before_payload", f"$.{table.pk}"),
                    F.get_json_object("record_key", f"$.{table.pk}"),
                ),
            )
            .withColumn("_updated_raw", F.get_json_object("before_payload", "$.updated_at"))
            .withColumn(
                "source_updated_at",
                F.coalesce(
                    F.expr("timestamp_micros(try_cast(_updated_raw as bigint))"),
                    F.to_timestamp("_updated_raw"),
                ),
            )
            .filter(F.col("source_updated_at") <= F.lit(cutoff))
        )
        if events.filter(F.col("primary_key").isNull() | ~F.col("primary_key").rlike("^[0-9]+$")).head(1):
            raise ValueError(f"CDC delete for {name} has an invalid primary key")
        # One deletion decision per key. Older CDC replay events do not create new decisions.
        latest = Window.partitionBy("primary_key").orderBy(F.col("event_ts").desc(), F.col("kafka_offset").desc())
        pending = (
            events.withColumn("_rn", F.row_number().over(latest))
            .filter(F.col("_rn") == 1)
            .select("table_name", "primary_key", "event_id", "source_updated_at", F.col("event_ts").alias("deleted_at"))
        )
        if spark.catalog.tableExists(DELETIONS):
            applied = spark.table(DELETIONS).filter((F.col("table_name") == name) & F.col("applied_at").isNotNull())
            pending = pending.join(applied.select("primary_key"), "primary_key", "left_anti")

        last_key = None
        table_count = 0
        while True:
            page = pending if last_key is None else pending.filter(F.col("primary_key") > last_key)
            page = page.orderBy("primary_key").limit(10_000).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                keys = [row.primary_key for row in page.select("primary_key").collect()]
                if not keys:
                    break
                key_list = ",".join(str(int(key)) for key in keys)
                # A historical delete must never remove a key that exists in source again.
                source = (
                    spark.read.format("jdbc")
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .option("url", jdbc_url)
                    .option("query", f"SELECT {table.pk} FROM {name} WHERE {table.pk} IN ({key_list})")
                    .load()
                )
                if source.head(1):
                    raise ValueError(f"Delete keys still exist in source: {name}; source is never purged by this DAG")

                references = ()
                if name == "products":
                    references = (
                        ("lakehouse.silver.silver_product_variants", "product_id"),
                        ("lakehouse.silver.silver_product_reviews", "product_id"),
                        ("lakehouse.silver.silver_wishlist_items", "product_id"),
                        ("lakehouse.gold.fact_order_item", "product_key"),
                    )
                elif name == "coupons":
                    references = (
                        ("lakehouse.silver.silver_orders", "coupon_id"),
                        ("lakehouse.silver.silver_coupon_redemptions", "coupon_id"),
                        ("lakehouse.gold.fact_order", "coupon_id"),
                    )
                for target, column in references:
                    if spark.catalog.tableExists(target) and spark.table(target).filter(
                        F.col(column).cast("string").isin(keys)
                    ).head(1):
                        raise ValueError(f"Cannot delete {name}: keys are referenced by {target}")

                silver = f"lakehouse.silver.{table.silver_table}"
                if spark.catalog.tableExists(silver):
                    newer = (
                        spark.table(silver)
                        .select(
                            F.col(table.pk).cast("string").alias("primary_key"),
                            F.col(table.cursor_field).alias("current_updated_at"),
                        )
                        .join(page.select("primary_key", "source_updated_at"), "primary_key")
                    )
                    if newer.filter(F.col("current_updated_at") > F.col("source_updated_at")).head(1):
                        raise ValueError(f"Cannot delete a newer Silver version: {name}")

                if apply:
                    page.createOrReplaceTempView("hard_delete_pending")
                    existing = spark.table(DELETIONS).filter(F.col("table_name") == name).select("primary_key")
                    decisions = (
                        page.join(existing, "primary_key", "left_anti")
                        .withColumn("requested_run_id", F.lit(run_id))
                        .withColumn("recorded_at", F.current_timestamp())
                        .withColumn("applied_at", F.lit(None).cast("timestamp"))
                    )
                    if decisions.head(1):
                        decisions.select(*spark.table(DELETIONS).columns).writeTo(DELETIONS).append()
                    # Record the decision first, so an interrupted sync cannot be undone by a batch replay.
                    if spark.catalog.tableExists(silver):
                        spark.sql(f"""MERGE INTO {silver} t USING hard_delete_pending s
                            ON CAST(t.{table.pk} AS STRING) = s.primary_key
                            WHEN MATCHED THEN DELETE""")
                    spark.sql(f"""MERGE INTO {DELETIONS} t USING hard_delete_pending s
                        ON t.table_name = s.table_name AND t.primary_key = s.primary_key
                        WHEN MATCHED THEN UPDATE SET applied_at = current_timestamp()""")
                table_count += len(keys)
                last_key = keys[-1]
            finally:
                page.unpersist()
        total += table_count
        print(f"HARD_DELETE table={name} rows={table_count} cutoff={cutoff.isoformat()} apply={apply}", flush=True)
    return total
