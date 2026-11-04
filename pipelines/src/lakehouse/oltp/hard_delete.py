"""Delete Silver cart/wishlist rows selected from CDC soft-delete evidence."""

import logging
from datetime import timedelta, timezone
from urllib.parse import urlsplit

from pyspark.sql import functions as F

from lakehouse.utils.config import HARD_DELETE_TABLES

log = logging.getLogger(__name__)


def sync_hard_deletes(spark, cfg, as_of):
    if as_of.tzinfo is None:
        raise ValueError("Observation time must include a timezone")
    cutoff = as_of.astimezone(timezone.utc) - timedelta(days=30)
    database = urlsplit(cfg.jdbc_url.removeprefix("jdbc:")).path.strip("/")
    audit = spark.table("lakehouse.bronze.oltp_cdc_events")
    for name in HARD_DELETE_TABLES:
        table = cfg.table(name)
        keys = (
            audit.filter(
                (F.col("operation") == "d")
                & (F.col("database_name") == database)
                & (F.col("table_name") == name)
                & F.lower(F.get_json_object("before_payload", "$.is_present")).isin("0", "false")
                & F.get_json_object("before_payload", "$.removed_at").isNotNull()
            )
            .filter(F.col("source_ts") <= F.lit(cutoff))
            .select(
                F.coalesce(
                    F.get_json_object("before_payload", f"$.{table.pk}"),
                    F.get_json_object("record_key", f"$.{table.pk}"),
                ).alias("primary_key")
            )
            .distinct()
        )
        target = f"lakehouse.silver.{table.silver_table}"
        deleted_rows = (
            spark.table(target).join(keys, F.col(table.pk).cast("string") == keys["primary_key"], "left_semi").count()
        )
        keys.createOrReplaceTempView("hard_delete_keys")
        spark.sql(f"""MERGE INTO {target} t USING hard_delete_keys s
            ON CAST(t.{table.pk} AS STRING) = s.primary_key
            WHEN MATCHED THEN DELETE""")
        log.info("HARD_DELETE table=%s deleted_rows=%d cutoff=%s", target, deleted_rows, cutoff.isoformat())
