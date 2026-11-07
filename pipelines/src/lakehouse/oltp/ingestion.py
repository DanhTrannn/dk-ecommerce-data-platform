"""Staging -> idempotent Bronze history -> incremental Silver."""

import logging
from pyspark.sql import functions as F

from lakehouse.oltp.bronze.writer import merge_source_versions
from lakehouse.oltp.silver import merge_oltp_table
from lakehouse.oltp.silver.schema import ensure_silver_tables

log = logging.getLogger(__name__)


def ingest_bronze_tables(spark, cfg):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    # Never overwrite retained MySQL history by reusing its source PK/timestamp.
    # A deliberate source migration must choose its catalog/storage boundaries.
    for table in cfg.tables:
        target = f"lakehouse.bronze.{table.name}"
        if spark.catalog.tableExists(target):
            existing = spark.table(target)
            if "_source_system" not in existing.columns or existing.filter(
                F.col("_source_system").isNull() | (F.col("_source_system") != "postgresql_ecommerce")
            ).head(1):
                raise ValueError(
                    "Bronze contains legacy or unclassified source data; provision a separate PostgreSQL "
                    "catalog/warehouse or perform an explicit reviewed migration. History was not rewritten."
                )
    for table in cfg.tables:
        target = f"lakehouse.bronze.{table.name}"
        staging = f"s3a://{cfg.bucket}/{cfg.landing.oltp_prefix}/staging/{table.name}"
        for path in sorted(spark.read.parquet(staging).inputFiles()):
            df = spark.read.parquet(path)
            if df.head(1) or not spark.catalog.tableExists(target):
                merge_source_versions(spark, df, target, version_columns=(table.pk, table.cursor_field))
        log.info("Bronze complete: table=%s", table.name)


def ingest_silver_tables(spark, cfg, run_id):
    ensure_silver_tables(spark, tables=[table.silver_table for table in cfg.tables])
    for table in cfg.tables:
        staging = f"s3a://{cfg.bucket}/{cfg.landing.oltp_prefix}/staging/{table.name}"
        for path in sorted(spark.read.parquet(staging).inputFiles()):
            keys = spark.read.parquet(path).select(table.pk, table.cursor_field).alias("staged")
            bronze = spark.table(f"lakehouse.bronze.{table.name}").alias("bronze")
            chunk = bronze.join(
                keys,
                (F.col(f"bronze.{table.pk}") == F.col(f"staged.{table.pk}"))
                & F.col(f"bronze.{table.cursor_field}").eqNullSafe(F.col(f"staged.{table.cursor_field}")),
                "left_semi",
            )
            if chunk.head(1):
                result = merge_oltp_table(spark, table, chunk, run_id, f"lakehouse.silver.{table.silver_table}")
                log.info("Silver complete: table=%s result=%s", table.name, result)
