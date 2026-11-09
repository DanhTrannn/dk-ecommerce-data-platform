from __future__ import annotations

from typing import Any
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import BinaryType, StringType
from dataclasses import dataclass
from .pii import _pseudonymize, _pseudonymized_column_name
from .validation import _validate_rows
from .quarantine import write_quarantine


@dataclass
class MergeResult:
    quarantined: int = 0


def _aligned_column(field, source_schema):
    if field.name not in source_schema.names:
        return F.lit(None).cast(field.dataType).alias(field.name)
    column = F.col(field.name)
    if isinstance(field.dataType, BinaryType) and isinstance(source_schema[field.name].dataType, StringType):
        # PostgreSQL JDBC exposes UUID as text; a binary cast would store UTF-8,
        # not the 16 UUID bytes required by the existing analytical schemas.
        column = F.unhex(F.regexp_replace(column, "-", ""))
    return column.cast(field.dataType).alias(field.name)


def merge_oltp_table(
    spark: SparkSession,
    table: Any,
    bronze_df: DataFrame,
    run_id: str,
    target_path: str,
) -> MergeResult:
    result = MergeResult()

    order_columns = [
        F.col(name).desc()
        for name in (table.cursor_field, "_silver_ingested_at", "_ingested_at_utc")
        if name in bronze_df.columns
    ]
    window = Window.partitionBy(table.pk).orderBy(*(order_columns or [F.lit(1)]))
    deduped = bronze_df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    if table.name == "orders":
        regions = ("Bình Tân", "Quận 1", "Quận 7", "Thủ Đức", "Gò Vấp", "Tân Bình", "Cầu Giấy", "Đống Đa", "Hoàn Kiếm")
        region = F.lit("Khu vực khác")
        for name in reversed(regions):
            region = F.when(F.lower(F.col("shipping_address_text")).contains(name.lower()), F.lit(name)).otherwise(
                region
            )
        deduped = deduped.withColumn("shipping_region", region)

    # Treat a missing primary key as a quarantinable row-level violation.
    if table.pk in deduped.columns:
        deduped = deduped.withColumn(
            "_missing_primary_key",
            F.when(F.col(table.pk).isNull(), F.lit("missing_primary_key")),
        )

    valid_df, violations_df = _validate_rows(deduped, table.name)
    valid_df = valid_df.drop("_missing_primary_key")
    violations_df = violations_df.drop("_missing_primary_key")

    if violations_df.head(1):
        quarantine_count = write_quarantine(
            spark,
            violations_df,
            table.name,
            run_id,
        )
        result.quarantined = quarantine_count

    if table.pseudonymize:
        for col_name in table.pseudonymize:
            if col_name in valid_df.columns:
                valid_df = valid_df.withColumn(
                    _pseudonymized_column_name(col_name),
                    _pseudonymize(F.col(col_name)),
                )
                valid_df = valid_df.drop(col_name)
            else:
                valid_df = valid_df.withColumn(
                    _pseudonymized_column_name(col_name),
                    F.lit(None).cast("string"),
                )
        valid_df = valid_df.withColumn("_pii_pseudonymized_at", F.current_timestamp())
    else:
        valid_df = valid_df.withColumn("_pii_pseudonymized_at", F.lit(None).cast("timestamp"))

    valid_df = valid_df.withColumn("_silver_ingested_at", F.current_timestamp()).withColumn(
        "_source_bronze_run_id", F.lit(run_id)
    )

    from lakehouse.utils.sql import identifier

    target_schema = spark.table(target_path).schema
    aligned = valid_df.select(*[_aligned_column(field, valid_df.schema) for field in target_schema.fields])
    aligned.createOrReplaceTempView("silver_delta")
    key, cursor = identifier(table.pk), identifier(table.cursor_field)
    # A historical backfill must not overwrite a newer source version.
    spark.sql(f"""MERGE INTO {identifier(target_path)} t USING silver_delta s
        ON t.{key} = s.{key}
        WHEN MATCHED AND (t.{cursor} IS NULL OR s.{cursor} >= t.{cursor}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    return result
