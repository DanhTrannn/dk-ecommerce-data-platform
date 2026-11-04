from __future__ import annotations

from typing import Any
from pyspark.sql import DataFrame, SparkSession, functions as F
from dataclasses import dataclass
from .dedup import _dedup_by_pk, _add_silver_metadata
from .pii import _pseudonymize, _pseudonymized_column_name
from .validation import _validate_rows
from .quarantine import write_quarantine


@dataclass
class MergeResult:
    quarantined: int = 0


def merge_oltp_table(
    spark: SparkSession,
    table: Any,
    bronze_df: DataFrame,
    run_id: str,
    target_path: str,
    _write_format: str = "iceberg",
) -> MergeResult:
    result = MergeResult()

    deduped = _dedup_by_pk(bronze_df, table.pk, table.cursor_field)
    if table.name == "orders":
        from lakehouse.oltp.region import region_column

        deduped = deduped.withColumn("shipping_region", region_column(F.col("shipping_address_text")))

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
            target_path=f"{target_path}_quarantine",
            _write_format=_write_format,
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

    valid_df = _add_silver_metadata(valid_df, run_id)

    if _write_format == "iceberg":
        from lakehouse.sql import identifier

        target_schema = spark.table(target_path).schema
        aligned = valid_df.select(
            *[
                F.col(field.name).cast(field.dataType).alias(field.name)
                if field.name in valid_df.columns
                else F.lit(None).cast(field.dataType).alias(field.name)
                for field in target_schema.fields
            ]
        )
        aligned.createOrReplaceTempView("silver_delta")
        key, cursor = identifier(table.pk), identifier(table.cursor_field)
        # A historical backfill must not overwrite a newer source version.
        spark.sql(f"""MERGE INTO {identifier(target_path)} t USING silver_delta s
            ON t.{key} = s.{key}
            WHEN MATCHED AND (t.{cursor} IS NULL OR s.{cursor} >= t.{cursor}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        return result

    raise ValueError("Silver writes require Iceberg")
