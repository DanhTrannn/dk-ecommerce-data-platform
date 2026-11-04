from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from .pii import _redact_quarantine_pii


def write_quarantine(
    spark: SparkSession,
    violations_df: DataFrame,
    source_table: str,
    run_id: str,
    target_path: str | None = None,
    _write_format: str = "iceberg",
) -> int:
    safe_violations_df = _redact_quarantine_pii(violations_df)
    quarantined = safe_violations_df.select(
        F.to_json(
            F.struct(*[F.col(name) for name in sorted(safe_violations_df.columns) if not name.startswith("_")])
        ).alias("record_data"),
        F.col("_violation_type").alias("violation_type"),
        F.col("_violation_types").alias("violation_types"),
        F.lit(f"Business rule violation in {source_table}").alias("violation_detail"),
        F.lit(source_table).alias("source_table"),
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("_quarantined_at"),
    )

    count = quarantined.count()
    if count == 0:
        return 0

    if _write_format == "iceberg":
        quarantined.createOrReplaceTempView("quarantine_delta")
        spark.sql("""MERGE INTO lakehouse.quarantine.silver_oltp_violations t USING quarantine_delta s
            ON t.source_table = s.source_table AND t.record_data = s.record_data
                AND t.violation_type <=> s.violation_type
            WHEN NOT MATCHED THEN INSERT *""")
    else:
        quarantined.write.format(_write_format).mode("append").save(target_path)

    return count
