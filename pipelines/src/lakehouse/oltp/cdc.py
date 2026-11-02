"""Apply Debezium row images to typed Silver with durable per-key ordering.

The Bronze log remains immutable. Deletes remove Silver rows while a separate
watermark prevents an older replay from resurrecting them. All writes can be
retried before the publication checkpoint advances.
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from lakehouse.oltp.silver import _pseudonymize, _pseudonymized_column_name, _validate_rows, write_quarantine

STATE = "lakehouse.system.cdc_row_state"
REGIONS = ("Bình Tân", "Quận 1", "Quận 7", "Thủ Đức", "Gò Vấp", "Tân Bình", "Cầu Giấy", "Đống Đa", "Hoàn Kiếm")


def ensure_cdc_state(spark):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {STATE} (
        table_name STRING, row_key STRING, file_number BIGINT,
        binlog_position BIGINT, binlog_row INT, kafka_offset BIGINT,
        deleted BOOLEAN, applied_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (table_name)
    TBLPROPERTIES ('format-version'='2')""")


def source_columns(path: str | Path) -> dict[str, dict[str, str]]:
    sql = Path(path).read_text()
    return {
        name: dict(re.findall(r"^  `([^`]+)` ([a-z]+(?:\(\d+(?:,\d+)?\))?)", body, re.M))
        for name, body in re.findall(r"CREATE TABLE `([^`]+)` \(\n(.*?)\n\) ENGINE=", sql, re.S)
    }


def region_column(address):
    result = F.lit("Khu vực khác")
    for region in reversed(REGIONS):
        result = F.when(F.lower(address).contains(region.lower()), F.lit(region)).otherwise(result)
    return result


def decode_rows(events, columns):
    raw_schema = StructType([StructField(name, StringType()) for name in columns])
    rows = (
        events.withColumn(
            "_image",
            F.from_json(
                F.when(F.col("operation") == "d", F.col("before_payload")).otherwise(F.col("after_payload")),
                raw_schema,
            ),
        )
        .select("*", "_image.*")
        .drop("_image")
    )
    for name, kind in columns.items():
        value = F.col(name)
        if kind.startswith(("datetime", "timestamp")):
            value = F.when(value.rlike(r"^-?\d+$"), F.expr(f"timestamp_micros(CAST(`{name}` AS BIGINT))")).otherwise(
                value.cast("timestamp")
            )
        elif kind == "date":
            value = F.when(
                value.rlike(r"^-?\d+$"), F.date_add(F.lit("1970-01-01").cast("date"), value.cast("int"))
            ).otherwise(value.cast("date"))
        elif kind.startswith("tinyint(1)"):
            value = F.when(value.isNull(), F.lit(None).cast("boolean")).otherwise(F.lower(value).isin("true", "1"))
        elif kind.startswith(("bigint", "int", "smallint", "tinyint")):
            value = value.cast("long")
        elif kind.startswith(("binary", "varbinary")):
            value = F.unbase64(value)
        elif kind.startswith(("decimal", "double", "float")):
            value = value.cast(kind)
        rows = rows.withColumn(name, value)
    return rows


def curate_rows(rows, table):
    if table.name == "orders":
        rows = rows.withColumn("shipping_region", region_column(F.col("shipping_address_text")))
    for column in table.pseudonymize:
        if column in rows.columns:
            rows = rows.withColumn(_pseudonymized_column_name(column), _pseudonymize(F.col(column))).drop(column)
    return rows


def apply_table(spark, table, events, columns, run_id):
    target = f"lakehouse.silver.{table.silver_table}"
    rows = decode_rows(events.withColumnRenamed("transaction_id", "_transaction_id"), columns)
    # Full before images are required for deletes, including the unique cursor
    # key on store_inventory, whose physical PK is composite.
    rows = rows.withColumn("row_key", F.col(table.pk).cast("string"))
    if rows.filter(F.col("row_key").isNull()).limit(1).count():
        raise ValueError(f"CDC {table.name}: missing source identity")
    rows = rows.withColumn("file_number", F.regexp_extract("binlog_file", r"(\d+)$", 1).cast("long"))
    for column in ("file_number", "binlog_position", "binlog_row", "kafka_offset"):
        rows = rows.withColumn(column, F.coalesce(F.col(column), F.lit(0)))
    ordering = F.struct("file_number", "binlog_position", "binlog_row", "kafka_offset")
    window = Window.partitionBy("row_key").orderBy(ordering.desc())
    rows = rows.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")
    previous = (
        spark.table(STATE)
        .filter(F.col("table_name") == table.name)
        .select("row_key", F.struct("file_number", "binlog_position", "binlog_row", "kafka_offset").alias("_previous"))
    )
    rows = (
        rows.join(previous, "row_key", "left")
        .filter(F.col("_previous").isNull() | (ordering > F.col("_previous")))
        .drop("_previous")
    )
    rows = rows.persist()
    try:
        count = rows.count()
        if not count:
            return 0
        upserts = rows.filter(F.col("operation") != "d").withColumn(
            "_missing_primary_key", F.when(F.col(table.pk).isNull(), F.lit("missing_primary_key"))
        )
        valid, invalid = _validate_rows(upserts, table.name)
        if invalid.limit(1).count():
            write_quarantine(spark, invalid, table.name, run_id, target + "_quarantine")
            raise ValueError(f"CDC {table.name}: invalid records quarantined; checkpoint not advanced")
        valid = curate_rows(valid, table)
        valid = (
            valid.withColumn("_silver_ingested_at", F.current_timestamp())
            .withColumn("_source_bronze_run_id", F.lit(run_id))
            .withColumn(
                "_pii_pseudonymized_at", F.current_timestamp() if table.pseudonymize else F.lit(None).cast("timestamp")
            )
        )
        target_schema = spark.table(target).schema
        aligned = valid.select(
            *[
                F.col(field.name).cast(field.dataType).alias(field.name)
                if field.name in valid.columns
                else F.lit(None).cast(field.dataType).alias(field.name)
                for field in target_schema.fields
            ]
        ).withColumn("_delete", F.lit(False))
        deletes = (
            rows.filter(F.col("operation") == "d")
            .select(
                *[
                    F.col(field.name).cast(field.dataType).alias(field.name)
                    if field.name == table.pk
                    else F.lit(None).cast(field.dataType).alias(field.name)
                    for field in target_schema.fields
                ]
            )
            .withColumn("_delete", F.lit(True))
        )
        aligned.unionByName(deletes).createOrReplaceTempView("cdc_changes")
        names = [field.name for field in target_schema.fields]
        updates = ", ".join(f"t.`{name}` = s.`{name}`" for name in names)
        inserts = ", ".join(f"`{name}`" for name in names)
        values = ", ".join(f"s.`{name}`" for name in names)
        spark.sql(f"""MERGE INTO {target} t USING cdc_changes s ON t.`{table.pk}` = s.`{table.pk}`
            WHEN MATCHED AND s._delete THEN DELETE
            WHEN MATCHED THEN UPDATE SET {updates}
            WHEN NOT MATCHED AND NOT s._delete THEN INSERT ({inserts}) VALUES ({values})""")
        state = rows.select(
            F.lit(table.name).alias("table_name"),
            "row_key",
            "file_number",
            "binlog_position",
            "binlog_row",
            "kafka_offset",
            (F.col("operation") == "d").alias("deleted"),
            F.current_timestamp().alias("applied_at"),
        )
        state.createOrReplaceTempView("cdc_new_state")
        spark.sql(f"""MERGE INTO {STATE} t USING cdc_new_state s
            ON t.table_name=s.table_name AND t.row_key=s.row_key
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
        return count
    finally:
        rows.unpersist()
