from __future__ import annotations

from lakehouse.sql import identifier, render_sql, sql_type

from pyspark.sql.functions import col, lit


def merge_source_versions(spark, df, target_table: str, *, version_columns) -> None:
    """Upsert one source chunk while preserving earlier source versions."""
    if spark.catalog.tableExists(target_table):
        # MERGE resolves an explicit target schema; new columns are added below.
        spark.sql(f"ALTER TABLE {target_table} SET TBLPROPERTIES ('write.spark.accept-any-schema'='false')")

        target_schema = spark.table(target_table).schema
        target_names = {field.name for field in target_schema.fields}
        for field in df.schema.fields:
            if field.name not in target_names:
                column_name = field.name.replace("`", "``")
                spark.sql(
                    render_sql(
                        "spark/bronze/add_column.sql",
                        target_table=identifier(target_table),
                        column_name=identifier(column_name),
                        field_datatype_simplestring=sql_type(field.dataType.simpleString()),
                    )
                )

        # Iceberg validates physical field ordering even for V2 writes. Align
        # to the evolved table and backfill a typed NULL when a source column
        # has been retired.
        target_schema = spark.table(target_table).schema
        source_names = set(df.columns)
        aligned_df = df.select(
            *[
                col(f"`{field.name.replace('`', '``')}`")
                if field.name in source_names
                else lit(None).cast(field.dataType).alias(field.name)
                for field in target_schema.fields
            ]
        )
        aligned_df.createOrReplaceTempView("bronze_delta")
        match = " AND ".join(f"t.{column} <=> s.{column}" for column in version_columns)
        spark.sql(f"""MERGE INTO {target_table} t USING bronze_delta s ON {match}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        return

    (
        df.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.spark.accept-any-schema", "false")
        .create()
    )
