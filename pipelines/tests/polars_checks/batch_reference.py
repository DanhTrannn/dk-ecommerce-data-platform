"""Frozen Spark behavior in a private stack, for all-source native migration parity."""

import json
import sys

from pyspark.sql import functions as F

from batch_contracts import FAULTS, PREFIX, fingerprint
from lakehouse.oltp.extract import run_extract, run_initial_extract
from lakehouse.oltp.ingestion import ingest_bronze_tables, ingest_silver_tables
from lakehouse.oltp.silver.writer import merge_oltp_table
from lakehouse.utils.config import load_config
from lakehouse.utils.spark import spark_session

spark = spark_session("native_batch_reference")
spark.sparkContext.setLogLevel("ERROR")
try:
    mode = sys.argv[1]
    config = load_config()
    if mode in ("initial", "interval"):
        if mode == "initial":
            run_initial_extract(spark, config)
        else:
            run_extract(spark, config, sys.argv[2], sys.argv[3])
        ingest_bronze_tables(spark, config)
        ingest_silver_tables(spark, config, "reference_" + mode)
    elif mode == "faults":
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{PREFIX}_reference")
        for table in config.tables:
            if table.name not in FAULTS:
                continue
            source = (
                spark.table(f"lakehouse.bronze.{table.name}")
                .orderBy(table.pk, F.col(table.cursor_field).desc())
                .limit(1)
            )
            if not source.count():
                source = spark.createDataFrame([tuple(None for _ in source.schema)], source.schema)
            changes = {table.pk: 9000000, **FAULTS[table.name]}
            for field in source.schema:
                if field.name in changes:
                    source = source.withColumn(field.name, F.lit(changes[field.name]).cast(field.dataType))
            # Both engines must receive the same sampled row, not two lazy LIMIT reads.
            source.persist()
            source.count()
            try:
                target = f"lakehouse.{PREFIX}_reference.fault_{table.name}"
                source.writeTo(target).using("iceberg").tableProperty("format-version", "2").create()
                merge_oltp_table(spark, table, source, "reference_faults", f"lakehouse.silver.{table.silver_table}")
            finally:
                source.unpersist()
    result = {}
    for table in config.tables:
        frame = spark.table(f"lakehouse.silver.{table.silver_table}")
        result[table.name] = fingerprint(frame.collect(), frame.columns)
        bronze = spark.table(f"lakehouse.bronze.{table.name}")
        result["bronze." + table.name] = fingerprint(bronze.collect(), bronze.columns)
    rejected = spark.table("lakehouse.quarantine.silver_oltp_violations")
    result["quarantine"] = fingerprint(rejected.collect(), rejected.columns)
    print("STACK_RESULT " + json.dumps(result), flush=True)
finally:
    spark.stop()
