"""Container-side real PostgreSQL/S3 staging and local Iceberg integration checks."""

import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import patch

from pyspark.sql import SparkSession

from lakehouse.oltp.extract import run_extract, run_initial_extract
from lakehouse.oltp.ingestion import ingest_bronze_tables, ingest_silver_tables
from lakehouse.oltp.quality.reconcile import run_reconciliation_gate
from lakehouse.utils.config import load_config
from lakehouse.utils.postgres import jdbc_options
from lakehouse.utils.spark import configure_s3a


def main():
    # A local test catalog deliberately avoids external Polaris identities or volumes.
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("postgres_batch_check")
        .config("spark.jars", "/opt/spark/jars/postgresql.jar")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "file:///tmp/batch-warehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = (
        configure_s3a(builder)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    jvm = spark._jvm
    s3 = (
        jvm.com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
        .withCredentials(
            jvm.com.amazonaws.auth.AWSStaticCredentialsProvider(
                jvm.com.amazonaws.auth.BasicAWSCredentials(
                    os.environ["MINIO_SPARK_ACCESS_KEY"], os.environ["MINIO_SPARK_SECRET_KEY"]
                )
            )
        )
        .withEndpointConfiguration(
            jvm.com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(
                os.environ["MINIO_ENDPOINT"], "us-east-1"
            )
        )
        .withPathStyleAccessEnabled(True)
        .build()
    )
    s3.createBucket("batch-check")
    s3.shutdown()
    cfg = load_config("/opt/project/pipelines/config/default.yml")
    try:

        def read(query):
            return spark.read.format("jdbc").options(**jdbc_options()).option("query", query).load()

        # Read-only is a server setting, not merely a client intent.
        assert read("SELECT current_setting('transaction_read_only') AS value").first()["value"] == "on"
        assert (
            read("SELECT has_table_privilege(current_user, 'public.customer_credentials', 'SELECT') AS value").first()[
                "value"
            ]
            is False
        )
        for table in cfg.tables:
            for action in ("INSERT", "UPDATE", "DELETE"):
                assert (
                    read(
                        f"SELECT has_table_privilege(current_user, 'public.{table.name}', '{action}') AS value"
                    ).first()["value"]
                    is False
                )
        from lakehouse.oltp.silver.validation import _validate_rows

        valid, invalid = _validate_rows(
            spark.createDataFrame([("order_cancellation",), ("unsupported",)], ["movement_type"]),
            "inventory_transactions",
        )
        assert valid.count() == invalid.count() == 1
        assert valid.first()["movement_type"] == "order_cancellation"
        orders = replace(cfg, tables=(cfg.table("orders"),))
        with patch("lakehouse.oltp.extract.CHUNK_ROWS", 7):
            run_initial_extract(spark, orders)
        staged_orders = spark.read.parquet("s3a://batch-check/landing/oltp/staging/orders")
        assert staged_orders.count() == staged_orders.select("order_id").distinct().count() == 30
        assert len(staged_orders.inputFiles()) == 5
        assert staged_orders.select("_source_system").distinct().first()[0] == "postgresql_ecommerce"
        assert staged_orders.select("_source_schema").distinct().first()[0] == "public"
        assert staged_orders.schema["created_at"].dataType.simpleString() == "timestamp"
        run_initial_extract(spark, cfg)
        ingest_bronze_tables(spark, cfg)
        ingest_silver_tables(spark, cfg, "pg-initial")
        summary = run_reconciliation_gate(spark, cfg, tolerance_pct=0)
        assert summary["passed_checks"] == 30, summary
        # Replaying an existing delta must not duplicate source history or Silver.
        bronze_count = spark.table("lakehouse.bronze.orders").count()
        ingest_bronze_tables(spark, orders)
        ingest_silver_tables(spark, orders, "pg-replay")
        assert spark.table("lakehouse.bronze.orders").count() == bronze_count
        assert spark.table("lakehouse.silver.silver_orders").count() == 30
        # Source fixtures include lower/inside/upper boundaries; non-UTC input is equivalent.
        categories = replace(cfg, tables=(cfg.table("categories"),))
        run_extract(spark, categories, "2020-01-01T07:00:00+07:00", "2020-01-01T08:00:00+07:00")
        path = "s3a://batch-check/landing/oltp/staging/categories"
        boundary = spark.read.parquet(path)
        assert {row["code"] for row in boundary.select("code").collect()} == {"boundary-lower", "boundary-inside"}
        run_extract(spark, categories, "2010-01-01T00:00:00Z", "2010-01-02T00:00:00Z")
        empty = spark.read.parquet(path)
        assert empty.count() == 0 and "category_id" in empty.columns
        # Replacing staging with an empty interval must not wipe persistent history.
        existing = spark.table("lakehouse.silver.silver_categories").count()
        ingest_bronze_tables(spark, categories)
        ingest_silver_tables(spark, categories, "pg-empty")
        assert spark.table("lakehouse.silver.silver_categories").count() == existing
        # Exercise the existing Gold builders and quality/publication with the new source types.
        from lakehouse.oltp.gold.schema import GOLD_TABLE_DDL, ensure_oltp_gold_tables
        from lakehouse.oltp.gold.pipeline import run_gold_table
        from lakehouse.oltp.quality.gate import run_gold_quality_gate
        from lakehouse.oltp.publication import publish

        observed = datetime.now(timezone.utc)
        ensure_oltp_gold_tables(spark)
        for table in GOLD_TABLE_DDL:
            run_gold_table(spark, table, "pg-initial", observed.date().isoformat(), as_of=observed)
        quality = run_gold_quality_gate(spark, run_id="pg-initial", batch_date=observed.date().isoformat())
        publish(spark, cfg, "pg-initial", mode="batch")
        # Mixed MySQL/PostgreSQL source identities must fail before overwriting history.
        spark.sql("UPDATE lakehouse.bronze.orders SET _source_system='mysql_ecommerce'")
        try:
            ingest_bronze_tables(spark, orders)
        except ValueError as error:
            assert "legacy" in str(error)
        else:
            raise AssertionError("Legacy source history was accepted")
        assert (
            spark.table("lakehouse.bronze.orders").select("_source_system").distinct().first()[0] == "mysql_ecommerce"
        )
        print(
            "POSTGRES_BATCH_CHECK_OK "
            + json.dumps(
                {
                    "source_tables": 27,
                    "reconciliation_checks": 30,
                    "gold_tables": len(GOLD_TABLE_DDL),
                    "quality_checks": quality["total_checks"],
                }
            ),
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
