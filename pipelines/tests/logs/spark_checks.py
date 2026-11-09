"""Actual source/catalog bootstrap and retry checks using production Spark builders."""

import json
import sys
from datetime import datetime, timezone

from lakehouse.utils.spark import spark_session
from lakehouse.logs.pipeline import run_logs_marts

spark = spark_session("access_log_acceptance")
spark.sparkContext.setLogLevel("ERROR")
try:
    mode, run_id = sys.argv[1:3]
    if mode == "catalog":
        from lakehouse.utils.config import load_config
        from lakehouse.oltp.extract import run_initial_extract
        from lakehouse.oltp.ingestion import ingest_bronze_tables, ingest_silver_tables
        from lakehouse.oltp.gold.schema import ensure_oltp_gold_tables, GOLD_TABLE_DDL
        from lakehouse.oltp.gold.pipeline import run_gold_table
        from lakehouse.oltp.quality.gate import run_gold_quality_gate
        from lakehouse.oltp.quality.reconcile import run_reconciliation_gate
        from lakehouse.oltp.publication import publish

        cfg = load_config()
        run_initial_extract(spark, cfg)
        ingest_bronze_tables(spark, cfg)
        ingest_silver_tables(spark, cfg, run_id)
        reconciliation = run_reconciliation_gate(spark, cfg, tolerance_pct=0)
        ensure_oltp_gold_tables(spark)
        now = datetime.now(timezone.utc)
        for table in GOLD_TABLE_DDL:
            run_gold_table(spark, table, run_id, now.date().isoformat(), as_of=now)
        quality = run_gold_quality_gate(spark, run_id=run_id, batch_date=now.date().isoformat())
        publish(spark, cfg, run_id, mode="batch")
        uuid_columns = 0
        from pyspark.sql.types import BinaryType
        from pyspark.sql import functions as F

        for table in cfg.tables:
            silver = spark.table(f"lakehouse.silver.{table.silver_table}")
            for field in silver.schema:
                if isinstance(field.dataType, BinaryType):
                    assert silver.filter(F.col(field.name).isNotNull() & (F.length(field.name) != 16)).count() == 0, (
                        table.name,
                        field.name,
                    )
                    uuid_columns += 1
        from lakehouse.logs.product_identity import resolve_product_events

        products = spark.table("lakehouse.silver.silver_products")
        variants = spark.table("lakehouse.silver.silver_product_variants")
        item = variants.select("product_id", "public_id").first()
        from uuid import UUID

        events = spark.createDataFrame([("", str(UUID(bytes=bytes(item.public_id))))], ["product_key", "variant_key"])

        def legacy_uuid(column):
            token = F.hex(column)
            return F.concat_ws(
                "-", *[F.substring(token, start, size) for start, size in [(1, 8), (9, 4), (13, 4), (17, 4), (21, 12)]]
            ).cast("binary")

        for source in [variants, variants.withColumn("public_id", legacy_uuid("public_id"))]:
            resolved = resolve_product_events(events, products, source).first()
            assert resolved.product_id == item.product_id
        product = products.select("product_id", "public_id").first()
        product_events = spark.createDataFrame(
            [(str(UUID(bytes=bytes(product.public_id))), "")], ["product_key", "variant_key"]
        )
        for source in [products, products.withColumn("public_id", legacy_uuid("public_id"))]:
            assert resolve_product_events(product_events, source, variants).first().product_id == product.product_id

        result = dict(
            reconciliation=reconciliation,
            quality=quality,
            canonical_uuid_columns=uuid_columns,
            legacy_uuid_mapping_checks=4,
        )
    elif mode == "conflict":
        # Same identity, different valid raw envelope: must fail before a new publication.
        raw = spark.sql(
            "SELECT raw_payload FROM lakehouse.landing.access_logs WHERE event_id IS NOT NULL LIMIT 1"
        ).first()[0]
        event = json.loads(raw)
        event["event"]["duration_ns"] += 1000
        from pyspark.sql import functions as F

        payload = spark.createDataFrame([(json.dumps(event),)], ["raw_payload"]).withColumn(
            "ingest_ts", F.current_timestamp()
        )
        schema = spark.table("lakehouse.landing.access_logs").schema
        payload.select(
            *[
                F.col(field.name)
                if field.name in payload.columns
                else F.lit(None).cast(field.dataType).alias(field.name)
                for field in schema
            ]
        ).writeTo("lakehouse.landing.access_logs").append()
        before = spark.table("lakehouse.system.logs_analytics_publications").count()
        try:
            run_logs_marts(spark, run_id)
        except ValueError as error:
            assert "Conflicting valid raw payloads" in str(error)
        else:
            raise AssertionError("Conflicting identity was published")
        assert spark.table("lakehouse.system.logs_analytics_publications").count() == before
        result = dict(conflicting_id_rejected=True, publication_retained=True)
    else:
        before = spark.table("lakehouse.system.logs_analytics_publications").count()
        run_logs_marts(spark, run_id)
        run_logs_marts(spark, run_id)
        assert spark.table("lakehouse.system.logs_analytics_publications").count() == before + 1
        result = dict(retry_publication_rows=1)
    print("STACK_RESULT " + json.dumps(result, default=str), flush=True)
finally:
    spark.stop()
