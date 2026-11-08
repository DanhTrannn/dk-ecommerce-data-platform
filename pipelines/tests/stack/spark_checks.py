"""Production Spark/Polaris acceptance checks and reversible fixture-only faults."""

import argparse
import json
from datetime import datetime, timezone

from lakehouse.utils.spark import spark_session
from lakehouse.utils.config import load_config
from lakehouse.oltp.quality.reconcile import run_reconciliation_gate
from lakehouse.oltp.quality.gate import run_gold_quality_gate
from lakehouse.oltp.gold.pipeline import run_gold_table


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=[
            "reconcile",
            "corrupt",
            "restore",
            "counts",
            "permission-check",
            "permission-table",
        ],
    )
    args = parser.parse_args()
    spark = spark_session("stack_acceptance_" + args.mode)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if args.mode == "reconcile":
            result = run_reconciliation_gate(spark, load_config(), tolerance_pct=0)
        elif args.mode == "corrupt":
            spark.sql("UPDATE lakehouse.gold.fact_order SET order_total_vnd=order_total_vnd+1")
            result = {"fault": "fact_order_total_changed"}
        elif args.mode == "restore":
            from lakehouse.oltp.publication import publish

            now = datetime.now(timezone.utc)
            # A retry keeps the original logical run identity. A new ID would
            # correctly fail the unchanged marts' current-run freshness checks.
            run_id = spark.table("lakehouse.gold.fact_order_item").select("_source_run_id").distinct().first()[0]
            publication_count = spark.table("lakehouse.system.analytics_publications").count()
            run_gold_table(spark, "fact_order", run_id, now.date().isoformat(), as_of=now)
            result = run_gold_quality_gate(spark, run_id=run_id, batch_date=now.date().isoformat())
            publish(spark, load_config(), run_id, mode="batch")
            assert spark.table("lakehouse.system.analytics_publications").count() == publication_count
            result["recovery_publication_run_id"] = run_id
            result["publication_count"] = publication_count
        elif args.mode == "permission-table":
            spark.sql(
                "CREATE TABLE lakehouse.gold.stack_permission_probe (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')"
            )
            spark.sql("INSERT INTO lakehouse.gold.stack_permission_probe VALUES (1)")
            result = {"permission_probe": "created"}
        elif args.mode == "counts":
            result = {
                "bronze_orders": spark.table("lakehouse.bronze.orders").count(),
                "silver_orders": spark.table("lakehouse.silver.silver_orders").count(),
            }
        else:
            spark.sql("SELECT count(*) FROM lakehouse.gold.fact_order").collect()
            result = {"permission": "allowed"}
        print("STACK_RESULT " + json.dumps(result, default=str), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
