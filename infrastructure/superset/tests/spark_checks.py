"""Prepare actual batch publications and inject faults only in the private BI fixture."""

import json
import sys
from datetime import datetime, timezone

from lakehouse.utils.spark import spark_session
from lakehouse.utils.config import load_config
from lakehouse.oltp.publication import publish

spark = spark_session("superset_acceptance")
spark.sparkContext.setLogLevel("ERROR")
try:
    mode = sys.argv[1]
    cfg = load_config()
    if mode in {"bootstrap", "restore"}:
        from lakehouse.oltp.extract import run_initial_extract
        from lakehouse.oltp.ingestion import ingest_bronze_tables, ingest_silver_tables
        from lakehouse.oltp.gold.schema import ensure_oltp_gold_tables, GOLD_TABLE_DDL
        from lakehouse.oltp.gold.pipeline import run_gold_table
        from lakehouse.oltp.quality.gate import run_gold_quality_gate
        from lakehouse.oltp.quality.reconcile import run_reconciliation_gate

        run_id = "bi-initial" if mode == "bootstrap" else "bi-next"
        now = datetime.now(timezone.utc)
        run_initial_extract(spark, cfg)
        ingest_bronze_tables(spark, cfg)
        ingest_silver_tables(spark, cfg, run_id)
        reconciliation = run_reconciliation_gate(spark, cfg, tolerance_pct=0)
        ensure_oltp_gold_tables(spark)
        for name in GOLD_TABLE_DDL:
            run_gold_table(spark, name, run_id, now.date().isoformat(), as_of=now)
        quality = run_gold_quality_gate(spark, run_id=run_id, batch_date=now.date().isoformat())
        publish(spark, cfg, run_id, mode="batch")
        result = dict(reconciliation_checks=reconciliation["passed_checks"], gold_checks=quality["passed_checks"])
    elif mode == "corrupt":
        spark.sql("UPDATE lakehouse.gold.mart_order_daily SET net_revenue_vnd=net_revenue_vnd+1000000")
        result = dict(unpublished_mart_mutation=True)
    else:
        raise ValueError(mode)
    print("STACK_RESULT " + json.dumps(result), flush=True)
finally:
    spark.stop()
