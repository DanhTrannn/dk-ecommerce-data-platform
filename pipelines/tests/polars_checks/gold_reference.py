"""Production Spark Gold builders/gate/publication as the private migration oracle."""

import json
import sys
from datetime import datetime

from gold_contracts import gold_fingerprint
from lakehouse.oltp.extract import run_initial_extract, run_extract
from lakehouse.oltp.ingestion import ingest_bronze_tables, ingest_silver_tables
from lakehouse.oltp.gold.schema import GOLD_TABLE_DDL, ensure_oltp_gold_tables
from lakehouse.oltp.gold.pipeline import run_gold_table
from lakehouse.oltp.quality.gate import run_gold_quality_gate
from lakehouse.oltp.publication import publish
from lakehouse.utils.config import load_config
from lakehouse.utils.spark import spark_session

spark = spark_session("native_gold_reference")
spark.sparkContext.setLogLevel("ERROR")
try:
    mode, run_id, observed = sys.argv[1], sys.argv[2], datetime.fromisoformat(sys.argv[3])
    cfg = load_config()
    if mode == "initial":
        run_initial_extract(spark, cfg)
    else:
        run_extract(spark, cfg, sys.argv[4], sys.argv[5])
    ingest_bronze_tables(spark, cfg)
    ingest_silver_tables(spark, cfg, run_id)
    ensure_oltp_gold_tables(spark)
    for table in GOLD_TABLE_DDL:
        run_gold_table(spark, table, run_id, observed.date().isoformat(), as_of=observed)
    summary = run_gold_quality_gate(spark, run_id=run_id, batch_date=observed.date().isoformat())
    publish(spark, cfg, run_id, mode="batch")
    results = {}
    for name in GOLD_TABLE_DDL:
        frame = spark.table("lakehouse.gold." + name)
        results[name] = gold_fingerprint(frame.collect(), frame.columns)
    print("STACK_RESULT " + json.dumps({"gold": results, "quality": summary}), flush=True)
finally:
    spark.stop()
