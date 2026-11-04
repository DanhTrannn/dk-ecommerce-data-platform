import argparse
from dataclasses import replace
import sys

from lakehouse.config import SOURCE_TABLES, load_config
from lakehouse.oltp.ingestion import ingest_silver_tables
from lakehouse.spark import spark_session


def parse_args(args):
    parser = argparse.ArgumentParser(description="Ingest OLTP Bronze to Silver")
    parser.add_argument("--table", choices=[table.name for table in SOURCE_TABLES])
    parser.add_argument("--config-path", required=True, help="Path to pipeline config YAML")
    parser.add_argument("--run-id", required=True, help="Airflow DAG run ID")
    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])
    cfg = load_config(args.config_path)
    if args.table:
        cfg = replace(cfg, tables=(cfg.table(args.table),))
    spark = spark_session("ingest_oltp_bronze_to_silver")
    try:
        ingest_silver_tables(spark, cfg, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
