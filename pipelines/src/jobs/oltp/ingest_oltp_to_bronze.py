"""Load reusable staging into Bronze without duplicating source versions."""

import argparse
from dataclasses import replace
from lakehouse.config import SOURCE_TABLES, load_config
from lakehouse.oltp.ingestion import ingest_bronze_tables
from lakehouse.spark import spark_session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", choices=[table.name for table in SOURCE_TABLES])
    parser.add_argument("--config-path", required=True)
    args = parser.parse_args()
    cfg = load_config(args.config_path)
    if args.table:
        cfg = replace(cfg, tables=(cfg.table(args.table),))
    spark = spark_session("staging_to_bronze")
    try:
        ingest_bronze_tables(spark, cfg)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
