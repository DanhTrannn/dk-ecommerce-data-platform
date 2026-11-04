"""Initial full source extraction without any time filtering."""

import argparse
from dataclasses import replace
from lakehouse.utils.config import SOURCE_TABLES, load_config
from lakehouse.oltp.extract import run_initial_extract
from lakehouse.utils.spark import spark_session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", choices=[table.name for table in SOURCE_TABLES])
    parser.add_argument("--config-path", required=True)
    args = parser.parse_args()
    cfg = load_config(args.config_path)
    if args.table:
        cfg = replace(cfg, tables=(cfg.table(args.table),))
    spark = spark_session("init_oltp_staging")
    try:
        run_initial_extract(spark, cfg)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
