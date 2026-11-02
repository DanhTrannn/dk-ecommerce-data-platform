from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from lakehouse.oltp.gold.pipeline import run_gold_dimensions, run_gold_facts, run_gold_marts
from lakehouse.oltp.gold_ddl import ensure_oltp_gold_tables
from lakehouse.spark import spark_session

from lakehouse.writer import oltp_writer_lock

VALID_STAGES = ("all", "dimensions", "facts", "marts")


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OLTP Gold layer tables (Star Schema & Marts)")
    parser.add_argument("--run-id", required=True, help="Batch or DAG run ID")
    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Logical batch date; inventory snapshots use the actual Vietnam observation date",
    )
    parser.add_argument(
        "--stage",
        choices=VALID_STAGES,
        default="all",
        help="Gold stage to build: dimensions, facts, marts, or all (default: all)",
    )
    return parser.parse_args(args)


def _run() -> None:
    args = parse_args(sys.argv[1:])
    app_name = f"build_oltp_gold_{args.stage}" if args.stage != "all" else "build_oltp_gold"
    spark = spark_session(app_name)

    try:
        print(f"[{args.run_id}] Ensuring Gold tables DDL...")
        ensure_oltp_gold_tables(spark)

        if args.stage in ("all", "dimensions"):
            run_gold_dimensions(spark, args.run_id)

        if args.stage in ("all", "facts"):
            run_gold_facts(spark, args.run_id, args.snapshot_date)

        if args.stage in ("all", "marts"):
            run_gold_marts(spark, args.run_id)

        print(f"[{args.run_id}] Gold stage '{args.stage}' successfully finished!")

    finally:
        spark.stop()


def main() -> None:
    with oltp_writer_lock():
        _run()


if __name__ == "__main__":
    main()
