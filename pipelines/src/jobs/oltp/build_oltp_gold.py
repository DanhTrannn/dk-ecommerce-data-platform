from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from lakehouse.oltp.gold.pipeline import run_gold_table
from lakehouse.oltp.gold_ddl import GOLD_TABLE_DDL, ensure_oltp_gold_tables
from lakehouse.spark import spark_session

from lakehouse.writer import oltp_writer_lock

VALID_STAGES = ("all", "dimensions", "facts", "marts")


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OLTP Gold layer tables (Star Schema & Marts)")
    parser.add_argument("--table", choices=list(GOLD_TABLE_DDL))
    parser.add_argument("--run-id", required=True, help="Batch or DAG run ID")
    parser.add_argument("--as-of", help="Fixed observation time for all retries of this DAG run")
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
    parsed = parser.parse_args(args)
    prefix = {"dimensions": "dim_", "facts": "fact_", "marts": "mart_"}.get(parsed.stage, "")
    if parsed.table and not parsed.table.startswith(prefix):
        parser.error(f"{parsed.table} does not belong to stage {parsed.stage}")
    return parsed


def _run() -> None:
    args = parse_args(sys.argv[1:])
    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")) if args.as_of else None
    if as_of is not None and as_of.tzinfo is None:
        raise ValueError("as-of must include a timezone")
    app_name = f"build_oltp_gold_{args.stage}" if args.stage != "all" else "build_oltp_gold"
    spark = spark_session(app_name)

    prefix = {"dimensions": "dim_", "facts": "fact_", "marts": "mart_"}.get(args.stage, "")
    tables = [args.table] if args.table else [name for name in GOLD_TABLE_DDL if name.startswith(prefix)]
    try:
        for table in tables:
            ensure_oltp_gold_tables(spark, tables=[table])
            run_gold_table(spark, table, args.run_id, args.snapshot_date, as_of=as_of)
        print(f"[{args.run_id}] Gold stage '{args.stage}' successfully finished!")

    finally:
        spark.stop()


def main() -> None:
    with oltp_writer_lock():
        _run()


if __name__ == "__main__":
    main()
