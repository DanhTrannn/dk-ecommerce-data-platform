"""OLTP batch: PostgreSQL to Bronze, pre-merge checks, Silver merge and dbt Gold in one run."""

import argparse
import json
import os
from contextlib import closing

from lakehouse.oltp.batch import run_pipeline
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", required=True)
    parser.add_argument("--run-id", required=True, help="Airflow retries reuse it; Bronze replaces that run's earlier rows")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--as-of", required=True, help="ISO timestamp with timezone; Gold's current day")
    parser.add_argument("--interval-start", help="Incremental only: inclusive cursor lower bound (ISO, with timezone)")
    parser.add_argument("--interval-end", help="Incremental only: exclusive cursor upper bound (ISO, with timezone)")
    parser.add_argument("--page-size", type=int, default=10000)
    args = parser.parse_args(argv)
    has_interval = (args.interval_start, args.interval_end)
    if args.mode == "full" and any(has_interval):
        parser.error("Full mode reads every source row; omit the interval bounds")
    if args.mode == "incremental" and not all(has_interval):
        parser.error("Incremental mode requires both interval bounds")
    return args


def main(argv=None):
    args = parse_args(argv)
    with closing(postgres_reader()) as connection:
        result = run_pipeline(
            iceberg_catalog(),
            connection,
            args.namespace_prefix,
            run_id=args.run_id,
            mode=args.mode,
            as_of=args.as_of,
            interval_start=args.interval_start or None,
            interval_end=args.interval_end or None,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            page_size=args.page_size,
        )
    print("BATCH_PIPELINE_RESULT " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
