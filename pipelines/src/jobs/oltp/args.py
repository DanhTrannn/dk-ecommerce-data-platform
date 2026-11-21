"""Arguments every per-table OLTP CLI shares: target namespace, run identity, mode and source window."""

import argparse
import os
from datetime import datetime

from lakehouse.utils.runs import validate_run


def run_parser(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--namespace-prefix", required=True)
    parser.add_argument("--run-id", required=True, help="Airflow retries reuse it; Bronze replaces that run's earlier rows")
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    parser.add_argument("--run-started-at", required=True, help="ISO timestamp with timezone; full runs read rows created before it")
    parser.add_argument("--interval-start", help="Incremental only: inclusive cursor lower bound (ISO, with timezone)")
    parser.add_argument("--interval-end", help="Incremental only: exclusive cursor upper bound (ISO, with timezone)")
    return parser


def parse(parser, argv):
    args = parser.parse_args(argv)
    window = (args.interval_start, args.interval_end)
    if args.mode == "full" and any(window):
        parser.error("Full mode reads every row created before the run start; omit the interval bounds")
    if args.mode == "incremental" and not all(window):
        parser.error("Incremental mode requires both interval bounds")
    if datetime.fromisoformat(args.run_started_at.replace("Z", "+00:00")).tzinfo is None:
        parser.error("--run-started-at must include a timezone")
    try:
        validate_run(args.namespace_prefix, args.run_id, os.environ["MINIO_LAKEHOUSE_BUCKET"], getattr(args, "page_size", 10000))
    except ValueError as error:
        parser.error(str(error))
    return args
