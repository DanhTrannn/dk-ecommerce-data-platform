"""Ingest PostgreSQL source tables into Bronze/Silver, without a BI publication."""

import argparse
import json
import os
from contextlib import closing

from lakehouse.oltp.polars.pipeline import run_batch
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--table", action="append", help="Repeat for a subset; default: all 27 source tables")
    parser.add_argument("--interval-start")
    parser.add_argument("--interval-end")
    parser.add_argument("--page-size", type=int, default=10000)
    args = parser.parse_args()
    with closing(postgres_reader()) as connection:
        result = run_batch(
            iceberg_catalog(),
            connection,
            args.namespace_prefix,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=args.run_id,
            tables=args.table,
            start=args.interval_start,
            end=args.interval_end,
            page_size=args.page_size,
        )
    print("POLARS_RESULT " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
