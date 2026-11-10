"""Explicit categories pilot; no scheduler, publication or production namespace writes."""

import argparse
import json
import os
from contextlib import closing

from lakehouse.oltp.polars_pilot import run_pilot
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--interval-start")
    parser.add_argument("--interval-end")
    parser.add_argument("--page-size", type=int, default=10000)
    args = parser.parse_args()
    with closing(postgres_reader()) as connection:
        result = run_pilot(
            iceberg_catalog(),
            connection,
            args.namespace,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=args.run_id,
            start=args.interval_start,
            end=args.interval_end,
            page_size=args.page_size,
        )
    print("POLARS_RESULT " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
