"""Build Gold using dbt/Trino, validate and publish audited Iceberg snapshots."""

import argparse
import json
import os
from contextlib import closing

from lakehouse.analytics.commerce import build_commerce as run_gold
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", default="serving")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-date", help="Logical batch date (YYYY-MM-DD); defaults to the observation UTC date")
    parser.add_argument("--as-of", required=True, help="Fixed timezone-aware observation time; retain it on retries")
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Audit current tables without rebuilding or publishing",
    )
    args = parser.parse_args()
    with closing(postgres_reader()) as connection:
        result = run_gold(
            iceberg_catalog(),
            connection,
            args.namespace_prefix,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=args.run_id,
            as_of=args.as_of,
            verify_only=args.verify_only,
            batch_date=args.batch_date,
        )
    print("NATIVE_GOLD_RESULT " + json.dumps(result, default=str), flush=True)


if __name__ == "__main__":
    main()
