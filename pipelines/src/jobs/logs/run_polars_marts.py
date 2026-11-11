"""Build isolated native log marts from the persisted serving access-log fact."""

import argparse
import json
import os
from contextlib import closing

from lakehouse.logs.polars.pipeline import run_marts
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", required=True, help="Timezone-aware event cutoff; keep fixed on retries")
    parser.add_argument("--catalog-source", choices=("native", "serving"), default="native")
    parser.add_argument("--catalog-run-id", help="Specific OLTP batch publication; default latest eligible batch")
    parser.add_argument(
        "--verify-only", action="store_true", help="Audit existing marts without rebuilding or publishing"
    )
    args = parser.parse_args()
    with closing(postgres_reader()) as connection:
        result = run_marts(
            iceberg_catalog(),
            connection,
            args.namespace_prefix,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=args.run_id,
            as_of=args.as_of,
            catalog_source=args.catalog_source,
            catalog_run_id=args.catalog_run_id,
            verify_only=args.verify_only,
        )
    print("NATIVE_LOG_RESULT " + json.dumps(result, default=str), flush=True)


if __name__ == "__main__":
    main()
