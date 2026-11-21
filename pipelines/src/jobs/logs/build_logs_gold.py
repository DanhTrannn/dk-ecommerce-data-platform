"""Native Landing through Bronze/Silver/fact and three published log marts."""

import argparse
import json
import os
from contextlib import closing
from datetime import UTC, datetime

from lakehouse.logs.pipeline import run_logs
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def run(*, run_id, as_of):
    """The log DAG's task; as_of is an ISO timestamp with timezone."""
    with closing(postgres_reader()) as connection:
        result = run_logs(
            iceberg_catalog(),
            connection,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=run_id,
            as_of=as_of,
        )
    print("NATIVE_LOG_RESULT " + json.dumps(result, default=str), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", default=None)
    args = parser.parse_args()
    run(run_id=args.run_id, as_of=args.as_of or datetime.now(UTC).isoformat())


if __name__ == "__main__":
    main()
