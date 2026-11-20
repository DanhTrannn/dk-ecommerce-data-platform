"""Unified batch pipeline CLI orchestrating ingestion, build, audit and publication."""

import argparse
from contextlib import closing
import json
import os

from lakehouse.oltp.batch import run_pipeline
from lakehouse.oltp.run_state import BatchRunIdentity
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--as-of", required=True, help="ISO UTC timestamp for KPI observation clock")
    parser.add_argument("--batch-date", required=True, help="YYYY-MM-DD business date")
    parser.add_argument("--interval-start", help="UTC ISO timestamp for lower bound")
    parser.add_argument("--interval-end", help="UTC ISO timestamp for upper bound")
    parser.add_argument("--page-size", type=int, default=10000)
    parser.add_argument("--date-start", help="First date in dim_date")
    parser.add_argument("--date-end", help="Last date in dim_date")
    parser.add_argument("--migrate-schema", action="store_true", help="Apply the legacy-to-v4 schema evolution in a full run")
    parser.add_argument("--trino-reconciliation", action="store_true", help="Compare the SQL and native reconciliation on pinned inputs")

    args = parser.parse_args()

    interval_start = args.interval_start if args.interval_start else None
    interval_end = args.interval_end if args.interval_end else None

    if args.mode == "full":
        if interval_start is not None or interval_end is not None:
            raise ValueError("Full mode reads all rows; interval bounds must be omitted")
    elif args.mode == "incremental":
        if interval_start is None or interval_end is None:
            parser.error("Incremental mode requires both interval bounds")
        if args.migrate_schema:
            parser.error("--migrate-schema requires full mode")

    identity = BatchRunIdentity(
        run_id=args.run_id,
        mode=args.mode,
        namespace_prefix=args.namespace_prefix,
        interval_start=interval_start,
        interval_end=interval_end,
        as_of=args.as_of,
        batch_date=args.batch_date,
        date_start=args.date_start,
        date_end=args.date_end,
        migrate_schema=args.migrate_schema,
        trino_reconciliation=args.trino_reconciliation,
    )

    with closing(postgres_reader()) as connection:
        result = run_pipeline(
            iceberg_catalog(),
            connection,
            args.namespace_prefix,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            identity=identity,
            page_size=args.page_size,
        )

    print("BATCH_PIPELINE_RESULT " + json.dumps({
        "run_id": result.get("run_id"),
        "status": result.get("status"),
        "attempt": result.get("attempt"),
    }), flush=True)


if __name__ == "__main__":
    main()
