"""Run the explicit quiescent-source PostgreSQL/Silver native diagnostic."""

import argparse
import json
from contextlib import closing
from datetime import UTC, datetime

from lakehouse.oltp.quality.reconcile import run_reconciliation_gate
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", default="serving")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-date", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--tolerance-pct", type=float, default=0.05)
    args = parser.parse_args()
    with closing(postgres_reader()) as connection:
        result = run_reconciliation_gate(
            iceberg_catalog(),
            connection,
            prefix=args.namespace_prefix,
            tolerance_pct=args.tolerance_pct,
            run_id=args.run_id,
            batch_date=args.batch_date,
        )
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
