"""CLI entrypoint for OLTP-to-Silver Reconciliation Gate Spark job."""

from __future__ import annotations

import argparse
import sys

from lakehouse.config import load_config
from lakehouse.oltp.reconcile import ReconciliationGateError, run_reconciliation_gate
from lakehouse.spark import spark_session
from lakehouse.writer import oltp_writer_lock


def main() -> None:
    parser = argparse.ArgumentParser(description="OLTP-to-Silver Reconciliation Gate")
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--tolerance-pct", type=float, default=0.05)
    args = parser.parse_args()

    cfg = load_config(args.config_path)

    def run():
        spark = spark_session("oltp_reconciliation_gate")
        try:
            summary = run_reconciliation_gate(
                spark,
                cfg,
                tolerance_pct=args.tolerance_pct,
                run_id=args.run_id,
                batch_date=args.batch_date,
                persist_results=True,
            )
            print(f"RECONCILIATION GATE OK: passed {summary['passed_checks']}/{summary['total_checks']} checks.")
        except ReconciliationGateError as exc:
            print(f"RECONCILIATION GATE FAILURE: {exc}", file=sys.stderr)
            sys.exit(1)
        finally:
            spark.stop()

    with oltp_writer_lock():
        run()


if __name__ == "__main__":
    main()
