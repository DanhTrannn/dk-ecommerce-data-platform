"""CLI entrypoint for persistent post-Gold data-quality validation."""

from __future__ import annotations

import argparse
import sys
import os

from lakehouse.oltp.quality.gate import GoldQualityGateError, run_gold_quality_gate
from lakehouse.spark import spark_session
from lakehouse.config import load_config
from lakehouse.oltp.publication import publish


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post-Gold data quality gate")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-date", required=True)
    parser.add_argument(
        "--config-path", default=os.environ.get("PIPELINE_CONFIG_PATH", "/opt/project/pipelines/config/default.yml")
    )
    return parser.parse_args(args)


def main() -> None:
    args = parse_args(sys.argv[1:])
    spark = spark_session("post_gold_quality_gate")
    try:
        summary = run_gold_quality_gate(
            spark,
            run_id=args.run_id,
            batch_date=args.batch_date,
        )
        print(f"GOLD QUALITY GATE OK: passed {summary['passed_checks']}/{summary['total_checks']} checks.")
        publish(spark, load_config(args.config_path), args.run_id, mode="batch")
    except GoldQualityGateError as exc:
        print(f"GOLD QUALITY GATE FAILURE: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
