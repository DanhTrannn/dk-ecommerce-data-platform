"""Roll up and publish all log marts from one captured fact snapshot."""

import argparse
import sys

from lakehouse.logs.pipeline import run_logs_marts
from lakehouse.utils.spark import spark_session


def parse_args(args):
    parser = argparse.ArgumentParser(description="Build and publish a complete log-mart snapshot set")
    parser.add_argument("--run-id", required=True)
    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])
    spark = spark_session("build_logs_gold_marts")
    try:
        run_logs_marts(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
