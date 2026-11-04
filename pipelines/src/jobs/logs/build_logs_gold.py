"""Process Landing through Bronze/Silver/fact, then publish three log marts."""

import argparse
import sys

from lakehouse.logs.pipeline import run_logs_marts
from lakehouse.utils.spark import spark_session


def parse_args(args):
    parser = argparse.ArgumentParser(description="Process access-log layers and publish a complete log-mart snapshot set")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", help="Fixed UTC observation time; defaults to invocation time")
    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])
    spark = spark_session("build_logs_gold_marts")
    try:
        run_logs_marts(spark, args.run_id, as_of=args.as_of)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
