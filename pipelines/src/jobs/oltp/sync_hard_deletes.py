"""Synchronize cart/wishlist CDC deletes that occurred in MySQL at least 30 days ago."""

import argparse
import logging
from datetime import datetime

from lakehouse.utils.config import load_config
from lakehouse.oltp.hard_delete import sync_hard_deletes
from lakehouse.utils.spark import spark_session


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--as-of", required=True)
    args = parser.parse_args()
    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
    if as_of.tzinfo is None:
        parser.error("as-of must include a timezone")
    cfg = load_config(args.config_path)
    spark = spark_session("sync_oltp_hard_deletes")
    try:
        sync_hard_deletes(spark, cfg, as_of)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
