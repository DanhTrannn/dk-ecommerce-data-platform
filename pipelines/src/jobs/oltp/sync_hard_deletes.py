"""Preview or apply explicitly requested, soft-deleted CDC keys older than 30 days."""

import argparse
import json
from datetime import datetime

from lakehouse.config import load_config
from lakehouse.oltp.hard_delete import sync_hard_deletes
from lakehouse.spark import spark_session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--tables", required=True, help="JSON array of explicitly selected source tables")
    parser.add_argument("--apply", choices=("true", "false"), default="false")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", required=True)
    args = parser.parse_args()
    tables = json.loads(args.tables)
    if not isinstance(tables, list) or any(not isinstance(name, str) for name in tables):
        parser.error("tables must be a JSON array of table names")
    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
    if as_of.tzinfo is None:
        parser.error("as-of must include a timezone")
    cfg = load_config(args.config_path)
    spark = spark_session("sync_oltp_hard_deletes")
    try:
        sync_hard_deletes(spark, cfg, tables, args.run_id, as_of, apply=args.apply == "true")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
