"""Native job for Iceberg table maintenance in Pure Streaming and OLTP Lakehouse.

Actions:
- rewrite_data_files: Compaction of small Parquet files into optimal sizes (128MB target).
- Compaction also regenerates manifests.
- expire_snapshots: Purging old metadata snapshots beyond retention policies.
- Orphan scan: report unreachable data files; delete only with writers explicitly stopped.
"""

from __future__ import annotations


import argparse
from typing import Sequence

import json
from contextlib import closing
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader
from lakehouse.utils.maintenance import maintain_tables

# Standard table profiles across Lakehouse layers


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iceberg table maintenance")
    parser.add_argument(
        "--profile",
        choices=["streaming", "oltp", "all", "custom"],
        default=None,
        help="Predefined table profile (streaming, oltp, all)",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated table names (e.g. landing.access_logs,gold.fact_order). Overrides profile.",
    )
    parser.add_argument(
        "--retain-snapshots",
        type=int,
        default=20,
        help="Number of snapshots to retain during expire_snapshots (default: 20)",
    )
    parser.add_argument(
        "--target-file-size-bytes",
        type=int,
        default=134217728,  # 128 MB
        help="Target data file size in bytes for compaction (default: 134217728 = 128MB)",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=3,
        help="Retention period in days for orphan files cleanup (default: 3 days)",
    )
    parser.add_argument(
        "--catalog",
        default="lakehouse",
        help="Iceberg catalog name (default: lakehouse)",
    )
    parser.add_argument(
        "--action",
        choices=["all", "compact", "expire", "orphan", "quarantine"],
        default="all",
        help="Maintenance action to perform (default: all)",
    )
    parser.add_argument(
        "--quarantine-retention-days",
        type=int,
        default=30,
        help="Delete quarantined records older than this many days (default: 30)",
    )
    parser.add_argument("--namespace-prefix", default="serving")
    parser.add_argument("--delete-orphans", action="store_true")
    parser.add_argument("--writers-stopped", action="store_true")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    with closing(postgres_reader()) as connection:
        result = maintain_tables(iceberg_catalog(), connection, args)
    print(json.dumps(result, default=str), flush=True)


if __name__ == "__main__":
    main()
