"""Iceberg table maintenance for the streaming and OLTP lakehouse tables.

Actions:
- compact: rewrite small Parquet files toward the target size; this also regenerates manifests.
- expire: remove snapshots beyond the retention policy.
- orphan: report unreachable data files; delete them only with writers explicitly stopped.

The DAGs call `maintain`; run this module by hand only for an operator action such as orphan deletion.
"""

import argparse
import json
from contextlib import closing

from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.maintenance import maintain_tables
from lakehouse.utils.postgres import postgres_reader


def maintain(**options):
    """Run maintain_tables with the given keyword options and print its per-table result."""
    with closing(postgres_reader()) as connection:
        result = maintain_tables(iceberg_catalog(), connection, **options)
    print(json.dumps(result, default=str), flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Iceberg table maintenance")
    parser.add_argument("--namespace-prefix", default="serving")
    parser.add_argument("--profile", choices=["streaming", "oltp", "all"], default=None, help="Predefined table profile")
    parser.add_argument("--tables", default=None, help="Comma-separated tables, e.g. gold.fact_order; overrides the profile")
    parser.add_argument("--action", choices=["all", "compact", "expire", "orphan"], default="all")
    parser.add_argument("--retain-snapshots", type=int, default=20)
    parser.add_argument("--target-file-size-bytes", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--older-than-days", type=int, default=3, help="Orphan files must be older than this")
    parser.add_argument("--delete-orphans", action="store_true")
    parser.add_argument("--writers-stopped", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    maintain(**vars(parse_args()))
