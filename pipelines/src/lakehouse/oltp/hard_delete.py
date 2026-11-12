"""Bounded CDC evidence deletes for retired cart/wishlist Silver records."""

import json
import os
from datetime import UTC, timedelta

import polars as pl
from pyiceberg.expressions import In

from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.utils.config import HARD_DELETE_TABLES, SOURCE_TABLES
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.postgres import namespace_lock


def deletion_keys(events, table, database, as_of):
    cutoff = as_of.astimezone(UTC) - timedelta(days=30)
    candidates = events.filter(
        (pl.col("operation") == "d")
        & (pl.col("database_name") == database)
        & (pl.col("table_name") == table.name)
        & (pl.col("source_ts") <= cutoff)
    )
    keys = set()
    for row in candidates.to_dicts():
        try:
            before = json.loads(row["before_payload"] or "{}")
            record = json.loads(row["record_key"] or "{}")
            if not isinstance(before, dict) or not isinstance(record, dict):
                continue
            present = before.get("is_present")
            if str(present).lower() not in {"0", "false"} or before.get("removed_at") is None:
                continue
            key = before.get(table.pk)
            key = record.get(table.pk) if key is None else key
            if isinstance(key, bool):
                continue
            keys.add(int(key))
        except (ValueError, TypeError):
            continue
    return sorted(keys)


def sync_hard_deletes(catalog, connection, as_of, prefix="serving"):
    if as_of.tzinfo is None:
        raise ValueError("Observation time must include a timezone")
    database = os.environ["POSTGRES_ECOMMERCE_DB"]
    with namespace_lock(connection, prefix):
        audit = catalog.load_table((ns(prefix, "bronze"), "oltp_cdc_events"))
        events = read_snapshot(audit, capture(audit))
        result = {}
        for table in SOURCE_TABLES:
            if table.name not in HARD_DELETE_TABLES:
                continue
            keys = deletion_keys(events, table, database, as_of)
            target = catalog.load_table((ns(prefix, "silver"), table.silver_table))
            count = target.scan(row_filter=In(table.pk, keys)).count() if keys else 0
            if keys:
                target.delete(In(table.pk, keys))
            result[table.name] = count
        return result
