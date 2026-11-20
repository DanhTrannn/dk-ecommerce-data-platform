"""Publish only the exact audited native snapshot set, keeping schema-v3 field semantics."""

from lakehouse.utils.namespaces import namespace as ns
import json
from datetime import datetime, UTC

import pyarrow as pa
from pyiceberg.expressions import EqualTo

from lakehouse.oltp.polars.writer import PROPERTIES
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.iceberg import load_or_create
from lakehouse.oltp.gold.schema import TABLES

from lakehouse.oltp.quality.contracts import validate_required_checks
from lakehouse.oltp.quality.required_rules import REQUIRED_CHECK_IDS

PUBLICATION_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("published_at", pa.timestamp("us", tz="UTC")),
        ("source_event_at", pa.timestamp("us", tz="UTC")),
        ("snapshots_json", pa.string()),
        ("offsets_json", pa.string()),
        ("changed_tables_json", pa.string()),
        ("schema_version", pa.int32()),
        ("source_mode", pa.string()),
    ]
)


def load_publication(catalog, prefix, run_id):
    key = (ns(prefix, "system"), "analytics_publications")
    if not catalog.table_exists(key):
        return None
    rows = catalog.load_table(key).scan(row_filter=EqualTo("run_id", run_id)).to_arrow().to_pylist()
    matching = [row for row in rows if row["source_mode"] == "batch"]
    if len(matching) > 1:
        raise ValueError("Multiple publications match the batch run ID")
    return matching[0] if matching else None


def publish(catalog, prefix, run_id, snapshots, summary):
    if not isinstance(summary, dict) or summary.get("status") != "PASSED" or summary.get("failed_checks", 0) > 0:
        raise ValueError("A complete successful native quality gate is required")
    validate_required_checks(summary, REQUIRED_CHECK_IDS)
    expected = {f"lakehouse.{ns(prefix, 'silver')}.{t.silver_table}" for t in SOURCE_TABLES}
    expected.update(f"lakehouse.{ns(prefix, 'gold')}.{name}" for name in TABLES)
    if set(snapshots) != expected:
        raise ValueError("Publication requires all 27 Silver and 27 Gold snapshots from this namespace")
    for name, snapshot in snapshots.items():
        table = catalog.load_table(tuple(name.split(".")[1:]))
        if snapshot is not None:
            if type(snapshot) is not int or snapshot <= 0 or table.snapshot_by_id(snapshot) is None:
                raise ValueError("An audited snapshot is no longer readable")
    existing = load_publication(catalog, prefix, run_id)
    if existing is not None:
        if json.loads(existing["snapshots_json"]) != snapshots:
            raise ValueError("An existing publication has different snapshots for this run ID")
        return existing
    catalog.create_namespace_if_not_exists(ns(prefix, "system"))
    target = load_or_create(
        catalog, (ns(prefix, "system"), "analytics_publications"), schema=PUBLICATION_SCHEMA, properties=PROPERTIES
    )
    row = dict(
        run_id=run_id,
        published_at=datetime.now(UTC),
        source_event_at=None,
        snapshots_json=json.dumps(snapshots, sort_keys=True),
        offsets_json="{}",
        changed_tables_json="[]",
        schema_version=4,
        source_mode="batch",
    )
    target.upsert(pa.Table.from_pylist([row], schema=target.schema().as_arrow()), join_cols=["run_id", "source_mode"])
    return row
