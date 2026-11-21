"""Iceberg writes, append-only log audits and independently pinned publication."""

import json
from datetime import UTC, datetime

import pyarrow as pa

from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.utils.iceberg import PROPERTIES
from .marts import TABLES
from .quality import REQUIRED_CHECKS

AUDIT_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("checked_at", pa.timestamp("us", tz="UTC")),
        ("check_name", pa.string()),
        ("failed_rows", pa.int64()),
        ("status", pa.string()),
    ]
)
PUBLICATION_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("published_at", pa.timestamp("us", tz="UTC")),
        ("source_event_at", pa.timestamp("us", tz="UTC")),
        ("snapshots_json", pa.string()),
        ("schema_version", pa.int32()),
        ("source_mode", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("source_snapshots_json", pa.string()),
    ]
)


def audit(catalog, run_id, checks):
    target = catalog.create_table_if_not_exists(
        ("system", "logs_quality_results"), schema=AUDIT_SCHEMA, properties=PROPERTIES
    )
    now = datetime.now(UTC)
    rows = [dict(run_id=run_id, checked_at=now, **check) for check in checks]
    if rows:
        target.append(pa.Table.from_pylist(rows, schema=target.schema().as_arrow()))


def require_readable(catalog, snapshots):
    for name, snapshot in snapshots.items():
        table = catalog.load_table(tuple(name.split(".")[1:]))
        if snapshot is not None and (
            type(snapshot) is not int or snapshot <= 0 or table.snapshot_by_id(snapshot) is None
        ):
            raise ValueError("A pinned log input/output snapshot is no longer readable")


def publish(catalog, run_id, snapshots, sources, checks, *, observed, latest):
    expected = {f"lakehouse.gold.{name}" for name in TABLES}
    if (
        set(snapshots) != expected
        or len(checks) != len(REQUIRED_CHECKS)
        or {c["check_name"] for c in checks} != REQUIRED_CHECKS
        or any(c["status"] != "PASSED" for c in checks)
    ):
        raise ValueError("Complete successful log checks and three audited output snapshots are required")
    # The marts read the fact and the Silver product catalog; a run that also ingested names its earlier layers.
    marts_inputs = {"lakehouse.gold.fact_web_events", "lakehouse.silver.products", "lakehouse.silver.product_variants"}
    ingested = {"lakehouse.landing.access_logs", "lakehouse.bronze.web_events", "lakehouse.silver.silver_logs"}
    if set(sources) not in (marts_inputs, marts_inputs | ingested):
        raise ValueError("Publication requires the fact and all three published catalog references")
    require_readable(catalog, {**sources, **snapshots})
    target = catalog.create_table_if_not_exists(
        ("system", "logs_analytics_publications"), schema=PUBLICATION_SCHEMA, properties=PROPERTIES
    )
    row = dict(
        run_id=run_id,
        published_at=datetime.now(UTC),
        source_event_at=latest,
        snapshots_json=json.dumps(snapshots, sort_keys=True),
        schema_version=1,
        source_mode="logs_rollup",
        observed_at=observed,
        source_snapshots_json=json.dumps(sources, sort_keys=True),
    )
    target.upsert(pa.Table.from_pylist([row], schema=target.schema().as_arrow()), join_cols=["run_id", "source_mode"])
    return row


def pinned_catalog(catalog, run_id=None):
    """Current Silver product catalog snapshots; OLTP batches no longer publish a pinned snapshot set."""
    if run_id is not None:
        raise ValueError("Catalog run selection was removed together with OLTP publications")
    silver, sources, frames = "silver", {}, []
    for name in ("products", "product_variants"):
        table = catalog.load_table((silver, name))
        snapshot = capture(table)
        sources[f"lakehouse.{silver}.{name}"] = snapshot
        frames.append(read_snapshot(table, snapshot))
    return frames, sources, None
