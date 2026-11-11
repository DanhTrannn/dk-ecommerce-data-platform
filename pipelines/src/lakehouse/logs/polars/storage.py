"""Iceberg writes, append-only log audits and independently pinned publication."""

import json
from datetime import UTC, datetime

import pyarrow as pa
from pyiceberg.transforms import IdentityTransform

from lakehouse.oltp.polars.gold.storage import capture, read_snapshot
from lakehouse.oltp.polars.schema import align_arrow
from lakehouse.oltp.polars.writer import PROPERTIES
from .marts import TABLES, schema
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


def write_mart(catalog, prefix, name, frame):
    table = catalog.create_table_if_not_exists((prefix + "_gold", name), schema=schema(name), properties=PROPERTIES)
    if not table.spec().fields:
        with table.update_spec() as update:
            update.add_field("metric_date", IdentityTransform(), "metric_date")
    table.overwrite(align_arrow(frame.to_arrow(), table.schema()))
    return table


def audit(catalog, prefix, run_id, checks):
    target = catalog.create_table_if_not_exists(
        (prefix + "_system", "logs_quality_results"), schema=AUDIT_SCHEMA, properties=PROPERTIES
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


def publish(catalog, prefix, run_id, snapshots, sources, checks, *, observed, latest):
    expected = {f"lakehouse.{prefix}_gold.{name}" for name in TABLES}
    if (
        set(snapshots) != expected
        or len(checks) != len(REQUIRED_CHECKS)
        or {c["check_name"] for c in checks} != REQUIRED_CHECKS
        or any(c["status"] != "PASSED" for c in checks)
    ):
        raise ValueError("Complete successful log checks and three audited output snapshots are required")
    allowed = [
        {
            "lakehouse.gold.fact_web_events",
            f"lakehouse.{system}.analytics_publications",
            f"lakehouse.{silver}.silver_products",
            f"lakehouse.{silver}.silver_product_variants",
        }
        for system, silver in ((prefix + "_system", prefix + "_silver"), ("system", "silver"))
    ]
    if set(sources) not in allowed:
        raise ValueError("Publication requires the fact and all three published catalog references")
    require_readable(catalog, {**sources, **snapshots})
    target = catalog.create_table_if_not_exists(
        (prefix + "_system", "logs_analytics_publications"), schema=PUBLICATION_SCHEMA, properties=PROPERTIES
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


def pinned_catalog(catalog, prefix, source, run_id=None):
    namespace = prefix + "_system" if source == "native" else "system"
    silver = prefix + "_silver" if source == "native" else "silver"
    publication_name = f"lakehouse.{namespace}.analytics_publications"
    publication = catalog.load_table((namespace, "analytics_publications"))
    snapshot = capture(publication)
    rows = read_snapshot(publication, snapshot).to_dicts()
    rows = [
        r
        for r in rows
        if r["schema_version"] == 3 and r["source_mode"] == "batch" and (run_id is None or r["run_id"] == run_id)
    ]
    if not rows:
        raise ValueError("Publish the requested OLTP catalog before building native log marts")
    if run_id is not None and len(rows) != 1:
        raise ValueError("Catalog run identity must identify one publication")
    selected = max(rows, key=lambda r: (r["published_at"], r["run_id"]))
    manifest = json.loads(selected["snapshots_json"])
    sources, frames = {publication_name: snapshot}, []
    for name in ("silver_products", "silver_product_variants"):
        physical = f"lakehouse.{silver}.{name}"
        if physical not in manifest:
            raise ValueError("Catalog publication is missing a required product snapshot")
        sources[physical] = manifest[physical]
        require_readable(catalog, {physical: manifest[physical]})
        frames.append(read_snapshot(catalog.load_table((silver, name)), manifest[physical]))
    return frames, sources, selected["run_id"]
