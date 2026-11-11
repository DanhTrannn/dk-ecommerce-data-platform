"""Schema-compatible Gold writes and immutable snapshot reads through PyIceberg."""

import re
import uuid
from datetime import date, datetime, UTC
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa
from pyiceberg.expressions import EqualTo, In
from pyiceberg.transforms import IdentityTransform

from lakehouse.oltp.gold.schema import GOLD_TABLE_DDL
from lakehouse.oltp.polars.schema import align_arrow, ddl_schema
from lakehouse.oltp.polars.writer import PROPERTIES
from .builders import schema


def read_snapshot(table, snapshot_id):
    if snapshot_id is None:
        return pl.from_arrow(pa.Table.from_pylist([], schema=table.schema().as_arrow()))
    return pl.from_arrow(table.scan(snapshot_id=snapshot_id).to_arrow())


def capture(table):
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot else None


def write_gold(catalog, prefix, name, frame, observed):
    table = catalog.create_table_if_not_exists((prefix + "_gold", name), schema=schema(name), properties=PROPERTIES)
    partition = re.search(r"PARTITIONED BY\s*\(([a-z_]+)\)", GOLD_TABLE_DDL[name])
    if partition and not table.spec().fields:
        with table.update_spec() as update:
            update.add_field(partition[1], IdentityTransform(), partition[1])
    arrow = align_arrow(frame.to_arrow(), table.schema())
    if name == "fact_inventory_daily_snapshot":
        day = observed.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
        arrow = align_arrow(frame.filter(pl.col("snapshot_date") == day).to_arrow(), table.schema())
        table.overwrite(arrow, overwrite_filter=EqualTo("snapshot_date", day))
    elif name == "dim_date":
        table.overwrite(arrow, overwrite_filter=In("year", [2025, 2026, 2027]))
    else:
        table.overwrite(arrow)
    return table


def persist_quality(catalog, prefix, summary, run_id, batch_date):
    table = catalog.create_table_if_not_exists(
        (prefix + "_system", "data_quality_results"),
        schema=ddl_schema("spark/system/quality_results.sql"),
        properties=PROPERTIES,
    )
    if not table.spec().fields:
        from pyiceberg.transforms import DayTransform

        with table.update_spec() as update:
            update.add_field("checked_at", DayTransform(), "checked_at_day")
    now = datetime.now(UTC)
    rows = []
    for result in summary["results"]:
        rows.append(
            dict(
                check_id=uuid.uuid4().hex,
                run_id=run_id,
                batch_date=date.fromisoformat(batch_date),
                phase="post_gold",
                check_name=result["check_name"],
                dataset=result["dataset"],
                severity=result["severity"],
                observed_value=result["observed_value"],
                expected_value=result["expected_value"],
                variance_percent=result["variance_percent"],
                failed_rows=result["failed_rows"],
                status="PASSED" if result["passed"] else "FAILED",
                details=result["details"],
                checked_at=now,
            )
        )
    if rows:
        table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))
