"""Append persisted analytical quality results to Iceberg."""

import uuid
from datetime import UTC, date, datetime
import pyarrow as pa
from pyiceberg.transforms import DayTransform
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.oltp.polars.schema import ddl_schema
from lakehouse.oltp.polars.writer import PROPERTIES


def persist_quality(catalog, prefix, summary, run_id, batch_date, *, phase="post_gold"):
    table = catalog.create_table_if_not_exists(
        (ns(prefix, "system"), "data_quality_results"),
        schema=ddl_schema("contracts/system/quality_results.sql"),
        properties=PROPERTIES,
    )
    if not table.spec().fields:
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
                phase=phase,
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
