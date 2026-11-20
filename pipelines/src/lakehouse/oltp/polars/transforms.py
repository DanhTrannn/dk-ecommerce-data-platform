"""Pure Silver transformations and quarantine rows with analytical projections."""

import base64
from datetime import UTC, date, datetime
from decimal import Decimal
import json
import uuid

import polars as pl
import pyarrow as pa

from lakehouse.oltp.polars.schema import align_arrow, quarantine_schema
from lakehouse.oltp.polars.validation import validate_rows


def record_json(row):
    """Serialize row data safely for quarantine inspection."""
    def scalar(value):
        if isinstance(value, datetime):
            value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            return f'"{value.isoformat(timespec="milliseconds").replace("+00:00", "Z")}"'
        elif isinstance(value, date):
            return f'"{value.isoformat()}"'
        elif isinstance(value, (bytes, bytearray)):
            return f'"{base64.b64encode(value).decode()}"'
        elif isinstance(value, Decimal):
            return format(value, "f")
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    filtered = {k: v for k, v in row.items() if not k.startswith("_") and v is not None}
    items = [f'"{k}":{scalar(v)}' for k, v in sorted(filtered.items())]
    return "{" + ",".join(items) + "}"


def prepare_silver(page, table, schema, run_id, observed):
    frame = pl.from_arrow(page)
    order = [name for name in (table.cursor_field, "_silver_ingested_at", "_ingested_at_utc") if name in frame.columns]
    frame = frame.sort(order, nulls_last=False).unique(subset=[table.pk], keep="last", maintain_order=True)
    valid, invalid = validate_rows(frame, table)

    # Check required silver columns: missing required column must fail rather than silent null
    if hasattr(table, "silver_columns") and table.silver_columns:
        missing_req = set(table.silver_columns) - set(valid.columns)
        if missing_req:
            raise ValueError(f"Missing required silver columns in source data for {table.name}: {sorted(missing_req)}")
        # Analytical projection: keep only silver_columns
        keep_cols = [c for c in table.silver_columns if c in valid.columns]
        valid = valid.select(keep_cols)

    for field in schema:
        if pa.types.is_binary(field.type) and field.name in valid.columns and valid.schema[field.name] == pl.String:
            valid = valid.with_columns(
                pl.col(field.name).map_elements(lambda value: uuid.UUID(value).bytes, return_dtype=pl.Binary)
            )

    valid = valid.with_columns(
        pl.lit(observed).alias("_silver_ingested_at"),
        pl.lit(run_id).alias("_source_bronze_run_id"),
    )
    return align_arrow(valid.to_arrow(), schema), quarantine_rows(invalid, table.name, run_id, observed)


def quarantine_rows(invalid, table_name, run_id, observed):
    rows = [
        dict(
            record_data=record_json(row),
            violation_type=row["_violation_type"],
            violation_types=row["_violation_types"],
            violation_detail=f"Business rule violation in {table_name}",
            source_table=table_name,
            _run_id=run_id,
            _quarantined_at=observed,
        )
        for row in invalid.iter_rows(named=True)
    ]
    return pa.Table.from_pylist(rows, schema=quarantine_schema())


def filter_current(delta, current, table):
    if not delta.num_rows or not current.num_rows:
        return delta
    frame = (
        pl.from_arrow(delta)
        .join(
            pl.from_arrow(current).select(table.pk, pl.col(table.cursor_field).alias("_current_cursor")),
            on=table.pk,
            how="left",
        )
        .filter(pl.col("_current_cursor").is_null() | (pl.col(table.cursor_field) >= pl.col("_current_cursor")))
        .drop("_current_cursor")
    )
    return align_arrow(frame.to_arrow(), delta.schema)
