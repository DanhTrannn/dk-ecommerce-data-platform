"""Pure Silver transformations and quarantine rows with analytical projections."""

import base64
from datetime import UTC, date, datetime
from decimal import Decimal
import json
import uuid

import polars as pl
import pyarrow as pa

from lakehouse.oltp.polars.schema import align_arrow, quarantine_schema
from lakehouse.oltp.polars.validation import STATUS_RULES, validate_rows


VIOLATION_FIELDS = {
    "negative_price": ("price_vnd",),
    "negative_payment": ("amount_vnd",),
    "negative_refund": ("amount_vnd",),
    "negative_inventory": ("on_hand",),
    "negative_total_cost": ("total_cost_vnd",),
    "negative_total_items_count": ("total_items_count",),
    "invalid_quantity": ("quantity",),
    "invalid_rating": ("rating",),
    "invalid_discount_percentage": ("discount_type", "discount_value"),
    "order_total_mismatch": ("subtotal_vnd", "discount_amount_vnd", "shipping_fee_vnd", "total_vnd"),
    "order_item_total_mismatch": ("quantity", "unit_price_vnd", "line_total_vnd"),
    "inbound_item_total_mismatch": ("quantity", "unit_cost_vnd", "total_cost_vnd"),
    **{label: (field,) for field, label, _ in STATUS_RULES.values()},
}


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
    return align_arrow(valid.to_arrow(), schema), quarantine_rows(invalid, table, run_id, observed)


def quarantine_rows(invalid, table, run_id, observed):
    rows = []
    for row in invalid.iter_rows(named=True):
        fields = {table.pk, table.cursor_field}
        for label in row["_violation_types"]:
            if label in VIOLATION_FIELDS:
                fields.update(VIOLATION_FIELDS[label])
            elif label.startswith(("missing_required_", "negative_")):
                fields.add(label.removeprefix("missing_required_").removeprefix("negative_"))
        rows.append(dict(
            record_data=record_json({name: row[name] for name in fields & row.keys()}),
            violation_type=row["_violation_type"],
            violation_types=row["_violation_types"],
            violation_detail=f"Business rule violation in {table.name}",
            source_table=table.name,
            _run_id=run_id,
            _quarantined_at=observed,
        ))
    return pa.Table.from_pylist(rows, schema=quarantine_schema())


def filter_current(delta, current, table):
    if not delta.num_rows or not current.num_rows:
        return delta
    business = [
        name for name in delta.column_names if name in current.column_names
        and name not in {table.pk, table.cursor_field} and not name.startswith("_")
    ]
    previous = pl.col("_current_cursor")
    cursor = pl.col(table.cursor_field)
    changed = pl.any_horizontal([pl.col(name).ne_missing(pl.col("_current_" + name)) for name in business]) if business else pl.lit(True)
    frame = pl.from_arrow(delta).join(
        pl.from_arrow(current).select(
            table.pk, cursor.alias("_current_cursor"),
            *(pl.col(name).alias("_current_" + name) for name in business),
        ),
        on=table.pk,
        how="left",
    ).filter(previous.is_null() | (cursor > previous) | ((cursor == previous) & changed)).select(delta.column_names)
    return align_arrow(frame.to_arrow(), delta.schema)
