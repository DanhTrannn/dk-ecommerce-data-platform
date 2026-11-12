"""Pure Silver transformations and privacy-safe quarantine rows."""

import uuid

import polars as pl
import pyarrow as pa

from lakehouse.oltp.polars.privacy import PII_PATTERN, record_json, token_expr, token_name
from lakehouse.oltp.polars.schema import align_arrow, quarantine_schema
from lakehouse.oltp.polars.validation import validate_rows


def prepare_silver(page, table, schema, run_id, observed, salt):
    frame = pl.from_arrow(page)
    order = [name for name in (table.cursor_field, "_silver_ingested_at", "_ingested_at_utc") if name in frame.columns]
    frame = frame.sort(order, nulls_last=False).unique(subset=[table.pk], keep="last", maintain_order=True)
    valid, invalid = validate_rows(frame, table)
    for name in table.pseudonymize:
        expression = token_expr(name, salt) if name in valid.columns else pl.lit(None, dtype=pl.String)
        valid = valid.with_columns(expression.alias(token_name(name)))
        if name in valid.columns:
            valid = valid.drop(name)
    for field in schema:
        if pa.types.is_binary(field.type) and field.name in valid.columns and valid.schema[field.name] == pl.String:
            valid = valid.with_columns(
                pl.col(field.name).map_elements(lambda value: uuid.UUID(value).bytes, return_dtype=pl.Binary)
            )
    valid = valid.with_columns(
        pl.lit(observed if table.pseudonymize else None, dtype=pl.Datetime("us", "UTC")).alias("_pii_pseudonymized_at"),
        pl.lit(observed).alias("_silver_ingested_at"),
        pl.lit(run_id).alias("_source_bronze_run_id"),
    )
    return align_arrow(valid.to_arrow(), schema), quarantine_rows(invalid, table.name, run_id, observed, salt)


def quarantine_rows(invalid, table_name, run_id, observed, salt):
    safe = invalid.with_columns(
        [token_expr(name, salt).alias(name) for name in invalid.columns if PII_PATTERN.search(name)]
    )
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
        for row in safe.iter_rows(named=True)
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
