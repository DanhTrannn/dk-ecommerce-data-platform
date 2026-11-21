"""Merge the latest extract into a layer table: replace it on a full load, upsert it on an incremental one.

Bronze and Silver merge the same way; only the row transform differs.
"""

import polars as pl
import pyarrow as pa
from pyiceberg.expressions import In

from lakehouse.utils.contracts import align_arrow

BATCH_ROWS = 200_000  # rows per Iceberg commit; one batch is held in memory


def filter_current(delta, current, table):
    """Drop the rows that would replace a newer version: an older cursor, or the same cursor with nothing changed."""
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


def lots(reader, rows):
    """Regroup a batch stream into tables of at least `rows` rows (the last may be smaller)."""
    buffer, buffered = [], 0
    for batch in reader:
        buffer.append(batch)
        buffered += batch.num_rows
        if buffered >= rows:
            yield pa.Table.from_batches(buffer)
            buffer, buffered = [], 0
    if buffered:
        yield pa.Table.from_batches(buffer)


def replace_table(target, reader, transform, schema):
    """Full load: one overwrite commit, so readers see either the old or the new table, never a partial one."""
    written = 0

    def batches():
        nonlocal written
        for batch in reader:
            rows = transform(batch)
            written += rows.num_rows
            yield from rows.to_batches()

    write = target.overwrite if target.current_snapshot() is not None else target.append
    write(pa.RecordBatchReader.from_batches(schema, batches()))
    return written


def upsert_table(target, reader, transform, schema, table):
    """Incremental load: upsert by primary key, never replacing a row with an older version."""
    business = tuple(name for name in schema.names if not name.startswith("_"))
    written = 0
    for lot in lots(reader, BATCH_ROWS):
        delta = transform(lot)
        current = target.scan(row_filter=In(table.pk, delta[table.pk].to_pylist()), selected_fields=business).to_arrow()
        delta = filter_current(delta, current, table)
        if delta.num_rows:
            target.upsert(delta, join_cols=[table.pk])
            written += delta.num_rows
    return written


def merge_layer(target, reader, transform, schema, table, *, full):
    if full:
        return replace_table(target, reader, transform, schema)
    return upsert_table(target, reader, transform, schema, table)
