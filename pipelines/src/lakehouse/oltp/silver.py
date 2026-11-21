"""Merge a run's Bronze rows into silver.<table>, the current state of each source row."""

import polars as pl
import pyarrow as pa
from pyiceberg.expressions import In

from lakehouse.oltp.bronze import BRONZE_BATCH_ROWS, run_rows
from lakehouse.oltp.sources import LINEAGE
from lakehouse.utils.contracts import align_arrow, ddl_schema
from lakehouse.utils.iceberg import load_table
from lakehouse.utils.namespaces import namespace as ns


def silver_schema(table):
    return ddl_schema(f"contracts/silver/{table.name}.sql")


def to_silver(rows, table, schema):
    """Keep the configured columns and lineage, typing UUID strings as the 16-byte binary Silver stores."""
    frame = pl.from_arrow(rows).select([*table.columns, *LINEAGE])
    uuids = [field.name for field in schema if pa.types.is_binary(field.type) and frame.schema[field.name] == pl.String]
    frame = frame.with_columns(pl.col(name).str.replace_all("-", "").str.decode("hex") for name in uuids)
    return align_arrow(frame.to_arrow(), schema)


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


def replace_silver(target, reader, table, schema):
    """Full load: one overwrite commit, so readers see either the old or the new table, never a partial one."""
    written = 0

    def batches():
        nonlocal written
        for batch in reader:
            rows = to_silver(batch, table, schema)
            written += rows.num_rows
            yield from rows.to_batches()

    write = target.overwrite if target.current_snapshot() is not None else target.append
    write(pa.RecordBatchReader.from_batches(schema, batches()))
    return written


def upsert_silver(target, reader, table, schema):
    """Incremental run: upsert by primary key, never replacing a Silver row with an older cursor."""
    business = tuple(name for name in schema.names if not name.startswith("_"))
    written = 0
    for lot in lots(reader, BRONZE_BATCH_ROWS):
        delta = to_silver(lot, table, schema)
        current = target.scan(row_filter=In(table.pk, delta[table.pk].to_pylist()), selected_fields=business).to_arrow()
        delta = filter_current(delta, current, table)
        if delta.num_rows:
            target.upsert(delta, join_cols=[table.pk])
            written += delta.num_rows
    return written


def merge_silver(catalog, prefix, table, run_id, *, full):
    """Return the number of rows written to Silver."""
    schema = silver_schema(table)
    target = load_table(catalog, (ns(prefix, "silver"), table.name))
    reader = run_rows(catalog, prefix, table, run_id)
    written = replace_silver(target, reader, table, schema) if full else upsert_silver(target, reader, table, schema)
    print(f"SILVER table={table.name} rows={written} full={full}", flush=True)
    return written
