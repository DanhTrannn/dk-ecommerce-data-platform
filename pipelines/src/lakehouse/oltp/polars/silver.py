"""Merge a run's Bronze rows into silver.<table>, the current state of each source row."""

import pyarrow as pa
from pyiceberg.expressions import In

from lakehouse.oltp.polars.bronze import BRONZE_BATCH_ROWS, run_rows
from lakehouse.oltp.polars.schema import silver_schema
from lakehouse.oltp.polars.transforms import filter_current, to_silver
from lakehouse.utils.iceberg import PROPERTIES, load_or_create
from lakehouse.utils.namespaces import namespace as ns


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
    target = load_or_create(catalog, (ns(prefix, "silver"), table.name), schema=schema, properties=PROPERTIES)
    reader = run_rows(catalog, prefix, table, run_id) or pa.RecordBatchReader.from_batches(schema, [])
    written = replace_silver(target, reader, table, schema) if full else upsert_silver(target, reader, table, schema)
    print(f"SILVER table={table.name} rows={written} full={full}", flush=True)
    return written
