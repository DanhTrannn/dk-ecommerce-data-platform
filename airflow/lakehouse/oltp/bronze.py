"""bronze.<table> is a staging copy of the latest extract: each load truncates it, then appends the source chunks.

Reloading therefore always gives the same result, so a failed task is simply run again.
"""

import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue

from lakehouse.oltp.source import source_chunks
from lakehouse.utils.contracts import align_arrow
from lakehouse.utils.iceberg import load_table
from lakehouse.utils.postgres import source_read_transaction

BRONZE_BATCH_ROWS = 200_000
LINEAGE_FIELDS = (pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),)


def with_lineage(chunk, ingested_at):
    (ingested,) = LINEAGE_FIELDS
    return chunk.append_column(ingested, pa.array([ingested_at] * chunk.num_rows, ingested.type))


def write_bronze(catalog, table, chunks, ingested_at):
    """Truncate bronze.<table>, then append the chunks in batches; return rows written."""
    target = load_table(catalog, ("bronze", table.name))
    columns = set(target.schema().column_names)
    target.delete(AlwaysTrue())
    buffer, buffered, written = [], 0, 0
    for chunk in chunks:
        chunk = with_lineage(chunk, ingested_at)
        # Writing would silently drop a column the table lacks; the init DAG adds it first.
        unknown = sorted(set(chunk.column_names) - columns)
        if unknown:
            raise ValueError(f"bronze.{table.name} lacks source columns {unknown}; run the lakehouse_oltp_init DAG")
        buffer.append(chunk)
        buffered += chunk.num_rows
        if buffered >= BRONZE_BATCH_ROWS:
            target.append(align_arrow(pa.concat_tables(buffer), target.schema()))
            written, buffer, buffered = written + buffered, [], 0
    if buffered:
        target.append(align_arrow(pa.concat_tables(buffer), target.schema()))
        written += buffered
    return written


def bronze_rows(catalog, table, columns=("*",)):
    """Stream the rows of the latest extract."""
    target = load_table(catalog, ("bronze", table.name))
    return target.scan(selected_fields=tuple(columns)).to_arrow_batch_reader()


def ingest_table(catalog, connection, table, ingested_at, *, start=None, end=None, created_before=None):
    """Copy one source table's selected rows to Bronze inside its own repeatable-read source transaction."""
    with source_read_transaction(connection):
        chunks = source_chunks(connection, table, start, end, created_before=created_before)
        written = write_bronze(catalog, table, chunks, ingested_at)
    print(f"BRONZE table={table.name} rows={written}", flush=True)
    return written
