import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import schema_to_pyarrow

from lakehouse.oltp.source import source_chunks
from lakehouse.utils.iceberg import load_table
from lakehouse.utils.postgres import source_read_transaction

# Rows per Iceberg commit; one batch is held in memory.
BATCH_ROWS = 200_000
LINEAGE_FIELDS = (pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),)


def align_arrow(data, schema):
    if not isinstance(schema, pa.Schema):
        schema = schema_to_pyarrow(schema)
    arrays = [
        data[field.name].cast(field.type)
        if field.name in data.column_names and data[field.name].null_count != len(data)
        else pa.nulls(len(data), type=field.type)
        for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def with_lineage(chunk, ingested_at):
    (ingested,) = LINEAGE_FIELDS
    return chunk.append_column(ingested, pa.array([ingested_at] * chunk.num_rows, ingested.type))


def write_staging(catalog, table, chunks, ingested_at):
    target = load_table(catalog, ("staging", table.name))
    columns = set(target.schema().column_names)
    target.delete(AlwaysTrue())
    buffer, buffered, written = [], 0, 0
    for chunk in chunks:
        chunk = with_lineage(chunk, ingested_at)
        # Appending would silently drop a column the table lacks; the init DAG adds it first.
        unknown = sorted(set(chunk.column_names) - columns)
        if unknown:
            raise ValueError(f"staging.{table.name} lacks source columns {unknown}; run the lakehouse_oltp_init DAG")
        buffer.append(chunk)
        buffered += chunk.num_rows
        if buffered >= BATCH_ROWS:
            target.append(align_arrow(pa.concat_tables(buffer), target.schema()))
            written, buffer, buffered = written + buffered, [], 0
    if buffered:
        target.append(align_arrow(pa.concat_tables(buffer), target.schema()))
        written += buffered
    return written


def staging_rows(catalog, table, columns=("*",)):
    target = load_table(catalog, ("staging", table.name))
    return target.scan(selected_fields=tuple(columns)).to_arrow_batch_reader()


def extract_table(catalog, connection, table, ingested_at, *, start=None, end=None, created_before=None):
    with source_read_transaction(connection):
        chunks = source_chunks(connection, table, start, end, created_before=created_before)
        written = write_staging(catalog, table, chunks, ingested_at)
    print(f"STAGING table={table.name} rows={written}", flush=True)
    return written
