from datetime import UTC

import pyarrow as pa

from lakehouse.utils.postgres import source_keys_query, source_query

# Rows per fetch: each chunk is held as Python rows until it becomes an Arrow table.
CHUNK_ROWS = 10_000

PG_TYPES = {
    16: pa.bool_(),
    20: pa.int64(),
    21: pa.int32(),
    23: pa.int32(),
    25: pa.string(),
    1042: pa.string(),
    1043: pa.string(),
    2950: pa.string(),  # uuid, as text
    1114: pa.timestamp("us", tz="UTC"),
    1184: pa.timestamp("us", tz="UTC"),
    1082: pa.date32(),
    700: pa.float32(),
    701: pa.float64(),
    17: pa.binary(),
}


def arrow_source_schema(description):
    fields = []
    for column in description:
        dtype = PG_TYPES.get(column.type_code)
        if column.type_code == 1700 and column.precision and column.scale is not None:
            dtype = pa.decimal128(column.precision, column.scale)
        if dtype is None:
            raise ValueError(f"Unsupported PostgreSQL type for source column {column.name}")
        fields.append(pa.field(column.name, dtype))
    return pa.schema(fields)


def to_arrow(rows, description):
    schema = arrow_source_schema(description)
    records = []
    for row in rows:
        record = {}
        for column, value in zip(description, row):
            if value is not None:
                if column.type_code == 2950:
                    value = str(value)
                elif column.type_code in (1114, 1184):
                    value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            record[column.name] = value
        records.append(record)
    return pa.Table.from_pylist(records, schema=schema)


def source_chunks(connection, table, start=None, end=None, *, created_before=None, keys=None, columns=None):
    """Yield the source rows in chunks of CHUNK_ROWS, or one empty chunk when there are none.

    The named cursor keeps the result on the server and needs the transaction source_read_transaction opens.
    """
    if (start is None) != (end is None):
        raise ValueError("Both interval bounds are required")
    with connection.cursor(name=f"source_{table.name}") as cursor:
        query = source_query(table, start, end, created_before=created_before, keys=keys is not None, columns=columns)
        cursor.execute(query, None if keys is None else {"keys": keys})
        while True:
            rows = cursor.fetchmany(CHUNK_ROWS)
            chunk = to_arrow(rows, cursor.description)
            if not {table.pk, table.cursor_field}.issubset(chunk.column_names):
                raise ValueError("Source is missing the configured primary key/cursor")
            yield chunk
            if len(rows) < CHUNK_ROWS:
                break


def source_keys(connection, table):
    """Return every primary key of the source table, read in chunks on a named cursor."""
    chunks = []
    with connection.cursor(name=f"keys_{table.name}") as cursor:
        cursor.execute(source_keys_query(table))
        while True:
            rows = cursor.fetchmany(CHUNK_ROWS)
            chunks.append(to_arrow(rows, cursor.description)[table.pk])
            if len(rows) < CHUNK_ROWS:
                break
    return pa.chunked_array(chunks, pa.int64()).combine_chunks()
