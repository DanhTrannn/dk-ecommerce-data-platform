"""Bounded PostgreSQL pages with explicit Arrow types, including empty intervals."""

from datetime import UTC

import pyarrow as pa

from lakehouse.utils.postgres import page_query, source_query

PG_TYPES = {
    16: pa.bool_(),
    20: pa.int64(),
    21: pa.int32(),
    23: pa.int32(),
    25: pa.string(),
    1042: pa.string(),
    1043: pa.string(),
    2950: pa.string(),
    1114: pa.timestamp("us", tz="UTC"),
    1184: pa.timestamp("us", tz="UTC"),
    1082: pa.date32(),
    700: pa.float32(),
    701: pa.float64(),
    17: pa.binary(),
    114: pa.string(),
    3802: pa.string(),
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


def source_pages(connection, table, start=None, end=None, *, page_size=10000):
    if (start is None) != (end is None):
        raise ValueError("Both interval bounds are required")
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise ValueError("page_size must be between 1 and 10000")
    query, last = source_query(table, start, end), None
    while True:
        with connection.cursor() as cursor:
            cursor.execute(page_query(query, table.pk, last, page_size))
            description = cursor.description
            schema = arrow_source_schema(description)
            if not {table.pk, table.cursor_field}.issubset(schema.names):
                raise ValueError("Source is missing the configured primary key/cursor")
            rows = [dict(zip(schema.names, row)) for row in cursor.fetchall()]
        for row in rows:
            for column in description:
                value = row[column.name]
                if value is None:
                    continue
                if column.type_code == 2950:
                    value = str(value)
                elif column.type_code in (1114, 1184):
                    value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
                elif column.type_code == 17:
                    value = bytes(value)
                row[column.name] = value
        yield pa.Table.from_pylist(rows, schema=schema)
        if len(rows) < page_size:
            break
        last = rows[-1][table.pk]
