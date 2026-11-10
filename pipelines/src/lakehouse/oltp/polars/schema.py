"""Translate the existing schema contract without importing or running Spark."""

import re

import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow

from lakehouse.utils.sql import load_sql

TYPES = {
    "BIGINT": pa.int64(),
    "INT": pa.int32(),
    "STRING": pa.string(),
    "BINARY": pa.binary(),
    "BOOLEAN": pa.bool_(),
    "TIMESTAMP": pa.timestamp("us", tz="UTC"),
    "ARRAY<STRING>": pa.list_(pa.string()),
}


def ddl_schema(resource):
    ddl = load_sql(resource)
    match = re.search(r"\(\s*(.*?)\)\s*USING\s+iceberg", ddl, re.S | re.I)
    if not match:
        raise ValueError("Unsupported Silver schema contract")
    fields = []
    for column in match.group(1).split(","):
        parts = column.strip().split()
        if len(parts) != 2 or parts[1] not in TYPES:
            raise ValueError("Unsupported column in Silver schema contract")
        fields.append(pa.field(parts[0], TYPES[parts[1]]))
    return pa.schema(fields)


def silver_schema(table):
    return ddl_schema(f"spark/silver/{table.silver_table}.sql")


def quarantine_schema():
    return ddl_schema("spark/silver/silver_quarantine.sql")


def align_arrow(data, schema):
    if not isinstance(schema, pa.Schema):
        schema = schema_to_pyarrow(schema)
    arrays = [
        data[field.name].cast(field.type) if field.name in data.column_names else pa.nulls(len(data), type=field.type)
        for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)
