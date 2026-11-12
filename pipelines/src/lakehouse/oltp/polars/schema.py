"""Translate the reviewed declarative Iceberg column contracts."""

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
    "DATE": pa.date32(),
    "DOUBLE": pa.float64(),
}


def split_types(value):
    parts, depth, start = [], 0, 0
    for index, char in enumerate(value):
        depth += (char == "<") - (char == ">")
        if char == "," and depth == 0:
            parts.append(value[start:index].strip())
            start = index + 1
    if depth != 0:
        raise ValueError("Unbalanced schema type")
    return parts + [value[start:].strip()]


def arrow_type(value):
    value = value.strip()
    if value in TYPES:
        return TYPES[value]
    if value.startswith("STRUCT<") and value.endswith(">"):
        fields = []
        for item in split_types(value[7:-1]):
            name, kind = item.split(":", 1)
            fields.append(pa.field(name.strip().strip("`"), arrow_type(kind)))
        return pa.struct(fields)
    if value.startswith("MAP<") and value.endswith(">"):
        key, item = split_types(value[4:-1])
        return pa.map_(arrow_type(key), arrow_type(item))
    raise ValueError("Unsupported column type in schema contract")


def ddl_schema(resource):
    ddl = load_sql(resource)
    match = re.search(r"\(\s*(.*?)\)\s*USING\s+iceberg", ddl, re.S | re.I)
    if not match:
        raise ValueError("Unsupported table schema contract")
    fields = []
    for column in match.group(1).splitlines():
        if not column.strip():
            continue
        part = re.fullmatch(r"\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\s+(.+?)\s*,?\s*", column)
        if part is None:
            raise ValueError("Unsupported column in schema contract")
        kind = re.sub(r"\s+COMMENT\s+'(?:[^']|'')*'", "", part[2]).rstrip(",").strip()
        fields.append(pa.field(part[1], arrow_type(kind)))
    return pa.schema(fields)


def silver_schema(table):
    return ddl_schema(f"contracts/silver/{table.silver_table}.sql")


def quarantine_schema():
    return ddl_schema("contracts/silver/silver_quarantine.sql")


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
