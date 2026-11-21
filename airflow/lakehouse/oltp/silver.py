"""Merge the latest extract into silver.<table>, the typed current state of each source row."""

import polars as pl
import pyarrow as pa

from lakehouse.oltp.merge import merge_layer
from lakehouse.oltp.sources import LINEAGE
from lakehouse.oltp.staging import staging_rows
from lakehouse.utils.contracts import align_arrow, ddl_schema
from lakehouse.utils.iceberg import load_table


def silver_schema(table):
    return ddl_schema(f"contracts/silver/{table.name}.sql")


def to_silver(rows, table, schema):
    """Keep the configured columns and lineage, typing UUID strings as the 16-byte binary Silver stores."""
    frame = pl.from_arrow(rows).select([*table.columns, *LINEAGE])
    uuids = [field.name for field in schema if pa.types.is_binary(field.type) and frame.schema[field.name] == pl.String]
    frame = frame.with_columns(pl.col(name).str.replace_all("-", "").str.decode("hex") for name in uuids)
    return align_arrow(frame.to_arrow(), schema)


def merge_silver(catalog, table, *, full):
    """Return the number of rows written to Silver."""
    schema = silver_schema(table)
    target = load_table(catalog, ("silver", table.name))
    written = merge_layer(
        target, staging_rows(catalog, table), lambda rows: to_silver(rows, table, schema), schema, table, full=full
    )
    print(f"SILVER table={table.name} rows={written} full={full}", flush=True)
    return written
