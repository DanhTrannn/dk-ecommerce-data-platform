"""Merge Bronze into silver.<table>, the typed current state of each source row.

A full load takes all of Bronze. An incremental load takes the Bronze rows this load changed: every row of one
extract carries the same `_ingested_at`, and Bronze only keeps that value on the rows it upserted.
"""

import polars as pl
import pyarrow as pa
from pyiceberg.expressions import EqualTo

from lakehouse.oltp.merge import merge_layer
from lakehouse.oltp.sources import LINEAGE
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


def bronze_changes(catalog, table, *, full):
    """All of Bronze on a full load; otherwise the Bronze rows written by the latest extract."""
    bronze = load_table(catalog, ("bronze", table.name))
    if full:
        return bronze.scan().to_arrow_batch_reader()
    latest = load_table(catalog, ("staging", table.name)).scan(selected_fields=("_ingested_at",), limit=1).to_arrow()
    if not latest.num_rows:
        return pa.RecordBatchReader.from_batches(bronze.schema().as_arrow(), [])
    extracted_at = latest["_ingested_at"][0].as_py().isoformat()
    return bronze.scan(row_filter=EqualTo("_ingested_at", extracted_at)).to_arrow_batch_reader()


def merge_silver(catalog, table, *, full):
    """Return the number of rows written to Silver."""
    schema = silver_schema(table)
    target = load_table(catalog, ("silver", table.name))
    written = merge_layer(
        target, bronze_changes(catalog, table, full=full), lambda rows: to_silver(rows, table, schema), schema, table, full=full
    )
    print(f"SILVER table={table.name} rows={written} full={full}", flush=True)
    return written
