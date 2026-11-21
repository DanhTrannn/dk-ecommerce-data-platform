"""Merge the latest extract into bronze.<table>: the source rows as the source holds them, current version per key."""

from lakehouse.oltp.merge import merge_layer
from lakehouse.oltp.staging import staging_rows
from lakehouse.utils.contracts import align_arrow
from lakehouse.utils.iceberg import load_table


def merge_bronze(catalog, table, *, full):
    """Return the number of rows written to Bronze."""
    target = load_table(catalog, ("bronze", table.name))
    schema = target.schema().as_arrow()
    written = merge_layer(
        target, staging_rows(catalog, table), lambda rows: align_arrow(rows, schema), schema, table, full=full
    )
    print(f"BRONZE table={table.name} rows={written} full={full}", flush=True)
    return written
