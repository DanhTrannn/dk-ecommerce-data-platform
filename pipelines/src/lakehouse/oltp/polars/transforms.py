"""Pure Bronze-to-Silver transformations."""

import polars as pl
import pyarrow as pa

from lakehouse.oltp.polars.schema import align_arrow
from lakehouse.utils.config import LINEAGE


def to_silver(rows, table, schema):
    """Keep the configured columns and lineage, typing UUID strings as the 16-byte binary Silver stores."""
    frame = pl.from_arrow(rows).select([*table.columns, *LINEAGE])
    uuids = [field.name for field in schema if pa.types.is_binary(field.type) and frame.schema[field.name] == pl.String]
    frame = frame.with_columns(pl.col(name).str.replace_all("-", "").str.decode("hex") for name in uuids)
    return align_arrow(frame.to_arrow(), schema)


def filter_current(delta, current, table):
    if not delta.num_rows or not current.num_rows:
        return delta
    business = [
        name for name in delta.column_names if name in current.column_names
        and name not in {table.pk, table.cursor_field} and not name.startswith("_")
    ]
    previous = pl.col("_current_cursor")
    cursor = pl.col(table.cursor_field)
    changed = pl.any_horizontal([pl.col(name).ne_missing(pl.col("_current_" + name)) for name in business]) if business else pl.lit(True)
    frame = pl.from_arrow(delta).join(
        pl.from_arrow(current).select(
            table.pk, cursor.alias("_current_cursor"),
            *(pl.col(name).alias("_current_" + name) for name in business),
        ),
        on=table.pk,
        how="left",
    ).filter(previous.is_null() | (cursor > previous) | ((cursor == previous) & changed)).select(delta.column_names)
    return align_arrow(frame.to_arrow(), delta.schema)
