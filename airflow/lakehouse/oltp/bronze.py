from datetime import UTC, datetime

import polars as pl
import pyarrow as pa
from pyiceberg.expressions import EqualTo, In, IsNull, Or

from lakehouse.oltp.staging import BATCH_ROWS, align_arrow, staging_rows
from lakehouse.utils.iceberg import load_table

# A row that vanished from the source stays in Bronze with these set, so its deletion is kept as history.
DELETE_FIELDS = (pa.field("_is_deleted", pa.bool_()), pa.field("_deleted_at", pa.timestamp("us", tz="UTC")))
LIVE = Or(IsNull("_is_deleted"), EqualTo("_is_deleted", False))
# Bronze table property naming the columns the sync added and still has to fill by reloading the table.
RELOAD_PENDING = "lakehouse.reload-pending"


def filter_current(delta, current, table):
    """Drop rows that would replace a newer version: an older cursor, or the same cursor with no change."""
    if not delta.num_rows or not current.num_rows:
        return delta
    business = [
        name for name in delta.column_names if name in current.column_names
        and name not in {table.pk, table.cursor_field} and (not name.startswith("_") or name == "_is_deleted")
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


def lots(reader, rows):
    """Regroup record batches into tables of at least `rows` rows; the last one may be smaller."""
    buffer, buffered = [], 0
    for batch in reader:
        buffer.append(batch)
        buffered += batch.num_rows
        if buffered >= rows:
            yield pa.Table.from_batches(buffer)
            buffer, buffered = [], 0
    if buffered:
        yield pa.Table.from_batches(buffer)


def replace_table(target, reader, transform, schema, tombstones):
    """Write the whole table, and the deleted rows it keeps, in one commit, so readers never see a partial load."""
    written = 0

    def batches():
        nonlocal written
        for batch in reader:
            rows = transform(batch)
            written += rows.num_rows
            yield from rows.to_batches()
        yield from tombstones.to_batches()

    write = target.overwrite if target.current_snapshot() is not None else target.append
    write(pa.RecordBatchReader.from_batches(schema, batches()))
    return written


def upsert_table(target, reader, transform, schema, table):
    business = tuple(name for name in schema.names if not name.startswith("_") or name == "_is_deleted")
    written = 0
    for lot in lots(reader, BATCH_ROWS):
        delta = transform(lot)
        current = target.scan(row_filter=In(table.pk, delta[table.pk].to_pylist()), selected_fields=business).to_arrow()
        delta = filter_current(delta, current, table)
        if delta.num_rows:
            target.upsert(delta, join_cols=[table.pk])
            written += delta.num_rows
    return written


def with_values(rows, values):
    """Set each named column of `rows` to one value on every row."""
    for name, value in values.items():
        field = rows.schema.field(name)
        rows = rows.set_column(rows.schema.get_field_index(name), field, pa.array([value] * rows.num_rows, field.type))
    return rows


def live_keys(catalog, table):
    target = load_table(catalog, ("bronze", table.name))
    return target.scan(row_filter=LIVE, selected_fields=(table.pk,)).to_arrow()[table.pk].combine_chunks()


def mark_deleted(catalog, table, keys, deleted_at, ingested_at):
    """Flag the live Bronze rows of `keys` as deleted at `deleted_at`; rows already flagged keep their time."""
    if not len(keys):
        return 0
    target = load_table(catalog, ("bronze", table.name))
    schema = target.schema().as_arrow()
    rows = target.scan(row_filter=In(table.pk, keys.to_pylist())).filter(LIVE).to_arrow()
    if not rows.num_rows:
        return 0
    rows = with_values(
        align_arrow(rows, schema), {"_is_deleted": True, "_deleted_at": deleted_at, "_ingested_at": ingested_at}
    )
    target.upsert(rows, join_cols=[table.pk])
    return rows.num_rows


def full_load_tombstones(catalog, target, table, schema):
    """Return the Bronze rows a full load must keep: those absent from staging, flagged as deleted."""
    staged = pl.from_arrow(staging_rows(catalog, table, (table.pk, "_ingested_at")).read_all())
    ingested_at = staged["_ingested_at"].max() if staged.height else datetime.now(UTC)
    current = pl.from_arrow(target.scan(selected_fields=(table.pk,)).to_arrow())
    missing = current.join(staged.select(table.pk), on=table.pk, how="anti")[table.pk].to_list()
    if not missing:
        return schema.empty_table()
    rows = pl.from_arrow(target.scan(row_filter=In(table.pk, missing)).to_arrow())
    live = rows["_is_deleted"].is_null() | ~rows["_is_deleted"]
    rows = rows.with_columns(
        pl.when(live).then(True).otherwise(pl.col("_is_deleted")).alias("_is_deleted"),
        pl.when(live).then(pl.lit(ingested_at)).otherwise(pl.col("_deleted_at")).alias("_deleted_at"),
        pl.when(live).then(pl.lit(ingested_at)).otherwise(pl.col("_ingested_at")).alias("_ingested_at"),
    )
    return align_arrow(rows.to_arrow(), schema)


def merge_bronze(catalog, table, *, full):
    target = load_table(catalog, ("bronze", table.name))
    schema = target.schema().as_arrow()
    reader = staging_rows(catalog, table)
    transform = lambda rows: with_values(align_arrow(rows, schema), {"_is_deleted": False, "_deleted_at": None})  # noqa: E731
    if full:
        tombstones = full_load_tombstones(catalog, target, table, schema)
        written = replace_table(target, reader, transform, schema, tombstones)
        # A full load fills every column, so a pending reload is done. The flag goes only after the load committed.
        if reload_pending(target):
            with load_table(catalog, ("bronze", table.name)).transaction() as transaction:
                transaction.remove_properties(RELOAD_PENDING)
    else:
        written = upsert_table(target, reader, transform, schema, table)
    print(f"BRONZE table={table.name} rows={written} full={full}", flush=True)
    return written


def purge_bronze(catalog, table):
    """Delete the rows flagged as deleted for good, and return how many there were."""
    target = load_table(catalog, ("bronze", table.name))
    flagged = EqualTo("_is_deleted", True)
    count = target.scan(row_filter=flagged, selected_fields=(table.pk,)).to_arrow().num_rows
    if count:
        target.delete(flagged)
    print(f"PURGE table=bronze.{table.name} rows={count}", flush=True)
    return count


def reload_pending(target):
    """Return the columns of a Bronze table that the sync added and has not filled yet."""
    value = target.properties.get(RELOAD_PENDING, "")
    return value.split(",") if value else []
