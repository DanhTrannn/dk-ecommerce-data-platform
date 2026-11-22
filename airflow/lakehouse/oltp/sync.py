from datetime import UTC, datetime

import polars as pl

from lakehouse.oltp.bronze import live_keys, mark_deleted
from lakehouse.oltp.drift import refuse, source_columns
from lakehouse.oltp.schema import evolve_schema
from lakehouse.oltp.source import CHUNK_ROWS, source_chunks, source_keys
from lakehouse.oltp.staging import write_staging
from lakehouse.utils.iceberg import load_table
from lakehouse.utils.postgres import identifier, source_read_transaction


def sync_schema(catalog, connection, table):
    """Apply the safe column changes to staging and Bronze, then refuse a new column nobody has reviewed."""
    with source_read_transaction(connection):
        source = source_columns(connection, table)
    drift = evolve_schema(catalog, table, source)
    print(f"SCHEMA table={table.name} evolved={list(drift.evolve)}", flush=True)
    if catalog.table_exists(("silver", table.name)):
        bronze = load_table(catalog, ("bronze", table.name)).schema().column_names
        silver = catalog.load_table(("silver", table.name)).schema().column_names
        missing = [name for name in table.columns if name in bronze and name not in silver]
        if missing:
            print(f"SCHEMA table={table.name} silver_missing={missing}", flush=True)
    refuse(table, drift, ("unlisted",))
    return drift


def needs_reload(catalog, connection, table):
    """Return the configured columns that are NULL in every Bronze row but not at the source.

    A column the sync just added is NULL in Bronze until the table is reloaded. Reading this from the data, not
    from the run that added it, keeps the reload going after a failed or cleared run.
    """
    target = load_table(catalog, ("bronze", table.name))
    files = [file for file in target.inspect.files().to_pylist() if file["content"] == 0]
    if not sum(file["record_count"] for file in files):
        return []
    schema, columns = target.schema(), []
    for name in table.columns:
        if name not in schema.column_names:
            continue
        field_id = schema.find_field(name).field_id
        # A file written before the column existed has no statistics for it, and holds only NULLs there.
        if all(dict(file["null_value_counts"] or []).get(field_id, file["record_count"]) == file["record_count"]
               for file in files):
            with connection.cursor() as cursor:
                cursor.execute(
                    f'SELECT 1 FROM "public".{identifier(table.name)} WHERE {identifier(name)} IS NOT NULL LIMIT 1'
                )
                if cursor.fetchone():
                    columns.append(name)
    return columns


def detect_deletes(catalog, connection, table, detected_at):
    """Flag the Bronze rows gone from the source, and stage the source rows Bronze lacks or has flagged.

    Detect flags its own table's Bronze directly: one writer per table, since parallel commits to a shared table
    conflict. A table with a column still to fill (see needs_reload) is staged whole instead.
    """
    with source_read_transaction(connection):
        reload = needs_reload(catalog, connection, table)
        source = pl.DataFrame({table.pk: source_keys(connection, table)})
        bronze = pl.DataFrame({table.pk: live_keys(catalog, table)})
        deleted = bronze.join(source, on=table.pk, how="anti")[table.pk]
        restored = source.join(bronze, on=table.pk, how="anti")[table.pk].to_list()
        ingested_at = datetime.now(UTC)
        flagged = mark_deleted(catalog, table, deleted.to_arrow(), detected_at, ingested_at)
        if reload:
            bronze_columns = load_table(catalog, ("bronze", table.name)).schema().column_names
            chunks = source_chunks(connection, table, columns=[name for name in table.columns if name in bronze_columns])
        else:
            chunks = (
                chunk
                for start in range(0, len(restored), CHUNK_ROWS)
                for chunk in source_chunks(connection, table, keys=restored[start:start + CHUNK_ROWS])
            )
        staged = write_staging(catalog, table, chunks, ingested_at)
    print(f"SYNC table={table.name} deleted={flagged} restored={staged} reload={reload}", flush=True)
    return {"deleted": flagged, "restored": staged, "reload": reload}
