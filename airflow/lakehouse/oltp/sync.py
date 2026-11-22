from datetime import UTC, datetime

import polars as pl

from lakehouse.oltp.bronze import live_keys, mark_deleted
from lakehouse.oltp.source import CHUNK_ROWS, source_chunks, source_keys
from lakehouse.oltp.staging import write_staging
from lakehouse.utils.postgres import source_read_transaction


def detect_deletes(catalog, connection, table, detected_at):
    """Flag the Bronze rows gone from the source, and stage the source rows Bronze lacks or has flagged.

    Detect flags its own table's Bronze directly: one writer per table, since parallel commits to a shared table
    conflict.
    """
    with source_read_transaction(connection):
        source = pl.DataFrame({table.pk: source_keys(connection, table)})
        bronze = pl.DataFrame({table.pk: live_keys(catalog, table)})
        deleted = bronze.join(source, on=table.pk, how="anti")[table.pk]
        restored = source.join(bronze, on=table.pk, how="anti")[table.pk].to_list()
        ingested_at = datetime.now(UTC)
        flagged = mark_deleted(catalog, table, deleted.to_arrow(), detected_at, ingested_at)
        chunks = (
            chunk
            for start in range(0, len(restored), CHUNK_ROWS)
            for chunk in source_chunks(connection, table, keys=restored[start:start + CHUNK_ROWS])
        )
        staged = write_staging(catalog, table, chunks, ingested_at)
    print(f"SYNC table={table.name} deleted={flagged} restored={staged}", flush=True)
    return {"deleted": flagged, "restored": staged}
