"""Append every source page of a run to bronze.<table>, batched, with `_ingested_at` and `_run_id`."""

import pyarrow as pa
from pyiceberg.expressions import EqualTo

from lakehouse.utils.contracts import align_arrow
from lakehouse.oltp.source import source_pages
from lakehouse.utils.iceberg import PROPERTIES, load_table
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.postgres import source_read_transaction

BRONZE_BATCH_ROWS = 200_000
# Exact `_run_id` bounds let scans and deletes of one run skip other runs' files without reading them.
BRONZE_PROPERTIES = PROPERTIES | {"write.metadata.metrics.column._run_id": "full"}
LINEAGE_FIELDS = (pa.field("_ingested_at", pa.timestamp("us", tz="UTC")), pa.field("_run_id", pa.string()))


def with_lineage(page, run_id, ingested_at):
    ingested, run = LINEAGE_FIELDS
    return page.append_column(ingested, pa.array([ingested_at] * page.num_rows, ingested.type)).append_column(
        run, pa.array([run_id] * page.num_rows, run.type)
    )


def write_bronze(catalog, prefix, table, pages, run_id, ingested_at):
    """Replace this run's rows from an earlier attempt, then append the pages in batches; return rows written."""
    target = load_table(catalog, (ns(prefix, "bronze"), table.name))
    columns = set(target.schema().column_names)
    earlier_attempt = EqualTo("_run_id", run_id)
    if any(target.scan(row_filter=earlier_attempt).plan_files()):
        target.delete(earlier_attempt)
    buffer, buffered, written = [], 0, 0
    for page in pages:
        page = with_lineage(page, run_id, ingested_at)
        # Writing would silently drop a column the table lacks; the init DAG adds it first.
        unknown = sorted(set(page.column_names) - columns)
        if unknown:
            raise ValueError(f"bronze.{table.name} lacks source columns {unknown}; run the lakehouse_init DAG")
        buffer.append(page)
        buffered += page.num_rows
        if buffered >= BRONZE_BATCH_ROWS:
            target.append(align_arrow(pa.concat_tables(buffer), target.schema()))
            written, buffer, buffered = written + buffered, [], 0
    if buffered:
        target.append(align_arrow(pa.concat_tables(buffer), target.schema()))
        written += buffered
    return written


def run_rows(catalog, prefix, table, run_id, columns=("*",)):
    """Stream this run's Bronze rows."""
    target = load_table(catalog, (ns(prefix, "bronze"), table.name))
    return target.scan(row_filter=EqualTo("_run_id", run_id), selected_fields=tuple(columns)).to_arrow_batch_reader()


def ingest_table(catalog, connection, prefix, table, run_id, ingested_at, *, start=None, end=None, created_before=None, page_size=10000):
    """Copy one source table's selected rows to Bronze inside its own repeatable-read source transaction."""
    with source_read_transaction(connection):
        pages = source_pages(connection, table, start, end, created_before=created_before, page_size=page_size)
        written = write_bronze(catalog, prefix, table, pages, run_id, ingested_at)
    print(f"BRONZE table={table.name} rows={written}", flush=True)
    return written
