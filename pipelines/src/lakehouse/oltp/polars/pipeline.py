"""Bronze/Silver ingestion of the 27 configured source tables."""

from lakehouse.utils.namespaces import namespace as ns
import re
from datetime import UTC, datetime

import polars as pl

from lakehouse.oltp.polars.source import source_pages
from lakehouse.oltp.polars.writer import ingest_page
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.postgres import namespace_lock, source_query, source_read_transaction


def selected_tables(names=None):
    known = {table.name: table for table in SOURCE_TABLES}
    if names is None:
        return tuple(SOURCE_TABLES)
    if not names or len(names) != len(set(names)) or set(names) - known.keys():
        raise ValueError("Select unique tables from the configured 27-source whitelist")
    return tuple(known[name] for name in names)


def validate_run(prefix, run_id, bucket, page_size):
    if prefix != "serving" and not re.fullmatch(r"polars_batch_[a-z0-9_]{1,40}", prefix):
        raise ValueError("Use serving or a dedicated polars_batch_* namespace prefix")
    if run_id in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.~:+@=-]{1,250}", run_id):
        raise ValueError("Run ID must be a path-safe identifier")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid batch bucket")
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise ValueError("page_size must be between 1 and 10000")


def add_lineage(page, table, observed, run_id):
    return (
        pl.from_arrow(page)
        .with_columns(
            pl.lit("postgresql_ecommerce").alias("_source_system"),
            pl.lit("public").alias("_source_schema"),
            pl.lit(table.name).alias("_source_table"),
            pl.col(table.pk).cast(pl.String).alias("_source_primary_key"),
            pl.col(table.cursor_field).alias("_source_cursor_at"),
            pl.lit(observed).alias("_ingested_at_utc"),
            pl.lit(run_id).alias("_source_bronze_run_id"),
        )
        .to_arrow()
    )


def run_batch(catalog, connection, prefix, *, bucket, run_id, tables=None, start=None, end=None, page_size=10000):
    validate_run(prefix, run_id, bucket, page_size)
    specs = selected_tables(tables)
    if (start is None) != (end is None):
        raise ValueError("Both interval bounds are required")
    for table in specs:
        source_query(table, start, end)
    with namespace_lock(connection, prefix):
        return execute_batch(catalog, connection, prefix, bucket, run_id, specs, start, end, page_size)


def execute_batch(catalog, connection, prefix, bucket, run_id, specs, start, end, page_size):
    for layer in ("bronze", "silver", "quarantine"):
        catalog.create_namespace_if_not_exists(ns(prefix, layer))
    observed, results = datetime.now(UTC), {}
    with source_read_transaction(connection):
        for table in specs:
            count, pages, accepted, merged, quarantined_total = 0, 0, 0, 0, 0
            for source in source_pages(connection, table, start, end, page_size=page_size):
                page = add_lineage(source, table, observed, run_id)
                outcome = ingest_page(catalog, prefix, table, page, run_id, observed)
                count += page.num_rows
                pages += 1
                accepted += outcome.get("accepted", 0)
                merged += outcome.get("valid", 0)
                quarantined_total += outcome.get("quarantined", 0)

            bronze_count = catalog.load_table((ns(prefix, "bronze"), table.name)).scan().count()
            silver_count = catalog.load_table((ns(prefix, "silver"), table.silver_table)).scan().count()
            results[table.name] = {
                "source_rows": count,
                "pages": pages,
                "accepted": accepted,
                "valid": merged,
                "merged": merged,
                "quarantined": quarantined_total,
                "bronze_rows": bronze_count,
                "silver_rows": silver_count,
            }
            print(f"POLARS_BATCH table={table.name} source_rows={count}", flush=True)
    return {"namespace_prefix": prefix, "run_id": run_id, "tables": results, "published": False}
