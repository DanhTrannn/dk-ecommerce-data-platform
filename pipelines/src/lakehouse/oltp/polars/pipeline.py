"""Bronze/Silver ingestion of the 27 configured source tables."""

from lakehouse.utils.namespaces import namespace as ns
import re
from datetime import UTC, datetime

import polars as pl
import pyarrow.parquet as pq

from lakehouse.oltp.polars.privacy import required_salt
from lakehouse.oltp.polars.source import source_pages
from lakehouse.oltp.polars.writer import PROPERTIES, evolve_bronze, guard_bronze, ingest_page
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.postgres import namespace_lock, source_query


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


def add_lineage(page, table, observed):
    return (
        pl.from_arrow(page)
        .with_columns(
            pl.lit("postgresql_ecommerce").alias("_source_system"),
            pl.lit("public").alias("_source_schema"),
            pl.lit(table.name).alias("_source_table"),
            pl.col(table.pk).cast(pl.String).alias("_source_primary_key"),
            pl.col(table.cursor_field).alias("_source_cursor_at"),
            pl.lit(observed).alias("_ingested_at_utc"),
        )
        .to_arrow()
    )


def run_batch(catalog, connection, prefix, *, bucket, run_id, tables=None, start=None, end=None, page_size=10000):
    validate_run(prefix, run_id, bucket, page_size)
    specs, salt = selected_tables(tables), required_salt()
    if (start is None) != (end is None):
        raise ValueError("Both interval bounds are required")
    for table in specs:
        source_query(table, start, end)
    with namespace_lock(connection, prefix):
        return execute_batch(catalog, connection, prefix, bucket, run_id, specs, start, end, page_size, salt)


def execute_batch(catalog, connection, prefix, bucket, run_id, specs, start, end, page_size, salt):
    for layer in ("bronze", "silver", "quarantine"):
        catalog.create_namespace_if_not_exists(ns(prefix, layer))
    for table in specs:
        target = (ns(prefix, "bronze"), table.name)
        if catalog.table_exists(target):
            guard_bronze(catalog.load_table(target))
    observed, staged, results = datetime.now(UTC), {}, {}
    for table in specs:
        pages, count = [], 0
        for index, source in enumerate(source_pages(connection, table, start, end, page_size=page_size)):
            page = add_lineage(source, table, observed)
            bronze = catalog.create_table_if_not_exists(
                (ns(prefix, "bronze"), table.name), schema=page.schema, properties=PROPERTIES
            )
            evolve_bronze(bronze, page.schema)
            path = f"s3://{bucket}/landing/polars-batch/{prefix}/{run_id}/{table.name}/page-{index:06}.parquet"
            with bronze.io.new_output(path).create(overwrite=True) as stream:
                pq.write_table(page, stream)
            pages.append(path)
            count += page.num_rows
        staged[table.name] = pages
        results[table.name] = {"source_rows": count, "pages": len(pages), "valid": 0, "quarantined": 0}
    # No Bronze/Silver data commit until every selected source table is staged successfully.
    for table in specs:
        bronze = catalog.load_table((ns(prefix, "bronze"), table.name))
        for path in staged[table.name]:
            with bronze.io.new_input(path).open() as stream:
                page = pq.read_table(stream)
            outcome = ingest_page(catalog, prefix, table, page, run_id, observed, salt)
            for name in ("valid", "quarantined"):
                results[table.name][name] += outcome[name]
        results[table.name].update(
            bronze_rows=catalog.load_table((ns(prefix, "bronze"), table.name)).scan().count(),
            silver_rows=catalog.load_table((ns(prefix, "silver"), table.silver_table)).scan().count(),
        )
        print(f"POLARS_BATCH table={table.name} source_rows={results[table.name]['source_rows']}", flush=True)
    return {"namespace_prefix": prefix, "run_id": run_id, "tables": results, "published": False}
