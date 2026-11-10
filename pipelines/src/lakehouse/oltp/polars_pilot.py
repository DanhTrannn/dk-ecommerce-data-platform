"""Categories-only migration pilot. Never writes the serving namespaces/publications."""

import hashlib
import re
import uuid
from datetime import UTC, datetime

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import In

from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.postgres import page_query, source_query

CATEGORY = next(table for table in SOURCE_TABLES if table.name == "categories")
SOURCE_SCHEMA = pa.schema(
    [
        ("category_id", pa.int64()),
        ("public_id", pa.string()),
        ("parent_category_id", pa.int64()),
        ("code", pa.string()),
        ("name", pa.string()),
        ("is_active", pa.bool_()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)
BRONZE_SCHEMA = (
    SOURCE_SCHEMA.append(pa.field("_source_system", pa.string()))
    .append(pa.field("_source_schema", pa.string()))
    .append(pa.field("_source_table", pa.string()))
    .append(pa.field("_source_primary_key", pa.string()))
    .append(pa.field("_source_cursor_at", pa.timestamp("us", tz="UTC")))
    .append(pa.field("_ingested_at_utc", pa.timestamp("us", tz="UTC")))
)
SILVER_SCHEMA = pa.schema(
    [
        ("category_id", pa.int64()),
        ("public_id", pa.binary()),
        ("parent_category_id", pa.int64()),
        ("code", pa.string()),
        ("slug", pa.string()),
        ("name", pa.string()),
        ("is_active", pa.bool_()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
        ("_silver_ingested_at", pa.timestamp("us", tz="UTC")),
        ("_source_bronze_run_id", pa.string()),
    ]
)


def validate_namespace(namespace):
    if not re.fullmatch(r"polars_pilot_[a-z0-9_]{1,48}", namespace):
        raise ValueError("Pilot requires a dedicated polars_pilot_* namespace")


def category_pages(connection, start=None, end=None, *, page_size=10000):
    if (start is None) != (end is None):
        raise ValueError("Both interval bounds are required")
    query = source_query(CATEGORY, start, end)
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise ValueError("page_size must be between 1 and 10000")
    last = None
    while True:
        with connection.cursor() as cursor:
            cursor.execute(page_query(query, CATEGORY.pk, last, page_size))
            columns = [field.name for field in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        for row in rows:
            row["public_id"] = str(row["public_id"])
            for name in ("created_at", "updated_at"):
                value = row[name]
                row[name] = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        yield pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA)
        if len(rows) < page_size:
            break
        last = rows[-1][CATEGORY.pk]


def bronze_page(source, observed_at):
    frame = pl.from_arrow(source)
    return (
        frame.with_columns(
            pl.lit("postgresql_ecommerce").alias("_source_system"),
            pl.lit("public").alias("_source_schema"),
            pl.lit("categories").alias("_source_table"),
            pl.col("category_id").cast(pl.String).alias("_source_primary_key"),
            pl.col("updated_at").alias("_source_cursor_at"),
            pl.lit(observed_at).alias("_ingested_at_utc"),
        )
        .to_arrow()
        .cast(BRONZE_SCHEMA)
    )


def silver_candidates(source, current, run_id, observed_at):
    frame = (
        pl.from_arrow(source)
        .sort("updated_at", nulls_last=False)
        .unique(
            subset=["category_id"],
            keep="last",
            maintain_order=True,
        )
    )
    if frame.select(pl.any_horizontal(pl.col("category_id", "code", "name").is_null()).any()).item():
        # This pilot fails closed; full quarantine routing belongs to the production port.
        raise ValueError("Invalid categories: required key/code/name is missing")
    if not current.is_empty():
        frame = (
            frame.join(
                current.select("category_id", pl.col("updated_at").alias("_current_cursor")),
                on="category_id",
                how="left",
            )
            .filter(pl.col("_current_cursor").is_null() | (pl.col("updated_at") >= pl.col("_current_cursor")))
            .drop("_current_cursor")
        )
    frame = frame.with_columns(
        pl.col("public_id").map_elements(lambda value: uuid.UUID(value).bytes, return_dtype=pl.Binary),
        pl.lit(None, dtype=pl.String).alias("slug"),
        pl.lit(observed_at).alias("_silver_ingested_at"),
        pl.lit(run_id).alias("_source_bronze_run_id"),
    )
    return frame.select(SILVER_SCHEMA.names).to_arrow().cast(SILVER_SCHEMA)


def run_pilot(catalog, connection, namespace, *, bucket, run_id, start=None, end=None, page_size=10000):
    validate_namespace(namespace)
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", run_id):
        raise ValueError("Pilot run_id must be a path-safe identifier")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid pilot bucket")
    # Namespace isolation excludes Spark writers; session lock serializes all pilot writers.
    lock = int.from_bytes(hashlib.sha256(namespace.encode()).digest()[:8], "big", signed=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock,))
        if not cursor.fetchone()[0]:
            raise RuntimeError("Another writer owns this pilot namespace")
    try:
        return _run(catalog, connection, namespace, bucket, run_id, start, end, page_size)
    finally:
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (lock,))


def _run(catalog, connection, namespace, bucket, run_id, start, end, page_size):
    # Validate interval before creating catalog objects.
    if (start is None) != (end is None):
        raise ValueError("Both interval bounds are required")
    source_query(CATEGORY, start, end)
    catalog.create_namespace_if_not_exists(namespace)
    bronze = catalog.create_table_if_not_exists(
        (namespace, "categories"),
        schema=BRONZE_SCHEMA,
        properties={"format-version": "2"},
    )
    silver = catalog.create_table_if_not_exists(
        (namespace, "silver_categories"),
        schema=SILVER_SCHEMA,
        properties={"format-version": "2"},
    )
    if any(row["_source_system"] != "postgresql_ecommerce" for row in bronze.scan().to_arrow().to_pylist()):
        raise ValueError("Pilot Bronze contains legacy or unclassified source history")
    observed = datetime.now(UTC)
    pages = []
    count = 0
    for index, page in enumerate(category_pages(connection, start, end, page_size=page_size)):
        path = f"s3://{bucket}/landing/polars-pilot/{namespace}/{run_id}/categories/page-{index:06}.parquet"
        with bronze.io.new_output(path).create(overwrite=True) as stream:
            pq.write_table(bronze_page(page, observed), stream)
        pages.append(path)
        count += page.num_rows
    # Extraction must finish before any ingestion commit; only listed pages are consumed.
    for path in pages:
        with bronze.io.new_input(path).open() as stream:
            page = pq.read_table(stream)
        if not page.num_rows:
            continue
        bronze.upsert(page, join_cols=["category_id", "updated_at"])
        keys = page.column("category_id").to_pylist()
        current = pl.from_arrow(silver.scan(row_filter=In("category_id", keys)).to_arrow())
        delta = silver_candidates(page, current, run_id, observed)
        if delta.num_rows:
            silver.upsert(delta, join_cols=["category_id"])
    return {
        "namespace": namespace,
        "run_id": run_id,
        "source_rows": count,
        "staging_pages": len(pages),
        "bronze_rows": len(bronze.scan().to_arrow()),
        "silver_rows": len(silver.scan().to_arrow()),
        "bronze_snapshot": bronze.current_snapshot().snapshot_id if bronze.current_snapshot() else None,
        "silver_snapshot": silver.current_snapshot().snapshot_id if silver.current_snapshot() else None,
    }
