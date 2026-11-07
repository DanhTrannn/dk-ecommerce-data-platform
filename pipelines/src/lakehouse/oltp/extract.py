"""Extract a fixed Airflow interval into reusable Parquet staging."""

from pyspark.sql import functions as F
from pyspark import StorageLevel

from lakehouse.utils.postgres import page_query, source_query

CHUNK_ROWS = 10_000


def _write_staging(spark, cfg, table, query):
    last_pk = None
    first_chunk = True
    while True:
        query_page = page_query(query, table.pk, last_pk, CHUNK_ROWS)
        df = (
            spark.read.format("jdbc")
            .options(**cfg.jdbc_options)
            .option("query", query_page)
            .option("fetchsize", CHUNK_ROWS)
            .load()
        )
        missing = {table.pk, table.cursor_field} - set(df.columns)
        if missing:
            raise ValueError(
                f"Source table {table.name} is missing configured columns {sorted(missing)}; "
                "apply the PostgreSQL source bootstrap schema before extraction"
            )
        # Materialize only this page. Counting and writing must not re-query a
        # changing source; never collect source rows into the Python driver.
        df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            stats = df.agg(F.count("*").alias("rows"), F.max(table.pk).alias("last_pk")).first()
            count = stats["rows"]
            if not count and not first_chunk:
                break
            page = (
                df.withColumn("_source_system", F.lit("postgresql_ecommerce"))
                .withColumn("_source_schema", F.lit("public"))
                .withColumn("_source_table", F.lit(table.name))
                .withColumn("_source_primary_key", F.col(table.pk).cast("string"))
                .withColumn("_source_cursor_at", F.col(table.cursor_field))
                .withColumn("_ingested_at_utc", F.current_timestamp())
            )
            # One Parquet per page. The first page also clears stale staging,
            # including when the new run is empty.
            page.coalesce(1).write.mode("overwrite" if first_chunk else "append").parquet(
                f"s3a://{cfg.bucket}/{cfg.landing.oltp_prefix}/staging/{table.name}"
            )
            print(f"STAGING table={table.name} rows={count}", flush=True)
            if count < CHUNK_ROWS:
                break
            last_pk = int(stats["last_pk"])
            first_chunk = False
        finally:
            df.unpersist()


def run_extract(spark, cfg, interval_start, interval_end):
    if not interval_start or not interval_end:
        raise ValueError("Both batch interval bounds are required; use the explicit init job for a full load")
    # Build every interval query before starting source I/O.
    queries = [(table, source_query(table, interval_start, interval_end)) for table in cfg.tables]
    for table, query in queries:
        _write_staging(spark, cfg, table, query)


def run_initial_extract(spark, cfg):
    """Read every source row, with no timestamp or interval predicate."""
    for table in cfg.tables:
        _write_staging(spark, cfg, table, source_query(table))
