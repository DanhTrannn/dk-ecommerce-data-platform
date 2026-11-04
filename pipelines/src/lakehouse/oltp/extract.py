"""Extract a fixed Airflow interval into reusable Parquet staging."""

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark import StorageLevel

CHUNK_ROWS = 10_000


def _write_staging(spark, cfg, table, query):
    last_pk = None
    first_chunk = True
    while True:
        after = f" WHERE {table.pk} > {last_pk}" if last_pk is not None else ""
        page_query = f"SELECT * FROM ({query}) source_rows{after} ORDER BY {table.pk} LIMIT {CHUNK_ROWS}"
        df = (
            spark.read.format("jdbc")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", cfg.jdbc_url)
            .option("query", page_query)
            .option("fetchsize", CHUNK_ROWS)
            .load()
        )
        missing = {table.pk, table.cursor_field} - set(df.columns)
        if missing:
            raise ValueError(
                f"Source table {table.name} is missing configured columns {sorted(missing)}; "
                "apply the source schema migration before extraction"
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
                df.withColumn("_source_system", F.lit("mysql_ecommerce"))
                .withColumn("_source_schema", F.lit("ecommerce"))
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
    lower = datetime.fromisoformat(interval_start.replace("Z", "+00:00"))
    upper = datetime.fromisoformat(interval_end.replace("Z", "+00:00"))
    if lower.tzinfo is None or upper.tzinfo is None:
        raise ValueError("Batch interval must include a timezone")
    lower = lower.astimezone(timezone.utc).replace(tzinfo=None)
    upper = upper.astimezone(timezone.utc).replace(tzinfo=None)
    if lower >= upper:
        raise ValueError("interval_start must precede interval_end")
    for table in cfg.tables:
        query = (
            f"SELECT * FROM {table.name} "
            f"WHERE {table.cursor_field} >= '{lower.isoformat(sep=' ')}' "
            f"AND {table.cursor_field} < '{upper.isoformat(sep=' ')}'"
        )
        _write_staging(spark, cfg, table, query)


def run_initial_extract(spark, cfg):
    """Read every source row, with no timestamp or interval predicate."""
    for table in cfg.tables:
        _write_staging(spark, cfg, table, f"SELECT * FROM {table.name}")
