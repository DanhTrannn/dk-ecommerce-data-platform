"""One-time current-state repair after adding the source payment change timestamp."""

import os
from urllib.parse import unquote, urlsplit

from pyspark.sql import functions as F


def refresh_payments(spark):
    url = urlsplit(os.environ["MYSQL_ECOMMERCE_READER_URL"])
    source = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:mysql://{url.hostname}:{url.port or 3306}{url.path}?serverTimezone=UTC")
        .option("dbtable", "payments")
        .option("user", unquote(url.username or ""))
        .option("password", unquote(url.password or ""))
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )
    if "updated_at" not in source.columns:
        raise RuntimeError("Upgrade MySQL payments.updated_at before refreshing payments")
    target = "lakehouse.silver.silver_payments"
    source = source.withColumn("_silver_ingested_at", F.current_timestamp()).withColumn(
        "_source_bronze_run_id", F.lit("payment-timestamp-repair")
    )
    fields = spark.table(target).schema.fields
    source.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in fields]).writeTo(
        target
    ).overwrite(F.lit(True))
