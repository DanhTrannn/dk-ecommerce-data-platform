"""Resolve log slugs/public UUIDs using a pinned OLTP catalog, without source I/O."""

from pyspark.sql import functions as F


def _uuid_token(name):
    column = F.col(name)
    # Retain support for published snapshots created by the former text-to-binary
    # cast while new Silver batches write canonical 16-byte UUIDs.
    return F.when(F.length(column) == 16, F.lower(F.hex(column))).otherwise(
        F.regexp_replace(F.lower(column.cast("string")), "-", "")
    )


def resolve_product_events(events, products, variants):
    product_ids = products.select("product_id", "name", "slug", _uuid_token("public_id").alias("uuid"))
    tokens = (
        product_ids.select("product_id", "name", F.col("slug").alias("token"))
        .unionByName(product_ids.select("product_id", "name", F.col("uuid").alias("token")))
        .unionByName(product_ids.select("product_id", "name", F.col("product_id").cast("string").alias("token")))
    )
    # Public IDs are UUID bytes. Remove hyphens only for UUID-shaped keys;
    # product slugs must keep their hyphens.
    key = F.lower(F.col("product_key"))
    token = F.when(key.rlike("^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$"), F.regexp_replace(key, "-", "")).otherwise(
        key
    )
    product_map = tokens.dropDuplicates(["product_id", "name", "token"]).select(
        F.col("token").alias("_product_token"),
        F.col("product_id").alias("_product_id"),
        F.col("name").alias("_product_name"),
    )
    variant_map = variants.join(products.select("product_id", "name"), "product_id", "left").select(
        _uuid_token("public_id").alias("_variant_token"),
        F.col("product_id").alias("_variant_product_id"),
        F.col("name").alias("_variant_product_name"),
    )
    joined = (
        events.withColumn("_product_token", token)
        .withColumn("_variant_token", F.regexp_replace(F.lower("variant_key"), "-", ""))
        .join(product_map, "_product_token", "left")
        .join(variant_map, "_variant_token", "left")
    )
    product_id = F.coalesce("_product_id", "_variant_product_id")
    # Keep unresolved keyed events visible. Never join unrelated products to
    # fill a missing identity or call variant-only events zero product demand.
    raw_key = F.when(F.length("product_key") > 0, F.concat(F.lit("raw-product:"), F.col("product_key"))).when(
        F.length("variant_key") > 0, F.concat(F.lit("raw-variant:"), F.col("variant_key"))
    )
    return joined.select(
        *[F.col(c) for c in events.columns if c != "product_key"],
        F.coalesce(product_id.cast("string"), raw_key).alias("product_key"),
        product_id.alias("product_id"),
        F.coalesce("_product_name", "_variant_product_name").alias("product_name"),
    )
