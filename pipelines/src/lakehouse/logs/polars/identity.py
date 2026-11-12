"""Resolve product tokens against a pinned catalog without source database I/O."""

import polars as pl

UUID = r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$"


class AmbiguousProductIdentity(ValueError):
    """A catalog join would multiply immutable request events."""


def uuid_token(value):
    if value is None:
        return None
    value = bytes(value)
    return value.hex() if len(value) == 16 else value.decode("utf-8", errors="replace").lower().replace("-", "")


def resolve(events, products, variants):
    products = products.select("product_id", "name", "slug", "public_id")
    tokens = pl.concat(
        [
            products.select("product_id", "name", pl.col("slug").alias("token")),
            products.select(
                "product_id",
                "name",
                pl.col("public_id").map_elements(uuid_token, return_dtype=pl.String).alias("token"),
            ),
            products.select("product_id", "name", pl.col("product_id").cast(pl.String).alias("token")),
        ]
    ).unique()
    product_map = tokens.rename({"product_id": "_id", "name": "_name", "token": "_product_token"})
    variant_map = (
        variants.select("product_id", "public_id")
        .join(products.select("product_id", "name"), on="product_id", how="left")
        .select(
            pl.col("public_id").map_elements(uuid_token, return_dtype=pl.String).alias("_variant_token"),
            pl.col("product_id").alias("_variant_id"),
            pl.col("name").alias("_variant_name"),
        )
    )
    key = pl.col("product_key").str.to_lowercase()
    joined = (
        events.with_columns(
            pl.when(key.str.contains(UUID)).then(key.str.replace_all("-", "")).otherwise(key).alias("_product_token"),
            pl.col("variant_key").str.to_lowercase().str.replace_all("-", "").alias("_variant_token"),
        )
        .join(product_map, on="_product_token", how="left")
        .join(variant_map, on="_variant_token", how="left")
    )
    if joined.height != events.height:
        raise AmbiguousProductIdentity("Catalog identity mapping multiplied log events")
    raw = (
        pl.when(pl.col("product_key").str.len_chars() > 0)
        .then(pl.concat_str(pl.lit("raw-product:"), pl.col("product_key")))
        .when(pl.col("variant_key").str.len_chars() > 0)
        .then(pl.concat_str(pl.lit("raw-variant:"), pl.col("variant_key")))
    )
    return joined.select(
        *[name for name in events.columns if name != "product_key"],
        pl.coalesce("_id", "_variant_id").alias("product_id"),
        pl.coalesce("_name", "_variant_name").alias("product_name"),
        pl.coalesce(pl.coalesce("_id", "_variant_id").cast(pl.String), raw).alias("product_key"),
    )
