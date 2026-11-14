"""Request rows to access-log JSON, contract ecommerce.access 1.1.0 (same shape as the API's build_access_event)."""

import polars as pl

SCHEMA_VERSION = "1.1.0"

# action -> (method, route), as in the API's _ACTIONS table (services/platform-app/.../core/access_logging.py)
ROUTES: dict[str, tuple[str, str]] = {
    "catalog_search": ("GET", "/api/v1/products"),
    "product_detail": ("GET", "/api/v1/products/{slug}"),
    "cart_item_set": ("PUT", "/api/v1/cart/items/{variant_public_id}"),
    "cart_read": ("GET", "/api/v1/cart"),
    "checkout_quote": ("POST", "/api/v1/checkout/quote"),
    "checkout_submit": ("POST", "/api/v1/checkout"),
}

# The ecommerce object carries only the keys that apply to the action, like the API's ecommerce_context.
ECOMMERCE_KEYS = {
    "product_detail": ("product_key",),
    "cart_item_set": ("variant_key",),
    "checkout_submit": ("order_number",),
}

NULL = pl.lit(None, dtype=pl.String)


def _payload(ecommerce_keys: tuple[str, ...]) -> pl.Expr:
    timestamp = pl.col("ts").dt.strftime("%Y-%m-%dT%H:%M:%S%.6fZ")
    method = pl.col("action").replace_strict({a: m for a, (m, _) in ROUTES.items()})
    route = pl.col("action").replace_strict({a: r for a, (_, r) in ROUTES.items()})
    body = pl.struct(
        pl.struct(name=pl.lit("ecommerce.access"), version=pl.lit(SCHEMA_VERSION)).alias("schema"),
        timestamp.alias("timestamp"),
        timestamp.alias("observed_timestamp"),
        pl.lit("INFO").alias("severity_text"),
        pl.lit(9).alias("severity_number"),
        pl.struct(id=pl.col("request_id")).alias("request"),
        pl.struct(id=pl.col("session_id")).alias("session"),
        NULL.alias("trace_id"),
        NULL.alias("span_id"),
        pl.struct(
            name=pl.lit("ecommerce-api"),
            version=pl.lit("0.1.0"),
            environment=pl.lit("local"),
            instance_id=pl.lit("datagen"),
        ).alias("service"),
        pl.struct(
            name=pl.lit("http.server.request"),
            category=pl.lit("web"),
            kind=pl.lit("event"),
            outcome=pl.lit("success"),
            duration_ns=pl.lit(0),  # latency is not simulated
        ).alias("event"),
        pl.struct(request_method=method, route=route, status_code=pl.lit(200)).alias("http"),
        pl.struct(type=pl.col("actor_type"), key=pl.col("actor_key")).alias("actor"),
        pl.struct(pl.col("action"), *[pl.col(key) for key in ecommerce_keys]).alias("ecommerce"),
        pl.struct(code=NULL, type=NULL).alias("error"),
        pl.lit("synthetic").alias("data_origin"),
    ).struct.json_encode()
    # A struct cannot be empty, so the empty client object is spliced in front of the encoded fields.
    return pl.concat_str(pl.lit('{"client":{},'), body.str.slice(1))


def to_payloads(requests: pl.DataFrame) -> pl.DataFrame:
    """One (raw_payload, event_at) row per request, in the order of `requests`."""
    indexed = requests.with_row_index("row")
    parts = [
        group.select(
            "row", _payload(ECOMMERCE_KEYS.get(action, ())).alias("raw_payload"), pl.col("ts").alias("event_at")
        )
        for (action,), group in indexed.group_by("action", maintain_order=True)
    ]
    return (
        pl.concat(parts).sort("row").drop("row")
        if parts
        else pl.DataFrame(schema={"raw_payload": pl.String, "event_at": pl.Datetime("us", "UTC")})
    )
