"""Request-event aggregates with explicit null, UTC and quantile semantics."""

from datetime import UTC, datetime

import polars as pl

from lakehouse.oltp.polars.schema import align_arrow, ddl_schema
from .identity import resolve

TABLES = ("mart_hourly_route_metrics", "mart_daily_product_demand", "mart_marketing_funnel_daily")


def schema(name):
    if name not in TABLES:
        raise ValueError("Unknown native log mart")
    return ddl_schema(f"contracts/logs/{name}.sql")


def count_when(condition, name):
    return condition.fill_null(False).cast(pl.Int64).sum().alias(name)


def rate(numerator, denominator):
    return pl.when(pl.col(denominator) > 0).then(pl.col(numerator) * 100.0 / pl.col(denominator))


def routes(events):
    duration = pl.col("duration_ms")
    frame = (
        events.with_columns(pl.col("event_ts").dt.hour().alias("metric_hour"))
        .group_by("event_date", "metric_hour", "http_route", "http_request_method")
        .agg(
            pl.len().alias("total_requests"),
            count_when(pl.col("is_success"), "success_2xx_count"),
            count_when(pl.col("is_client_error"), "client_error_4xx_count"),
            count_when(pl.col("is_server_error"), "server_error_5xx_count"),
            duration.mean().alias("avg_duration_ms"),
            *[
                duration.quantile(q, interpolation="equiprobable").alias(f"p{label}_duration_ms")
                for q, label in ((0.5, 50), (0.95, 95), (0.99, 99))
            ],
        )
        .rename({"event_date": "metric_date"})
    )
    return frame.with_columns(
        ((pl.col("client_error_4xx_count") + pl.col("server_error_5xx_count")) * 100.0 / pl.col("total_requests"))
        .round(2, mode="half_away_from_zero")
        .alias("error_rate_pct"),
        *[
            pl.col(name).round(3, mode="half_away_from_zero")
            for name in ("avg_duration_ms", "p50_duration_ms", "p95_duration_ms", "p99_duration_ms")
        ],
    )


def demand(resolved):
    action = pl.col("ecommerce_action")
    frame = (
        resolved.filter(
            pl.col("is_success")
            & action.is_in(
                [
                    "product_detail",
                    "cart_item_set",
                    "cart_add",
                    "cart_item_remove",
                    "cart_remove",
                    "wishlist_add",
                    "checkout_quote",
                ]
            )
            & pl.col("product_key").is_not_null()
            & (pl.col("product_key") != "")
        )
        .group_by("event_date", "product_key", "product_id", "product_name")
        .agg(
            count_when(action == "product_detail", "detail_view_count"),
            count_when(action.is_in(["cart_add", "cart_item_set"]), "cart_add_count"),
            count_when(action.is_in(["cart_remove", "cart_item_remove"]), "cart_remove_count"),
            count_when(action == "wishlist_add", "wishlist_add_count"),
            count_when(action == "checkout_quote", "checkout_quote_count"),
            pl.col("actor_key").drop_nulls().n_unique().alias("unique_visitors_count"),
        )
        .rename({"event_date": "metric_date"})
    )
    return frame.with_columns(
        rate("cart_add_count", "detail_view_count").round(2, mode="half_away_from_zero").alias("cart_to_view_rate_pct")
    )


def funnel(events):
    action, success = pl.col("ecommerce_action"), pl.col("is_success")
    frame = (
        events.group_by("event_date")
        .agg(
            pl.len().alias("observed_event_count"),
            count_when(success & action.is_in(["product_detail", "catalog_search"]), "product_views"),
            count_when(success & action.is_in(["cart_item_set", "cart_add"]), "cart_additions"),
            count_when(success & action.is_in(["checkout_quote", "checkout_submit"]), "checkout_initiations"),
            count_when(success & (action == "checkout_submit"), "orders_completed"),
            pl.col("actor_key").drop_nulls().n_unique().alias("unique_visitors"),
        )
        .rename({"event_date": "metric_date"})
    )
    return frame.with_columns(
        rate("cart_additions", "product_views").alias("view_to_cart_rate_pct"),
        rate("checkout_initiations", "cart_additions").alias("cart_to_checkout_rate_pct"),
        rate("orders_completed", "checkout_initiations").alias("checkout_to_order_rate_pct"),
        rate("orders_completed", "product_views").alias("overall_conversion_rate_pct"),
    )


def build_all(events, products, variants, run_id, *, ingested_at=None):
    resolved = resolve(events, products, variants)
    frames = dict(zip(TABLES, [routes(events), demand(resolved), funnel(events)]))
    for name, frame in frames.items():
        frame = frame.with_columns(
            pl.lit(ingested_at or datetime.now(UTC)).alias("_gold_ingested_at"), pl.lit(run_id).alias("_source_run_id")
        )
        frames[name] = pl.from_arrow(align_arrow(frame.to_arrow(), schema(name)))
    return frames
