"""Input integrity, event-count conservation, grain and persisted-output checks."""

from collections import Counter

import polars as pl

from .marts import TABLES

REQUIRED_CHECKS = {
    "event_timestamp_present",
    "event_identity",
    "event_date_utc",
    "http_status_flags",
    "duration_domain",
    "route_event_conservation",
    "funnel_event_conservation",
    "checkout_request_conservation",
    "product_request_conservation",
    *["persisted_" + name for name in TABLES],
    *["grain_" + name for name in TABLES],
}


def result(name, failed):
    return {"check_name": name, "failed_rows": int(failed), "status": "FAILED" if failed else "PASSED"}


def input_checks(events):
    checks = [result("event_identity", events.height - events["event_id"].drop_nulls().n_unique())]
    bad_dates = events.filter(
        pl.col("event_date").is_null() | (pl.col("event_date") != pl.col("event_ts").dt.date())
    ).height
    checks.append(result("event_date_utc", bad_dates))
    status = pl.col("http_status_code")
    invalid = events.filter(
        status.is_null()
        | ~status.is_between(100, 599)
        | pl.any_horizontal([pl.col(c).is_null() for c in ("is_success", "is_client_error", "is_server_error")])
        | (pl.col("is_success") != (status < 400))
        | (pl.col("is_client_error") != status.is_between(400, 499))
        | (pl.col("is_server_error") != (status >= 500))
    ).height
    checks.append(result("http_status_flags", invalid))
    duration = pl.col("duration_ms")
    checks.append(
        result(
            "duration_domain", events.filter(duration.is_not_null() & (~duration.is_finite() | (duration < 0))).height
        )
    )
    return checks


def differences(left, right):
    columns = sorted(c for c in left.columns if not c.startswith("_"))
    a, b = Counter(left.select(columns).iter_rows()), Counter(right.select(columns).iter_rows())
    return sum((a - b).values()) + sum((b - a).values())


def output_checks(events, expected, actual):
    checks = []
    for name, keys in zip(
        TABLES,
        [
            ["metric_date", "metric_hour", "http_route", "http_request_method"],
            ["metric_date", "product_key", "product_id", "product_name"],
            ["metric_date"],
        ],
    ):
        checks.append(result("persisted_" + name, differences(expected[name], actual[name])))
        n = actual[name].group_by(keys).len().filter(pl.col("len") > 1).height
        checks.append(result("grain_" + name, n))
    routes, demand, funnel = [actual[name] for name in TABLES]
    for name, count in (
        ("route_event_conservation", routes["total_requests"].sum() or 0),
        ("funnel_event_conservation", funnel["observed_event_count"].sum() or 0),
    ):
        checks.append(result(name, abs(count - events.height)))
    checkouts = events.filter(pl.col("is_success") & (pl.col("ecommerce_action") == "checkout_submit")).height
    checks.append(result("checkout_request_conservation", abs((funnel["orders_completed"].sum() or 0) - checkouts)))
    # Unresolved keyed actions remain in demand; demand totals must retain eligible request events.
    eligible = events.filter(
        pl.col("is_success")
        & pl.col("ecommerce_action").is_in(
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
        & ((pl.col("product_key").str.len_chars() > 0) | (pl.col("variant_key").str.len_chars() > 0))
    ).height
    count = sum(
        demand[c].sum() or 0
        for c in (
            "detail_view_count",
            "cart_add_count",
            "cart_remove_count",
            "wishlist_add_count",
            "checkout_quote_count",
        )
    )
    checks.append(result("product_request_conservation", abs(count - eligible)))
    return checks
