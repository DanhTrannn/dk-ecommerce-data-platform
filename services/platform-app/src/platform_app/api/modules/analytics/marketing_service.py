"""Serve independent, published log and commerce mart sections."""

from datetime import UTC, datetime, timedelta

from platform_app.api.core.errors import INTERNAL_ERROR, AppError
from platform_app.api.modules.analytics.reader import serve
from platform_app.api.modules.analytics.schemas import (
    CouponUsageMetric,
    FunnelStep,
    MarketingMetricsResponse,
    MarketingSourceStatus,
    NegativeReviewMetric,
    ProductDemandMetric,
)


def _timestamp(value):
    parsed = datetime.fromisoformat(str(value).replace(" UTC", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _ratio(numerator, denominator):
    return 100.0 * numerator / denominator if numerator is not None and denominator and denominator > 0 else None


def _logs(reader):
    observed = _timestamp(reader.publication["observed_at"])
    start, end = (observed.date() - timedelta(days=29)).isoformat(), observed.date().isoformat()
    row = reader.one(f"""SELECT SUM(observed_event_count) observed,SUM(product_views) views,
        SUM(cart_additions) carts,SUM(checkout_initiations) checkouts,SUM(orders_completed) purchases
        FROM lakehouse.gold.mart_marketing_funnel_daily
        WHERE metric_date BETWEEN DATE '{start}' AND DATE '{end}'""")
    products = reader.query(f"""SELECT product_key,product_id,product_name,SUM(detail_view_count) detail_views,
        SUM(cart_add_count) cart_sets,SUM(wishlist_add_count) wishlist_adds,
        100.0*SUM(cart_add_count)/NULLIF(SUM(detail_view_count),0) cart_to_view_rate_percent
        FROM lakehouse.gold.mart_daily_product_demand
        WHERE metric_date BETWEEN DATE '{start}' AND DATE '{end}' GROUP BY 1,2,3
        ORDER BY detail_views DESC,cart_sets DESC,product_key LIMIT 10""")
    has_observations = row.get("observed") is not None and row["observed"] > 0
    counts = [row.get(k) for k in ["views", "carts", "checkouts", "purchases"]]
    labels = ["Xem / tìm sản phẩm", "Đặt lượng giỏ hàng", "Tương tác checkout", "Gửi checkout thành công"]
    return MarketingMetricsResponse(
        view_events_count=counts[0] if has_observations else None,
        checkout_success_events_count=counts[3] if has_observations else None,
        checkout_to_view_rate_percent=_ratio(counts[3], counts[0]) if has_observations else None,
        funnel_window_start=start,
        funnel_window_end=end,
        funnel_steps=[
            FunnelStep(step_name=label, count=count, conversion_rate_percent=_ratio(count, counts[0]))
            for label, count in zip(labels, counts, strict=True)
        ]
        if has_observations
        else [],
        product_demand=[ProductDemandMetric(**p) for p in products],
        log_status=MarketingSourceStatus(observed_at=observed.isoformat()),
    )


def _commerce(reader):
    summary = reader.one("SELECT * FROM lakehouse.gold.mart_marketing_summary WHERE summary_key='current'")
    if not summary:
        raise AppError(INTERNAL_ERROR, "Mart Marketing chưa có quan sát hợp lệ.", status_code=503)
    coupons = reader.query("""SELECT coupon_id,coupon_code,used_count,usage_limit,recent_redemptions_count,
        100.0*used_count/NULLIF(usage_limit,0) usage_percent,
        recent_redemptions_count/15.0 recent_redemptions_per_minute
        FROM lakehouse.gold.mart_coupon_usage WHERE is_available
        ORDER BY used_count DESC,coupon_id LIMIT 10""")
    reviews = reader.query("""SELECT product_id,product_name,recent_negative_count negative_count
        FROM lakehouse.gold.mart_product_review_summary WHERE recent_negative_count>0
        ORDER BY negative_count DESC,product_id LIMIT 5""")
    return MarketingMetricsResponse(
        coupon_source_rows=summary["coupon_rows"],
        review_source_rows=summary["review_rows"],
        coupons=[CouponUsageMetric(**c) for c in coupons],
        negative_reviews=[NegativeReviewMetric(**r) for r in reviews],
        commerce_status=MarketingSourceStatus(observed_at=_timestamp(summary["marketing_observed_at"]).isoformat()),
    )


def get_marketing_metrics(db=None, trino_client=None):
    parts, statuses = {}, {}
    for source, build, status_key in [("logs", _logs, "log_status"), ("oltp", _commerce, "commerce_status")]:
        try:
            part = serve(("marketing", source), build, trino_client, source=source)
            statuses[status_key] = MarketingSourceStatus(
                **{
                    k: getattr(part, k)
                    for k in [
                        "data_source",
                        "is_degraded",
                        "degraded_reason",
                        "source_run_id",
                        "data_updated_at",
                        "served_from_cache",
                        "cache_age_seconds",
                    ]
                },
                available=True,
                observed_at=getattr(part, status_key).observed_at,
            )
            parts[source] = part
        except AppError as error:
            if error.code != INTERNAL_ERROR:
                raise
            statuses[status_key] = MarketingSourceStatus(
                is_degraded=True, degraded_reason="Chưa đọc được mart đã công bố của nguồn này."
            )
    if not parts:
        raise AppError(INTERNAL_ERROR, "Chưa đọc được mart Marketing của cả hai nguồn.", status_code=503)
    values = {}
    for source, fields in [
        (
            "logs",
            [
                "view_events_count",
                "checkout_success_events_count",
                "checkout_to_view_rate_percent",
                "funnel_window_start",
                "funnel_window_end",
                "funnel_steps",
                "product_demand",
            ],
        ),
        ("oltp", ["coupon_source_rows", "review_source_rows", "coupons", "negative_reviews"]),
    ]:
        if source in parts:
            values.update({k: getattr(parts[source], k) for k in fields})
    degraded = any(s.is_degraded for s in statuses.values())
    # No single run ID/timestamp represents these two independent publications.
    return MarketingMetricsResponse(
        **values,
        **statuses,
        is_degraded=degraded,
        degraded_reason="Một phần nguồn chưa sẵn sàng hoặc đang dùng bản lưu; xem trạng thái từng nguồn."
        if degraded
        else None,
        served_from_cache=any(s.served_from_cache for s in statuses.values()),
        cache_age_seconds=max(s.cache_age_seconds for s in statuses.values()),
    )
