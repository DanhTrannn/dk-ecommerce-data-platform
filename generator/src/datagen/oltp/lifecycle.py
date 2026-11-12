"""How each online order ends: payment, cancellation, delivery, return and review, cut at the anchor."""

import numpy as np
import polars as pl

from datagen.distributions import sample_frequency_table
from datagen.oltp.plan import MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, utc_micros, utc_series
from datagen.oltp.reference import Reference
from datagen.presets import OltpPreset

STATUS_BY_LATEST_EVENT = (
    # First present time decides the order status.
    ("returned_at", "returned"),
    ("completed_at", "completed"),
    ("delivered_at", "delivered"),
    ("failed_at", "failed_delivery"),
    ("cancelled_at", "cancelled"),
    ("dispatched_at", "shipping"),
    ("confirmed_at", "confirmed"),
    ("paid_at", "paid"),
)
LATE_BUCKETS = ("on_time", "late_1_3", "late_4_7", "late_8_plus")
EVENT_COLUMNS = (
    "paid_at",
    "confirmed_at",
    "cancelled_at",
    "dispatched_at",
    "delivered_at",
    "attempt_1_at",
    "attempt_2_at",
    "attempt_3_at",
    "failed_at",
    "completed_at",
    "return_requested_at",
    "return_resolved_at",
    "returned_at",
    "review_at",
    "review_moderated_at",
)


def _uniform(rng: np.random.Generator, bounds: tuple[int, int], size: int) -> np.ndarray:
    return rng.integers(bounds[0], bounds[1] + 1, size)


def _weighted(rng: np.random.Generator, labels, weights, size: int) -> np.ndarray:
    weights = np.asarray(weights, dtype=np.float64)
    return rng.choice(np.asarray(labels, dtype=object), size=size, p=weights / weights.sum())


def draw_lifecycle(
    preset: OltpPreset, reference: Reference, rng: np.random.Generator, orders: pl.DataFrame, items: pl.DataFrame
) -> pl.DataFrame:
    """Every decision and event time of each online order, cut at the anchor (see spec section 5)."""
    o = orders.sort("order_index")
    n = o.height
    outcomes, timing, shipping = preset.outcomes, preset.policies.timing, preset.policies.shipping
    created = o["created_at"].dt.epoch("us").to_numpy()

    cod = rng.random(n) < outcomes.cod_rate
    cancelled = rng.random(n) < outcomes.cancellation_rate
    failed = cod & ~cancelled & (rng.random(n) < outcomes.cod_failure_rate)
    # A customer whose earlier COD orders failed cod_block_after_failures times pays later orders by VietQR.
    prior_failures = (
        pl.DataFrame(
            {"customer_id": o["customer_id"], "t": created, "i": o["order_index"], "f": failed.astype(np.int64)}
        )
        .select((pl.col("f").cum_sum() - pl.col("f")).over("customer_id", order_by=["t", "i"]))
        .to_series()
        .to_numpy()
    )
    blocked = cod & (prior_failures >= shipping.cod_block_after_failures)
    cod &= ~blocked
    failed &= ~blocked
    vietqr = ~cod
    reason = _weighted(rng, list(outcomes.cancellation_reasons), list(outcomes.cancellation_reasons.values()), n)

    t = {}
    present = {}
    t["paid_at"] = created + _uniform(rng, timing.payment_delay_minutes, n) * MICROS_PER_MINUTE
    present["paid_at"] = vietqr.copy()
    t["confirmed_at"] = np.where(
        cod, created, t["paid_at"] + _uniform(rng, timing.confirmation_delay_hours, n) * MICROS_PER_HOUR
    )
    present["confirmed_at"] = cod | ~cancelled
    t["cancelled_at"] = (
        np.where(cod, created, t["paid_at"]) + _uniform(rng, timing.cancellation_delay_minutes, n) * MICROS_PER_MINUTE
    )
    present["cancelled_at"] = cancelled
    t["dispatched_at"] = t["confirmed_at"] + _uniform(rng, timing.dispatch_delay_hours, n) * MICROS_PER_HOUR
    present["dispatched_at"] = ~cancelled

    fulfilment = preset.fulfilment
    late = rng.random(n) < fulfilment.late_rate
    buckets = [fulfilment.late_buckets[name] for name in LATE_BUCKETS[1:]]
    bucket = np.where(
        late, rng.choice([1, 2, 3], size=n, p=np.array([b.share for b in buckets]) / sum(b.share for b in buckets)), 0
    )
    late_days = np.zeros(n, dtype=np.int64)
    for index, rule in enumerate(buckets, start=1):
        late_days[bucket == index] = _uniform(rng, rule.days, int((bucket == index).sum()))
    delivery_days = _uniform(rng, fulfilment.on_time_delivery_days, n) + late_days
    t["delivered_at"] = (
        created
        + delivery_days * MICROS_PER_DAY
        + rng.integers(8, 21, n) * MICROS_PER_HOUR
        + rng.integers(0, 60, n) * MICROS_PER_MINUTE
    )
    present["delivered_at"] = ~cancelled & ~failed
    for attempt in (1, 2, 3):
        t[f"attempt_{attempt}_at"] = (
            created
            + shipping.failure_after_days * MICROS_PER_DAY
            + (attempt - 1) * shipping.attempt_interval_hours * MICROS_PER_HOUR
        )
        present[f"attempt_{attempt}_at"] = failed & (attempt <= shipping.failed_attempts)
    t["failed_at"] = t[f"attempt_{shipping.failed_attempts}_at"]
    present["failed_at"] = failed.copy()
    t["completed_at"] = t["delivered_at"] + _uniform(rng, timing.completion_delay_hours, n) * MICROS_PER_HOUR
    present["completed_at"] = present["delivered_at"].copy()
    t["paid_at"] = np.where(cod, t["completed_at"], t["paid_at"])
    present["paid_at"] = vietqr | present["completed_at"]

    returns = preset.policies.returns
    wants_return = present["delivered_at"] & (rng.random(n) < outcomes.return_rate)
    exchange = rng.random(n) < outcomes.exchange_rate
    lines = items.group_by("order_index").agg(pl.len().alias("lines"), pl.col("quantity").sum().alias("units"))
    lines = o.select("order_index").join(lines, on="order_index", how="left", maintain_order="left")
    return_item_index = np.floor(rng.random(n) * lines["lines"].to_numpy()).astype(np.int64)
    t["return_requested_at"] = t["completed_at"] + returns.request_delay_days * MICROS_PER_DAY
    present["return_requested_at"] = wants_return.copy()
    t["return_resolved_at"] = t["return_requested_at"] + returns.processing_delay_days * MICROS_PER_DAY
    present["return_resolved_at"] = wants_return.copy()

    reviews, review_policy = preset.reviews, preset.policies.reviews
    wants_review = present["delivered_at"] & (rng.random(n) < reviews.write_rate)
    t["review_at"] = (
        t["delivered_at"]
        + _uniform(rng, reviews.delay_days, n) * MICROS_PER_DAY
        + _uniform(rng, review_policy.extra_delay_hours, n) * MICROS_PER_HOUR
    )
    present["review_at"] = wants_review.copy()
    t["review_moderated_at"] = t["review_at"] + _uniform(rng, review_policy.moderation_delay_hours, n) * MICROS_PER_HOUR
    present["review_moderated_at"] = wants_review.copy()
    rating = np.zeros(n, dtype=np.int64)
    for index, name in enumerate(LATE_BUCKETS):
        rows = bucket == index
        rating[rows] = sample_frequency_table(rng, getattr(reviews.rating_by_lateness, name), int(rows.sum()))
    review_status = _weighted(rng, list(reviews.status_weights), list(reviews.status_weights.values()), n)

    # Cut at the anchor: whatever has not happened by then does not exist yet.
    anchor = utc_micros(preset.anchor_time)
    for name in t:
        present[name] &= t[name] <= anchor
    return_done = present["return_resolved_at"]
    present["return_requested_at"] &= return_done
    t["returned_at"] = t["return_resolved_at"]
    present["returned_at"] = return_done & (lines["units"].to_numpy() == 1)
    moderated = present["review_at"] & present["review_moderated_at"]
    present["review_moderated_at"] = moderated
    review_status = np.where(moderated, review_status, "approved")

    status = np.full(n, "pending_payment", dtype=object)
    status[cod] = "confirmed"
    for column, label in reversed(STATUS_BY_LATEST_EVENT):
        status[present[column]] = label
    latest = created.copy()
    for name in EVENT_COLUMNS:
        latest = np.where(present[name], np.maximum(latest, t[name]), latest)

    houses = _uniform(rng, preset.policies.customers.house_number_range, n)
    street = rng.integers(1, 51, n)
    city_names = o.select("city_id").join(
        reference.cities.select("city_id", "name"), on="city_id", how="left", maintain_order="left"
    )["name"]
    staff = rng.choice(reference.delivery_staff_ids, size=n)

    events = {
        name: pl.select(pl.when(pl.Series(present[name])).then(utc_series(name, t[name]))).to_series().alias(name)
        for name in EVENT_COLUMNS
    }
    return o.with_columns(
        channel=pl.lit("online"),
        store_id=pl.lit(None, dtype=pl.Int64),
        staff_id=pl.lit(None, dtype=pl.Int64),
        receiver_name=pl.col("display_name"),
        receiver_phone=pl.format("09{}", (pl.col("customer_id") % 10**8).cast(pl.String).str.zfill(8)),
        shipping_address_text=pl.format("{} Đường số {}, {}", pl.Series(houses), pl.Series(street), city_names),
        shipping_city_id=pl.col("city_id"),
        payment_method=pl.Series(np.where(cod, "cod", "vietqr")),
        is_cancelled=pl.Series(cancelled),
        is_failed_delivery=pl.Series(failed),
        cancel_reason=pl.Series(reason.astype(str)),
        late_bucket=pl.Series(np.asarray(LATE_BUCKETS)[bucket]),
        delivery_staff_id=pl.when(pl.Series(present["dispatched_at"])).then(pl.Series(staff)),
        return_action=pl.when(pl.Series(return_done)).then(pl.Series(np.where(exchange, "exchange", "refund"))),
        return_item_index=pl.when(pl.Series(return_done)).then(pl.Series(return_item_index)),
        review_rating=pl.when(pl.Series(present["review_at"])).then(pl.Series(rating)),
        review_status=pl.when(pl.Series(present["review_at"])).then(pl.Series(review_status.astype(str))),
        status=pl.Series(status.astype(str)),
        updated_at=utc_series("updated_at", latest),
        **events,
    )


def customer_cod_stats(preset: OltpPreset, lifecycle: pl.DataFrame) -> pl.DataFrame:
    limit = preset.policies.shipping.cod_block_after_failures
    return (
        lifecycle.filter(pl.col("customer_id").is_not_null())
        .group_by("customer_id")
        .agg(pl.col("failed_at").is_not_null().sum().cast(pl.Int32).alias("boom_count"))
        .with_columns(is_cod_blocked=pl.col("boom_count") >= limit)
    )
