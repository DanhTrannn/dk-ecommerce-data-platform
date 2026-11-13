"""Orders: when they are placed, what is in the basket, what they cost, and how they end."""

import numpy as np
import polars as pl

from datagen.distributions import repeat_gap_mean_days, sample_frequency_table
from datagen.oltp.plan import (
    MICROS_PER_HOUR,
    MICROS_PER_MINUTE,
    MICROS_PER_SECOND,
    Timeline,
    sale_event_dates,
    utc_series,
)
from datagen.oltp.reference import Reference
from datagen.presets import OltpPreset

MAX_REDRAW_ROUNDS = 10


def _group_starts(sizes: np.ndarray) -> np.ndarray:
    """Index of the first element of each group; empty when there are no groups."""
    return np.cumsum(sizes) - sizes


def _sale_events_by_day(preset: OltpPreset, timeline: Timeline) -> pl.Series:
    """Sale event name of each timeline day, null on ordinary days."""
    events = {np.datetime64(day): name for day, name in sale_event_dates(preset).items()}
    return pl.Series([events.get(day) for day in timeline.local_date], dtype=pl.String)


def _times_on_days(preset: OltpPreset, timeline: Timeline, rng: np.random.Generator, day: np.ndarray) -> np.ndarray:
    """Epoch micros on the given day indexes; the hour follows the normal or the sale-day profile."""
    hours = preset.calendar.hour_of_day
    sale = timeline.is_sale_day[day]
    hour = rng.choice(24, size=len(day), p=np.asarray(hours.normal) / sum(hours.normal))
    hour[sale] = rng.choice(24, size=int(sale.sum()), p=np.asarray(hours.sale_day) / sum(hours.sale_day))
    minute_low, minute_high = preset.policies.timing.minute_of_hour
    minute = rng.integers(minute_low, minute_high + 1, len(day))
    second = rng.integers(0, 60, len(day))
    return (
        timeline.day_start.astype(np.int64)[day]
        + hour * MICROS_PER_HOUR
        + minute * MICROS_PER_MINUTE
        + second * MICROS_PER_SECOND
    )


def draw_order_times(
    preset: OltpPreset, timeline: Timeline, rng: np.random.Generator, customers: pl.DataFrame
) -> pl.DataFrame:
    """One row per online order: R02 gaps laid on the R05 day-intensity axis.

    Positions live on a weighted-time axis of length T days; a position maps to the calendar day whose
    cumulative order intensity first exceeds position / T, so heavy days receive proportionally more orders.
    """
    buyers = customers.filter(pl.col("order_count") > 0).sort("customer_id")
    k = buyers["order_count"].to_numpy()
    days = len(timeline.day_weight)
    starts = _group_starts(k)

    mean_gap = np.repeat(repeat_gap_mean_days(days, k, preset.customers.repeat_gap.shrink_c), k)
    gap = np.maximum(rng.exponential(mean_gap), 1.0)
    gap[starts] = 0.0  # the first order of a customer has no preceding gap
    span = np.add.reduceat(gap, starts)
    scale = np.where(span > days - 1, (days - 1) / np.maximum(span, 1e-9), 1.0)
    offset = np.cumsum(gap) - np.repeat(np.cumsum(gap)[starts], k)
    first = rng.uniform(0, days - np.minimum(span, days - 1))
    position = np.repeat(first, k) + offset * np.repeat(scale, k)

    day = np.searchsorted(np.cumsum(timeline.day_weight), position / days, side="right")
    day = np.clip(day, 0, days - 1)
    created = _times_on_days(preset, timeline, rng, day)

    return (
        pl.DataFrame(
            {
                "customer_id": np.repeat(buyers["customer_id"].to_numpy(), k),
                "display_name": np.repeat(buyers["display_name"].to_numpy(), k),
                "city_id": np.repeat(buyers["city_id"].to_numpy(), k),
                "created_at": utc_series("created_at", created),
                "sale_event": _sale_events_by_day(preset, timeline).gather(day),
            }
        )
        .sort("customer_id", "created_at", maintain_order=True)
        .with_row_index("order_index")
        .with_columns(pl.col("order_index").cast(pl.Int64))
    )


def _distinct_products(preset: OltpPreset, reference: Reference, rng: np.random.Generator, order_count: int):
    """(order_index, product_id) with R04 basket sizes and R03 popularity, no product twice in an order."""
    basket = rng.geometric(preset.baskets.products.p, order_count)
    order_index = np.repeat(np.arange(order_count), basket)
    product_ids = reference.products["product_id"].to_numpy()
    weights = reference.products["weight"].to_numpy()
    product = rng.choice(product_ids, size=len(order_index), p=weights)
    keep = np.ones(len(order_index), dtype=bool)
    for _ in range(MAX_REDRAW_ROUNDS):
        frame = pl.DataFrame({"o": order_index, "p": product})
        repeated = ~frame.select(pl.struct("o", "p").is_first_distinct()).to_series().to_numpy()
        if not repeated.any():
            break
        product[repeated] = rng.choice(product_ids, size=int(repeated.sum()), p=weights)
    else:
        frame = pl.DataFrame({"o": order_index, "p": product})
        keep = frame.select(pl.struct("o", "p").is_first_distinct()).to_series().to_numpy()
    return pl.DataFrame({"order_index": order_index[keep], "product_id": product[keep]})


def draw_order_items(
    preset: OltpPreset, reference: Reference, rng: np.random.Generator, order_count: int
) -> pl.DataFrame:
    """Basket lines (order_index, variant_id, quantity) for order_count orders."""
    slots = _distinct_products(preset, reference, rng, order_count).with_row_index("slot")
    slots = slots.with_columns(
        wanted=pl.Series(sample_frequency_table(rng, preset.baskets.variants_per_product, slots.height))
    )
    options = slots.join(reference.variants.select("product_id", "variant_id"), on="product_id", maintain_order="left")
    options = options.with_columns(key=pl.Series(rng.random(options.height))).sort("slot", "key")
    chosen = options.filter(pl.int_range(pl.len()).over("slot") < pl.col("wanted")).sort("slot", "variant_id")
    quantity = sample_frequency_table(rng, preset.baskets.quantity_per_variant, chosen.height)
    return chosen.select(
        pl.col("order_index").cast(pl.Int64), "variant_id", pl.Series("quantity", quantity, dtype=pl.Int64)
    )


def price_orders(
    preset: OltpPreset,
    reference: Reference,
    rng: np.random.Generator,
    orders: pl.DataFrame,
    items: pl.DataFrame,
    channel: str,
) -> pl.DataFrame:
    """Add subtotal, coupon, discount, shipping fee and total; POS orders get no coupon and no shipping."""
    subtotal = (
        items.join(reference.variants.select("variant_id", "price_vnd"), on="variant_id", maintain_order="left")
        .group_by("order_index")
        .agg((pl.col("price_vnd") * pl.col("quantity")).sum().alias("subtotal_vnd"))
    )
    orders = orders.join(subtotal, on="order_index", how="left", maintain_order="left").sort("order_index")
    coupons = reference.coupons
    if channel == "pos":
        coupon_id = pl.repeat(None, orders.height, dtype=pl.Int64, eager=True)
    else:
        coupon_id = _draw_coupons(preset, rng, orders, coupons)
    orders = orders.with_columns(coupon_id=coupon_id).join(
        coupons.select(
            "coupon_id",
            pl.col("code").alias("coupon_code"),
            pl.col("percentage").alias("coupon_percentage"),
            "per_customer_usage_limit",
        ),
        on="coupon_id",
        how="left",
        maintain_order="left",
    )
    # Usage beyond the per-customer limit falls back to no coupon, counted in order time.
    use_rank = pl.int_range(1, pl.len() + 1).over("customer_id", "coupon_id", order_by=["created_at", "order_index"])
    orders = orders.with_columns(
        pl.when(use_rank > pl.col("per_customer_usage_limit")).then(None).otherwise(pl.col(name)).alias(name)
        for name in ("coupon_id", "coupon_code", "coupon_percentage")
    ).drop("per_customer_usage_limit")
    shipping = preset.policies.shipping
    fee = 0 if channel == "pos" else shipping.fee_vnd
    return orders.with_columns(
        discount_amount_vnd=(pl.col("subtotal_vnd") * pl.col("coupon_percentage").fill_null(0) // 100),
        shipping_fee_vnd=pl.when(pl.col("subtotal_vnd") >= shipping.free_from_subtotal_vnd).then(0).otherwise(fee),
    ).with_columns(total_vnd=pl.col("subtotal_vnd") - pl.col("discount_amount_vnd") + pl.col("shipping_fee_vnd"))


def _draw_coupons(preset, rng, orders, coupons) -> pl.Series:
    """Sale days offer only that event's coupon; other days offer the ordinary coupon."""
    rules = preset.coupons
    ordinary = coupons.filter(pl.col("sale_event").is_null())["coupon_id"][0]
    offered = orders.select("sale_event").join(
        coupons.select("sale_event", "coupon_id"), on="sale_event", how="left", maintain_order="left"
    )
    on_sale = offered["sale_event"].is_not_null().to_numpy()
    coupon = offered["coupon_id"].fill_null(ordinary)
    eligible = (orders["subtotal_vnd"] >= rules.minimum_subtotal_vnd).to_numpy()
    rate = np.where(on_sale, rules.sale_day_usage_rate, rules.usage_rate)
    uses = eligible & (rng.random(orders.height) < rate)
    return pl.select(pl.when(pl.Series(uses)).then(coupon)).to_series().cast(pl.Int64)
