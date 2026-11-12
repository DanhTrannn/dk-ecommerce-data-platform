"""Point-of-sale orders: completed and paid at the counter, shaped like online lifecycles."""

import numpy as np
import polars as pl

from datagen.distributions import split_by_shares
from datagen.oltp.lifecycle import EVENT_COLUMNS
from datagen.oltp.orders import draw_order_items, price_orders
from datagen.oltp.plan import MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND, Timeline, utc_series
from datagen.oltp.reference import Reference
from datagen.presets import OltpPreset

WALK_IN_NAME = "Khách tại quầy"
WALK_IN_PHONE = "0000000000"


def pos_order_counts(
    preset: OltpPreset, timeline: Timeline, reference: Reference, online_order_count: int
) -> pl.DataFrame:
    """POS orders per (store, month): the city share split evenly over its stores, times the month's intensity."""
    total = round(online_order_count * preset.pos.orders_per_online_order)
    stores = reference.stores.join(reference.cities.select("city_id", "share"), on="city_id", maintain_order="left")
    stores = stores.with_columns(weight=pl.col("share") / pl.len().over("city_id")).sort("store_id")
    months = (
        pl.DataFrame({"yearmonth": timeline.yearmonth, "weight": timeline.day_weight})
        .group_by("yearmonth")
        .agg(pl.col("weight").sum())
        .sort("yearmonth")
    )
    cells = stores.select("store_id", pl.col("weight").alias("store_weight")).join(months, how="cross")
    cells = cells.sort("store_id", "yearmonth")
    counts = split_by_shares(total, (cells["store_weight"] * cells["weight"]).to_list())
    return cells.select("store_id", "yearmonth", pl.Series("order_count", counts))


def draw_pos_lifecycle(
    preset: OltpPreset,
    timeline: Timeline,
    reference: Reference,
    rng: np.random.Generator,
    store_id: int,
    yearmonth: int,
    order_count: int,
    city_customers: np.ndarray,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """POS lifecycles of one store and month (same columns as online ones) and their basket lines."""
    pos = preset.pos
    days = np.flatnonzero(timeline.yearmonth == yearmonth)
    weights = timeline.day_weight[days] / timeline.day_weight[days].sum()
    day = rng.choice(days, size=order_count, p=weights)
    opening, closing = pos.opening_hours
    created = np.sort(  # order ids follow the time of sale
        timeline.day_start.astype(np.int64)[day]
        + rng.integers(opening, closing, order_count) * MICROS_PER_HOUR
        + rng.integers(0, 60, order_count) * MICROS_PER_MINUTE
        + rng.integers(0, 60, order_count) * MICROS_PER_SECOND
    )
    walk_in = rng.random(order_count) < pos.walk_in_rate
    if len(city_customers) == 0:
        walk_in[:] = True
        member = np.zeros(order_count, dtype=np.int64)
    else:
        member = rng.choice(city_customers, size=order_count)
    staff = reference.store_staff.filter(pl.col("store_id") == store_id)["customer_id"].to_numpy()
    methods = list(pos.payment_methods)
    method_weights = np.array([pos.payment_methods[m] for m in methods], dtype=np.float64)
    store = reference.stores.filter(pl.col("store_id") == store_id)

    orders = pl.DataFrame(
        {
            "order_index": np.arange(order_count, dtype=np.int64),
            "customer_id": pl.Series(member).set(pl.Series(walk_in), None),
            "created_at": utc_series("created_at", created),
            "sale_event": pl.repeat(None, order_count, dtype=pl.String, eager=True),
        }
    )
    items = draw_order_items(preset, reference, rng, order_count)
    orders = price_orders(preset, reference, rng, orders, items, "pos")

    null_time = pl.lit(None, dtype=pl.Datetime("us", "UTC"))
    return (
        orders.with_columns(
            display_name=pl.lit(None, dtype=pl.String),
            city_id=pl.lit(int(store["city_id"][0])),
            channel=pl.lit("pos"),
            store_id=pl.lit(store_id),
            staff_id=pl.Series(rng.choice(staff, size=order_count)),
            receiver_name=pl.lit(WALK_IN_NAME),
            receiver_phone=pl.lit(WALK_IN_PHONE),
            shipping_address_text=pl.lit(store["address"][0]),
            shipping_city_id=pl.lit(None, dtype=pl.Int64),
            payment_method=pl.Series(rng.choice(methods, size=order_count, p=method_weights / method_weights.sum())),
            is_cancelled=pl.lit(False),
            is_failed_delivery=pl.lit(False),
            cancel_reason=pl.lit(None, dtype=pl.String),
            late_bucket=pl.lit(None, dtype=pl.String),
            delivery_staff_id=pl.lit(None, dtype=pl.Int64),
            return_action=pl.lit(None, dtype=pl.String),
            return_item_index=pl.lit(None, dtype=pl.Int64),
            review_rating=pl.lit(None, dtype=pl.Int64),
            review_status=pl.lit(None, dtype=pl.String),
            status=pl.lit("completed"),
            updated_at=pl.col("created_at"),
            **{name: null_time for name in EVENT_COLUMNS},
        ).with_columns(
            paid_at=pl.col("created_at"), confirmed_at=pl.col("created_at"), completed_at=pl.col("created_at")
        ),
        items,
    )
