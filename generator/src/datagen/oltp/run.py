"""Orchestrate one generate run: checks, reference data, online and POS batches, inventory, finish.

Batches run one after another in a fixed order; each loads in its own transaction.
"""

import time

import numpy as np
import polars as pl
import psycopg2
from psycopg2.extras import execute_values

from datagen.oltp.customers import generate_customers
from datagen.oltp.engagement import cart_tables, wishlist_table
from datagen.oltp.fulfilment import cancellation_refunds, return_tables, review_table, shipment_tables
from datagen.oltp.inventory import demand_of, inventory_tables
from datagen.oltp.lifecycle import customer_cod_stats, draw_lifecycle
from datagen.oltp.order_tables import order_tables
from datagen.oltp.orders import draw_order_items, draw_order_times, price_orders
from datagen.oltp.plan import CustomerBatch, IdAllocator, Timeline, build_timeline, customer_batches
from datagen.oltp.pos import draw_pos_lifecycle, pos_order_counts
from datagen.oltp.reference import Reference, build_reference
from datagen.oltp.tables import conform
from datagen.postgres import (
    analyze,
    category_ids,
    check_schema,
    copy_tables,
    has_synthetic_data,
    next_ids,
    sync_sequences,
)
from datagen.presets import OltpPreset
from datagen.randomness import INVENTORY, ONLINE, POS, REFERENCE, stream


class AlreadyGenerated(Exception):
    pass


class BatchFailed(Exception):
    pass


def online_batch(
    preset: OltpPreset, timeline: Timeline, reference: Reference, ids: IdAllocator, batch: CustomerBatch
) -> tuple[dict[str, pl.DataFrame], pl.DataFrame, pl.DataFrame, pl.DataFrame, int]:
    """All rows of one customer batch: (tables, demand, coupon_usage, customer_cities, online_order_count)."""
    rng = stream(preset.seed, ONLINE, batch.index)
    customers = generate_customers(preset, reference, rng, ids, batch)
    orders = draw_order_times(preset, timeline, rng, customers)
    items = draw_order_items(preset, reference, rng, orders.height)
    orders = price_orders(preset, reference, rng, orders, items, "online")
    lifecycle = draw_lifecycle(preset, reference, rng, orders, items)
    tables, lifecycle, items = order_tables(preset, lifecycle, items, reference, rng, ids)
    tables |= shipment_tables(lifecycle, rng, ids)
    returns = return_tables(lifecycle, items, reference, rng, ids)
    refunds = cancellation_refunds(lifecycle, rng, ids)
    tables["product_reviews"] = review_table(lifecycle, items, reference, rng, ids)
    carts, order_cart = cart_tables(preset, lifecycle, items, customers, reference, rng, ids)
    tables |= carts
    tables["wishlist_items"] = wishlist_table(preset, lifecycle, items, customers, reference, rng, ids)

    tables["orders"] = (
        tables["orders"]
        .drop("cart_id")
        .join(order_cart, on="order_id", how="left", maintain_order="left")
        .select(tables["orders"].columns)
    )
    cod = customer_cod_stats(preset, lifecycle)
    tables["customers"] = conform(
        "customers",
        customers.drop("is_buyer", "order_count", "boom_count", "is_cod_blocked")
        .join(cod, on="customer_id", how="left", maintain_order="left")
        .with_columns(pl.col("boom_count").fill_null(0), pl.col("is_cod_blocked").fill_null(False)),
        null_columns=("cod_reinstated_at", "cod_reinstated_by", "cod_reinstatement_reason", "anonymized_at"),
    )
    tables["return_requests"] = returns["return_requests"]
    tables["return_items"] = returns["return_items"]
    tables["refunds"] = pl.concat([refunds, returns["refunds"]])
    tables["inventory_transactions"] = pl.concat([tables["inventory_transactions"], returns["inventory_transactions"]])

    coupon_usage = (
        tables["coupon_redemptions"]
        .filter(pl.col("status") == "redeemed")
        .group_by("coupon_id")
        .agg(pl.len().alias("used_count"), pl.col("redeemed_at").max().alias("last_redeemed_at"))
    )
    customer_cities = customers.filter(pl.col("status") == "active").select("customer_id", "city_id")
    demand = demand_of(preset, tables["inventory_transactions"])
    return tables, demand, coupon_usage, customer_cities, lifecycle.height


def pos_batch(
    preset: OltpPreset,
    timeline: Timeline,
    reference: Reference,
    ids: IdAllocator,
    store_id: int,
    yearmonth: int,
    order_count: int,
    batch_index: int,
    city_customers: np.ndarray,
) -> tuple[dict[str, pl.DataFrame], pl.DataFrame]:
    rng = stream(preset.seed, POS, batch_index)
    lifecycle, items = draw_pos_lifecycle(
        preset, timeline, reference, rng, store_id, yearmonth, order_count, city_customers
    )
    tables, _, _ = order_tables(preset, lifecycle, items, reference, rng, ids)
    return tables, demand_of(preset, tables["inventory_transactions"])


def _load(connection, tables: dict[str, pl.DataFrame], label: str, counts: dict[str, int], log) -> None:
    started = time.monotonic()
    try:
        loaded = copy_tables(connection, tables)
        connection.commit()
    except psycopg2.Error as error:
        connection.rollback()
        raise BatchFailed(f"Lô {label} lỗi: {error}") from error
    for table, rows in loaded.items():
        counts[table] = counts.get(table, 0) + rows
    log(f"{label}: {sum(loaded.values())} dòng, {time.monotonic() - started:.1f} giây")


def generate(preset: OltpPreset, connection, log=print) -> dict[str, int]:
    check_schema(connection)
    if has_synthetic_data(connection):
        raise AlreadyGenerated("Database đã có dữ liệu sinh; chạy 'datagen reset --yes' trước")
    ids = IdAllocator(next_ids(connection))
    timeline = build_timeline(preset)
    counts: dict[str, int] = {}

    tables, reference = build_reference(preset, stream(preset.seed, REFERENCE), ids, category_ids(connection))
    _load(connection, tables, "dữ liệu nền", counts, log)

    demands, usages, cities, online_orders = [], [], [], 0
    for batch in customer_batches(preset):
        tables, demand, usage, customer_cities, order_count = online_batch(preset, timeline, reference, ids, batch)
        _load(connection, tables, f"online {batch.index}", counts, log)
        demands.append(demand)
        usages.append(usage)
        cities.append(customer_cities)
        online_orders += order_count

    members = pl.concat(cities)
    store_city = dict(zip(reference.stores["store_id"].to_list(), reference.stores["city_id"].to_list()))
    for batch_index, row in enumerate(
        pos_order_counts(preset, timeline, reference, online_orders).iter_rows(named=True)
    ):
        if row["order_count"] == 0:
            continue
        city_customers = members.filter(pl.col("city_id") == store_city[row["store_id"]])["customer_id"].to_numpy()
        tables, demand = pos_batch(
            preset,
            timeline,
            reference,
            ids,
            row["store_id"],
            row["yearmonth"],
            row["order_count"],
            batch_index,
            city_customers,
        )
        _load(connection, tables, f"POS {batch_index}", counts, log)
        demands.append(demand)

    tables = inventory_tables(preset, timeline, reference, stream(preset.seed, INVENTORY), ids, pl.concat(demands))
    _load(connection, tables, "kho", counts, log)
    _update_coupons(preset, connection, pl.concat(usages))
    sync_sequences(connection)
    analyze(connection)
    connection.commit()
    return counts


def _update_coupons(preset: OltpPreset, connection, usage: pl.DataFrame) -> None:
    """Record how often each coupon was used; updated_at is set explicitly so the trigger keeps history time."""
    usage = usage.group_by("coupon_id").agg(pl.col("used_count").sum(), pl.col("last_redeemed_at").max())
    policy = preset.policies.coupons
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            """UPDATE coupons AS c
               SET used_count = v.used_count,
                   total_usage_limit = greatest(%s, v.used_count + %s),
                   updated_at = v.last_redeemed_at
               FROM (VALUES %%s) AS v(coupon_id, used_count, last_redeemed_at)
               WHERE c.coupon_id = v.coupon_id"""
            % (policy.minimum_total_usage_limit, policy.extra_total_usage_limit),
            usage.rows(),
            template="(%s::bigint, %s::bigint, %s::timestamptz)",
        )
    connection.commit()
