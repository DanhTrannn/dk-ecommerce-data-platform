import numpy as np
import polars as pl

from datagen.distributions import log_series_theta
from datagen.oltp.plan import IdAllocator
from datagen.oltp.reference import build_reference
from datagen.oltp.tables import ID_COLUMNS
from datagen.randomness import REFERENCE, stream


def fresh_ids(start=1):
    return IdAllocator({table: start for table in ID_COLUMNS})


def reference_for(preset):
    return build_reference(preset, stream(preset.seed, REFERENCE), fresh_ids(), {})[1]


def many_customers(preset, count, seed=5):
    rng = np.random.default_rng(seed)
    theta = log_series_theta(preset.customers.purchase_frequency.alpha_years, preset.history_years)
    return pl.DataFrame(
        {
            "customer_id": np.arange(1, count + 1),
            "display_name": ["Khách thử"] * count,
            "city_id": np.ones(count, dtype=np.int64),
            "order_count": rng.logseries(theta, count),
        }
    )


def online_orders(preset, customer_count=330, buyer_count=300, seed_batch=0):
    """Build the customers, lifecycle, items and order tables of one online batch, as run.py does."""
    from datagen.oltp.customers import generate_customers
    from datagen.oltp.lifecycle import draw_lifecycle
    from datagen.oltp.order_tables import order_tables
    from datagen.oltp.orders import draw_order_items, draw_order_times, price_orders
    from datagen.oltp.plan import CustomerBatch, build_timeline
    from datagen.randomness import ONLINE

    reference = reference_for(preset)
    timeline = build_timeline(preset)
    ids = fresh_ids(1000)
    rng = stream(preset.seed, ONLINE, seed_batch)
    customers = generate_customers(preset, reference, rng, ids, CustomerBatch(seed_batch, customer_count, buyer_count))
    orders = draw_order_times(preset, timeline, rng, customers)
    items = draw_order_items(preset, reference, rng, orders.height)
    orders = price_orders(preset, reference, rng, orders, items, "online")
    lifecycle = draw_lifecycle(preset, reference, rng, orders, items)
    tables, lifecycle, items = order_tables(preset, lifecycle, items, reference, rng, ids)
    return {
        "reference": reference,
        "timeline": timeline,
        "ids": ids,
        "rng": rng,
        "customers": customers,
        "lifecycle": lifecycle,
        "items": items,
        "tables": tables,
    }
