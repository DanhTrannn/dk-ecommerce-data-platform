"""Rule checks: each measures one preset rule on the generated rows with a single aggregate query."""

from datetime import timedelta

from datagen.distributions import (
    expected_orders_per_buyer,
    lognormal_top_share,
    log_series_theta,
    repeat_gap_mean_days,
    single_order_share,
)
from datagen.oltp.plan import build_timeline, sale_event_dates
from datagen.postgres import number
from datagen.presets import OltpPreset
from datagen.verify.report import Check, check

OLD_ORDER_DAYS = 45  # old enough that every event of the order has already happened

ONLINE = "channel = 'online' AND data_origin = 'synthetic'"
POS = "channel = 'pos' AND data_origin = 'synthetic'"
OLD = f"{ONLINE} AND created_at < %(old_before)s"
DELIVERED = "status IN ('delivered', 'completed', 'returned')"
LOCAL_DAY = "(created_at AT TIME ZONE %(zone)s)::date"
ORDERS_PER_BUYER = f"SELECT count(*) AS n FROM orders WHERE {ONLINE} GROUP BY customer_id"

MEASURES = {
    "r01.orders_per_buyer": f"SELECT avg(n) FROM ({ORDERS_PER_BUYER}) buyers",
    "r01.single_order_share": f"SELECT avg((n = 1)::int) FROM ({ORDERS_PER_BUYER}) buyers",
    "r02.mean_gap_days_two_orders": f"""
        SELECT avg(extract(epoch FROM last_order - first_order) / 86400)
        FROM (
            SELECT min(created_at) AS first_order, max(created_at) AS last_order
            FROM orders WHERE {ONLINE} GROUP BY customer_id HAVING count(*) = 2
        ) buyers""",
    "r03.top_decile_unit_share": """
        SELECT sum(units) FILTER (WHERE position <= products * 0.10) / sum(units)
        FROM (
            SELECT units, row_number() OVER (ORDER BY units DESC) AS position,
                   (SELECT count(*) FROM products) AS products
            FROM (SELECT sum(quantity) AS units FROM order_items GROUP BY product_public_id_snapshot) sold
        ) ranked""",
    "r04.products_per_order": """
        SELECT avg(n)
        FROM (SELECT count(DISTINCT product_public_id_snapshot) AS n FROM order_items GROUP BY order_id) baskets""",
    "r05.sale_day_ratio": f"""
        SELECT avg(n) FILTER (WHERE day = ANY(%(sale_days)s)) / percentile_cont(0.5) WITHIN GROUP (ORDER BY n)
        FROM (SELECT {LOCAL_DAY} AS day, count(*) AS n FROM orders WHERE {ONLINE} GROUP BY 1) daily""",
    "outcome.cod_share": f"SELECT avg((payment_method = 'cod')::int) FROM orders WHERE {OLD}",
    "outcome.cancellation_rate": f"SELECT avg((status = 'cancelled')::int) FROM orders WHERE {OLD}",
    "outcome.cod_failure_rate": f"""
        SELECT avg((status = 'failed_delivery')::int)
        FROM orders WHERE {OLD} AND payment_method = 'cod' AND status <> 'cancelled'""",
    "outcome.return_rate": f"""
        SELECT avg((EXISTS (SELECT 1 FROM return_requests r WHERE r.order_id = o.order_id))::int)
        FROM orders o WHERE {OLD} AND {DELIVERED}""",
    "outcome.exchange_share": "SELECT avg((action_type = 'exchange')::int) FROM return_requests",
    "coupon.usage_rate": f"""
        SELECT avg((coupon_id IS NOT NULL)::int)
        FROM orders
        WHERE {ONLINE} AND subtotal_vnd >= %(minimum_subtotal)s AND {LOCAL_DAY} <> ALL(%(sale_days)s)""",
    "r06.review_write_rate": f"""
        SELECT avg((EXISTS (
            SELECT 1 FROM order_items i JOIN product_reviews r USING (order_item_id) WHERE i.order_id = o.order_id
        ))::int)
        FROM orders o WHERE {OLD} AND {DELIVERED}""",
    "r06.on_time_five_star_share": """
        SELECT avg((r.rating = 5)::int)
        FROM product_reviews r
        JOIN order_items i USING (order_item_id)
        JOIN orders o USING (order_id)
        JOIN shipments s USING (order_id)
        WHERE s.delivered_at - o.created_at < make_interval(days => %(on_time_days)s + 1)""",
    "pos.orders_per_online_order": """
        SELECT count(*) FILTER (WHERE channel = 'pos')::float / nullif(count(*) FILTER (WHERE channel = 'online'), 0)
        FROM orders WHERE data_origin = 'synthetic'""",
    "pos.walk_in_rate": f"SELECT avg((customer_id IS NULL)::int) FROM orders WHERE {POS}",
    "pos.cash_share": f"SELECT avg((payment_method = 'cash')::int) FROM orders WHERE {POS}",
    "location.largest_city_customer_share": """
        SELECT max(n) / sum(n)
        FROM (
            SELECT count(*) AS n FROM customers
            WHERE role = 'customer' AND data_origin = 'synthetic' GROUP BY city_id
        ) cities""",
}


def expectations(preset: OltpPreset) -> dict[str, float]:
    theta = log_series_theta(preset.customers.purchase_frequency.alpha_years, preset.history_years)
    history_days = len(build_timeline(preset).day_weight)
    outcomes, pos = preset.outcomes, preset.pos
    five_star = preset.reviews.rating_by_lateness.on_time
    return {
        "r01.orders_per_buyer": expected_orders_per_buyer(theta),
        "r01.single_order_share": single_order_share(theta),
        "r02.mean_gap_days_two_orders": float(
            repeat_gap_mean_days(history_days, 2, preset.customers.repeat_gap.shrink_c)
        ),
        "r03.top_decile_unit_share": lognormal_top_share(preset.products.popularity.sigma, 0.10),
        "r04.products_per_order": 1 / preset.baskets.products.p,
        "r05.sale_day_ratio": preset.calendar.sale_events.event_day_multiplier,
        "outcome.cod_share": outcomes.cod_rate,
        "outcome.cancellation_rate": outcomes.cancellation_rate,
        "outcome.cod_failure_rate": outcomes.cod_failure_rate,
        "outcome.return_rate": outcomes.return_rate,
        "outcome.exchange_share": outcomes.exchange_rate,
        "coupon.usage_rate": preset.coupons.usage_rate,
        "r06.review_write_rate": preset.reviews.write_rate,
        "r06.on_time_five_star_share": five_star[4] / sum(five_star),
        "pos.orders_per_online_order": pos.orders_per_online_order,
        "pos.walk_in_rate": pos.walk_in_rate,
        "pos.cash_share": pos.payment_methods.cash / sum(pos.payment_methods.model_dump().values()),
        "location.largest_city_customer_share": max(city.share for city in preset.locations.cities),
    }


def checks(connection, preset: OltpPreset) -> list[Check]:
    parameters = {
        "old_before": preset.anchor_time - timedelta(days=OLD_ORDER_DAYS),
        "zone": preset.business_timezone,
        "sale_days": list(sale_event_dates(preset)),
        "minimum_subtotal": preset.coupons.minimum_subtotal_vnd,
        "on_time_days": preset.fulfilment.on_time_delivery_days[1],
    }
    expected = expectations(preset)
    return [check(name, expected[name], number(connection, sql, parameters)) for name, sql in MEASURES.items()]
