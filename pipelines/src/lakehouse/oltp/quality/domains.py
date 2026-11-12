"""Independent domain expectations for current stock, operations, marketing and admin."""

from collections import Counter
import math

import polars as pl

from lakehouse.utils.frames import query


def differences(expected, actual):
    if not set(expected.columns).issubset(actual.columns):
        return max(1, expected.height)
    left = Counter(expected.iter_rows())
    right = Counter(actual.select(expected.columns).iter_rows())
    return sum((left - right).values()) + sum((right - left).values())


def domain_checks(frames, record):
    def compare(name, table, sql, *, clock=None):
        inputs = frames if clock is None else {**frames, "clock": clock}
        expected = query(inputs, sql)
        bad = differences(expected, frames[table])
        record(name, table, bad, 0, bad == 0, "Independent source aggregate parity", bad)

    stock = frames["fact_inventory_current"].select(
        "location_type",
        "location_id",
        pl.col("variant_key").alias("variant_id"),
        pl.col("on_hand_quantity").alias("on_hand"),
        "reserved_quantity",
        "unit_cost_vnd",
        "inventory_value_cost_vnd",
    )
    bad = differences(stock, frames["mart_inventory_current"])
    record(
        "current_stock_balance_parity",
        "mart_inventory_current",
        bad,
        0,
        bad == 0,
        "Current balances and costs match",
        bad,
    )
    for table, keys in (
        ("mart_inventory_current", ["location_type", "location_id", "variant_id"]),
        ("mart_store_overview", ["store_id"]),
    ):
        bad = (
            frames[table]
            .group_by(keys)
            .len()
            .filter((pl.col("len") > 1) | pl.any_horizontal([pl.col(k).is_null() for k in keys]))
            .height
        )
        record("inventory_serving_grain", table, bad, 0, bad == 0, "Unique non-null serving keys", bad)
    compare(
        "inventory_summary_parity",
        "mart_inventory_summary",
        """WITH x AS (SELECT count(*) inventory_rows,
        sum(inventory_value_cost_vnd) total_inventory_value_vnd,sum(CASE WHEN location_type='warehouse' THEN on_hand_quantity END) warehouse_stock_units,
        sum(CASE WHEN location_type='store' THEN on_hand_quantity END) store_stock_units,
        CASE WHEN count(*)>0 THEN count(DISTINCT CASE WHEN is_out_of_stock THEN variant_key END) END stockout_count FROM fact_inventory_current)
        SELECT 'current' summary_key,x.*,r.inbound_batches_count FROM x CROSS JOIN (SELECT count(*) inbound_batches_count FROM inbound_receipts) r""",
    )
    bad = differences(frames["dim_store"].select("store_id"), frames["mart_store_overview"])
    record("store_coverage", "mart_store_overview", bad, 0, bad == 0, "All stores retained", bad)
    ops = frames["mart_operations_summary"]
    opclock = ops.filter(pl.col("summary_key") == "current").select(
        pl.col("operations_observed_at").alias("observed_at")
    )
    expected = query(
        {**frames, "clock": opclock},
        """WITH o AS (SELECT count(*) order_rows,
        sum(CASE WHEN status IN ('paid','confirmed') THEN 1 ELSE 0 END) pending_orders,
        sum(CASE WHEN status IN ('paid','confirmed') AND created_at<=observed_at-INTERVAL '2 hours' THEN 1 ELSE 0 END) over_two_hours_orders,
        avg(CASE WHEN status IN ('paid','confirmed') AND created_at<=observed_at THEN
        (FLOOR(EXTRACT(EPOCH FROM observed_at))-FLOOR(EXTRACT(EPOCH FROM created_at)))/3600.0 END) average_waiting_hours
        FROM fact_order CROSS JOIN clock),s AS (SELECT count(*) shipment_rows,sum(CASE WHEN attempt_count>1 OR status='failed' THEN 1 ELSE 0 END) problematic_shipments,
        sum(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed_shipments FROM fact_shipment),r AS (
        SELECT count(*) return_rows,sum(CASE WHEN status<>'cancelled' THEN 1 ELSE 0 END) return_requests_count FROM return_requests),
        f AS (SELECT count(*) refund_rows,sum(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) successful_refunds_count,
        sum(CASE WHEN status='succeeded' THEN amount_vnd ELSE 0 END) successful_refund_amount_vnd FROM refunds)
        SELECT o.*,s.*,r.*,f.* FROM o CROSS JOIN s CROSS JOIN r CROSS JOIN f""",
    )
    good = (
        ops.height == 1
        and expected.height == 1
        and ops["summary_key"][0] == "current"
        and ops["operations_observed_at"][0] is not None
    )
    if good:
        for name in expected.columns:
            a, b = expected[name][0], ops[name][0]
            good = good and (
                (a is None and b is None)
                or (
                    a is not None
                    and b is not None
                    and (math.isclose(a, b, rel_tol=0, abs_tol=1e-6) if name == "average_waiting_hours" else a == b)
                )
            )
    record(
        "operations_summary_parity",
        "mart_operations_summary",
        str(good),
        "True",
        good,
        "Frozen observations and independent source counts",
    )
    actual = query(
        frames,
        """SELECT coalesce(sum(total_shipments),0) shipments,coalesce(sum(delivered_count),0) delivered,
        coalesce(sum(boom_count),0) failed,coalesce(sum(problematic_shipments),0) problems,
        coalesce(sum(on_time_eligible_count),0) eligible,coalesce(sum(on_time_count),0) on_time,
        coalesce(sum(duration_sample_count),0) samples FROM mart_logistics_performance""",
    )
    expected = query(
        frames,
        """SELECT count(*) shipments,count(CASE WHEN status='delivered' THEN 1 END) delivered,
        count(CASE WHEN status='failed' THEN 1 END) failed,count(CASE WHEN attempt_count>1 OR status='failed' THEN 1 END) problems,
        count(CASE WHEN status='delivered' AND is_on_time IS NOT NULL THEN 1 END) eligible,
        count(CASE WHEN status='delivered' AND is_on_time THEN 1 END) on_time,count(delivery_duration_minutes) samples FROM fact_shipment""",
    )
    bad = differences(expected, actual)
    record(
        "logistics_rollup_parity",
        "mart_logistics_performance",
        bad,
        0,
        bad == 0,
        "Unknown deadlines remain ineligible",
        bad,
    )
    compare(
        "cod_region_parity",
        "mart_cod_region_daily",
        """SELECT s.shipment_date,coalesce(o.shipping_region,'Khu vực khác') region,
        count(*) total_cod_shipments,sum(CASE WHEN s.status='failed' THEN 1 ELSE 0 END) failed_cod_shipments FROM fact_shipment s
        JOIN fact_order o ON s.order_id=o.order_id WHERE o.payment_method='cod' GROUP BY s.shipment_date,region""",
    )
    compare(
        "product_return_grain_parity",
        "mart_product_returns",
        """SELECT return_date,returned_variant_key,action_type,coalesce(reason,'Unknown') reason,
        count(DISTINCT return_id) total_return_requests,count(*) return_item_count,sum(returned_quantity) total_returned_quantity,
        sum(CASE WHEN return_status='completed' AND inspection_status='passed' THEN returned_quantity ELSE 0 END) accepted_return_quantity,
        sum(refund_amount_vnd) requested_refund_amount_vnd FROM fact_return_exchange WHERE return_status<>'cancelled'
        GROUP BY return_date,returned_variant_key,action_type,reason""",
    )
    marketing = frames["mart_marketing_summary"]
    good = (
        marketing.height == 1
        and marketing["summary_key"][0] == "current"
        and marketing["marketing_observed_at"][0] is not None
        and marketing["coupon_rows"][0] == frames["coupons"].height
        and marketing["review_rows"][0] == frames["product_reviews"].height
    )
    record("marketing_summary_parity", "mart_marketing_summary", str(good), "True", good, "Independent source coverage")
    mc = marketing.select(pl.col("marketing_observed_at").alias("observed_at"))
    compare(
        "marketing_grain_parity",
        "mart_coupon_usage",
        """WITH r AS (SELECT coupon_id,count(*) n FROM coupon_redemptions CROSS JOIN clock
        WHERE status='redeemed' AND redeemed_at BETWEEN observed_at-INTERVAL '15 minutes' AND observed_at GROUP BY coupon_id)
        SELECT c.coupon_id,c.code_normalized coupon_code,c.used_count,c.total_usage_limit usage_limit,
        (c.is_active AND c.archived_at IS NULL AND c.starts_at<=m.observed_at AND c.ends_at>=m.observed_at) is_available,
        coalesce(r.n,0) recent_redemptions_count,m.observed_at marketing_observed_at FROM coupons c CROSS JOIN clock m LEFT JOIN r ON c.coupon_id=r.coupon_id""",
        clock=mc,
    )
    compare(
        "marketing_grain_parity",
        "mart_product_review_summary",
        """SELECT r.product_id,coalesce(p.product_name,'Unknown') product_name,count(*) review_count,
        sum(CASE WHEN r.status='approved' AND r.rating<=2 AND r.created_at BETWEEN m.observed_at-INTERVAL '45 minutes' AND m.observed_at THEN 1 ELSE 0 END) recent_negative_count,
        m.observed_at marketing_observed_at FROM product_reviews r CROSS JOIN clock m LEFT JOIN dim_product p ON r.product_id=p.product_id
        GROUP BY r.product_id,p.product_name,m.observed_at""",
        clock=mc,
    )
    admin_checks(frames, record, compare)


def admin_checks(frames, record, compare):
    compare(
        "admin_domain_parity",
        "mart_admin_overview",
        """WITH p AS (SELECT count(*) product_rows,sum(CAST(is_active AS BIGINT)) active_products FROM products),
        v AS (SELECT count(*) variant_rows FROM product_variants),a AS (SELECT v.variant_id FROM product_variants v JOIN products p ON v.product_id=p.product_id WHERE v.is_active AND p.is_active),
        i AS (SELECT count(*) inventory_rows FROM fact_inventory_current WHERE location_type='warehouse'),l AS (
        SELECT count(*) low_stock_variants FROM fact_inventory_current i JOIN a ON i.variant_key=a.variant_id WHERE i.location_type='warehouse' AND i.is_low_stock),
        c AS (SELECT count(*) customer_rows,sum(CASE WHEN role='customer' THEN 1 ELSE 0 END) customers FROM customers),r AS (SELECT count(*) review_rows FROM product_reviews),
        q AS (SELECT count(*) coupon_rows FROM coupons),ac AS (SELECT count(*) n FROM a)
        SELECT p.*,v.variant_rows,i.inventory_rows,c.*,r.review_rows,q.coupon_rows,
        CASE WHEN p.product_rows>0 AND v.variant_rows>0 THEN ac.n END active_variants,
        CASE WHEN p.product_rows>0 AND v.variant_rows>0 AND i.inventory_rows>0 THEN l.low_stock_variants END low_stock_variants,
        CASE WHEN r.review_rows>0 THEN r.review_rows END total_reviews FROM p CROSS JOIN v CROSS JOIN i CROSS JOIN l CROSS JOIN c CROSS JOIN r CROSS JOIN q CROSS JOIN ac""",
    )
    target = frames["mart_admin_overview"]
    clock = target.select(pl.col("admin_observed_at").alias("observed_at"))
    expected = query(
        {**frames, "clock": clock},
        """SELECT c.observed_at admin_observed_at,sum(CASE WHEN q.coupon_id IS NULL THEN NULL
        WHEN q.is_active AND q.archived_at IS NULL AND q.starts_at<=c.observed_at AND q.ends_at>c.observed_at THEN 1 ELSE 0 END) active_coupons
        FROM clock c LEFT JOIN coupons q ON true GROUP BY c.observed_at""",
    )
    bad = (
        differences(expected, target)
        + target.filter(
            pl.col("summary_key").is_null()
            | (pl.col("summary_key") != "current")
            | pl.col("admin_observed_at").is_null()
        ).height
    )
    record(
        "admin_observation_parity",
        "mart_admin_overview",
        bad,
        0,
        bad == 0,
        "Coupon availability at stored observation",
        bad,
    )
    states = (
        "pending_payment",
        "paid",
        "confirmed",
        "shipping",
        "delivered",
        "completed",
        "cancelled",
        "failed_delivery",
        "returned",
    )
    sums = ",".join(f"sum(CASE WHEN status='{state}' THEN 1 ELSE 0 END) {state}_orders" for state in states)
    compare("admin_order_status_parity", "mart_admin_overview", f"SELECT count(*) order_rows,{sums} FROM fact_order")
