import unittest
from datetime import date

import numpy as np
import polars as pl

from datagen.oltp.orders import draw_order_items, draw_order_times, price_orders
from datagen.oltp.plan import build_timeline
from datagen.randomness import ONLINE, stream
from fixtures import tiny_preset
from oltp.helpers import many_customers, reference_for


class OrderTimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        cls.timeline = build_timeline(cls.preset)
        cls.customers = many_customers(cls.preset, 60_000)
        cls.orders = draw_order_times(cls.preset, cls.timeline, stream(1, ONLINE), cls.customers)

    def test_every_buyer_gets_exactly_order_count_orders_inside_the_window(self):
        counts = self.orders.group_by("customer_id").len().join(self.customers, on="customer_id")
        self.assertTrue((counts["len"] == counts["order_count"]).all())
        self.assertEqual(self.orders.height, int(self.customers["order_count"].sum()))
        self.assertTrue((self.orders["created_at"] >= self.preset.history_start).all())
        self.assertTrue((self.orders["created_at"] < self.preset.anchor_time).all())
        self.assertEqual(self.orders["order_index"].to_list(), list(range(self.orders.height)))

    def test_orders_are_sorted_per_customer(self):
        resorted = self.orders.sort("customer_id", "created_at")
        self.assertTrue(resorted["created_at"].equals(self.orders["created_at"]))

    def test_sale_days_attract_more_orders(self):
        local = self.orders["created_at"].dt.convert_time_zone("Asia/Ho_Chi_Minh").dt.date()
        per_day = local.value_counts()
        on_sale = per_day.filter(pl.col("created_at") == date(2025, 11, 11))["count"][0]
        self.assertAlmostEqual(on_sale / per_day["count"].median(), 6.39, delta=1.0)
        self.assertEqual(self.orders.filter(local == date(2025, 11, 11))["sale_event"].unique().to_list(), ["11-11"])
        self.assertEqual(
            self.orders.filter(local == date(2025, 9, 10))["sale_event"].null_count(),
            int((local == date(2025, 9, 10)).sum()),
        )

    def test_sale_day_hours_peak_near_midnight(self):
        hours = (
            self.orders.filter(pl.col("sale_event").is_not_null())["created_at"]
            .dt.convert_time_zone("Asia/Ho_Chi_Minh")
            .dt.hour()
        )
        self.assertGreater(float((hours == 0).mean()), 0.035)


class OrderItemTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset(products=500)
        cls.reference = reference_for(cls.preset)
        cls.items = draw_order_items(cls.preset, cls.reference, stream(1, ONLINE), 50_000)

    def test_items_are_unique_per_order_and_follow_basket_rule(self):
        items = self.items
        self.assertEqual(int(items.select("order_index", "variant_id").is_duplicated().sum()), 0)
        per_order = (
            items.join(self.reference.variants, on="variant_id")
            .group_by("order_index")
            .agg(pl.col("product_id").n_unique())
        )
        self.assertEqual(per_order.height, 50_000)
        self.assertAlmostEqual(per_order["product_id"].mean(), 1 / 0.296, delta=0.12)
        self.assertTrue(items["quantity"].is_between(1, 3).all())

    def test_variant_and_quantity_tables(self):
        per_product = (
            self.items.join(self.reference.variants, on="variant_id").group_by("order_index", "product_id").len()
        )
        self.assertAlmostEqual(float((per_product["len"] == 1).mean()), 0.921, delta=0.01)
        self.assertAlmostEqual(float((self.items["quantity"] == 1).mean()), 0.869, delta=0.01)

    def test_popular_products_sell_more(self):
        units = (
            self.items.join(self.reference.variants, on="variant_id")
            .group_by("product_id")
            .agg(pl.col("quantity").sum())
            .join(self.reference.products, on="product_id")
        )
        self.assertGreater(np.corrcoef(units["quantity"], units["weight"])[0, 1], 0.9)


class PriceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        cls.reference = reference_for(cls.preset)
        timeline = build_timeline(cls.preset)
        rng = stream(2, ONLINE)
        orders = draw_order_times(cls.preset, timeline, rng, many_customers(cls.preset, 20_000))
        items = draw_order_items(cls.preset, cls.reference, rng, orders.height)
        cls.orders = price_orders(cls.preset, cls.reference, rng, orders, items, "online")
        cls.items = items

    def test_money_arithmetic(self):
        o = self.orders
        self.assertTrue((o["total_vnd"] == o["subtotal_vnd"] - o["discount_amount_vnd"] + o["shipping_fee_vnd"]).all())
        expected = (
            self.items.join(self.reference.variants, on="variant_id")
            .group_by("order_index")
            .agg((pl.col("price_vnd") * pl.col("quantity")).sum().alias("expected"))
        )
        joined = o.join(expected, on="order_index")
        self.assertTrue((joined["subtotal_vnd"] == joined["expected"]).all())

    def test_coupon_threshold_discount_and_shipping(self):
        o = self.orders
        with_coupon = o.filter(pl.col("coupon_id").is_not_null())
        self.assertGreater(with_coupon.height, 0)
        self.assertTrue((with_coupon["subtotal_vnd"] >= 199000).all())
        self.assertTrue(
            (
                with_coupon["discount_amount_vnd"]
                == with_coupon["subtotal_vnd"] * with_coupon["coupon_percentage"] // 100
            ).all()
        )
        self.assertTrue((o.filter(pl.col("coupon_id").is_null())["discount_amount_vnd"] == 0).all())
        self.assertTrue((o.filter(pl.col("subtotal_vnd") >= 700000)["shipping_fee_vnd"] == 0).all())
        self.assertTrue((o.filter(pl.col("subtotal_vnd") < 700000)["shipping_fee_vnd"] == 30000).all())

    def test_sale_coupons_only_on_their_day_and_within_customer_limits(self):
        with_coupon = self.orders.filter(pl.col("coupon_id").is_not_null())
        sale = with_coupon.filter(pl.col("coupon_code") != "SYN10")
        self.assertTrue((sale["coupon_code"] == "SALE-" + sale["sale_event"].str.to_uppercase()).all())
        self.assertTrue((sale.group_by("customer_id", "coupon_id").len()["len"] <= 1).all())
        ordinary = with_coupon.filter(pl.col("coupon_code") == "SYN10")
        self.assertTrue((ordinary.group_by("customer_id").len()["len"] <= 10).all())
        self.assertEqual(ordinary["sale_event"].null_count(), ordinary.height)

    def test_pos_pricing_has_no_coupon_or_shipping(self):
        pos = price_orders(
            self.preset,
            self.reference,
            stream(3, ONLINE),
            self.orders.select("order_index", "customer_id", "created_at", "sale_event"),
            self.items,
            "pos",
        )
        self.assertEqual(pos["coupon_id"].null_count(), pos.height)
        self.assertTrue((pos["shipping_fee_vnd"] == 0).all())
        self.assertTrue((pos["total_vnd"] == pos["subtotal_vnd"]).all())


if __name__ == "__main__":
    unittest.main()
