import unittest

import polars as pl

from datagen.oltp.plan import CustomerBatch, build_timeline
from datagen.oltp.run import online_batch
from datagen.oltp.tables import SCHEMAS
from fixtures import tiny_preset, tiny_preset_with
from oltp.helpers import fresh_ids, reference_for


def run_batch(preset, index=1, customer_count=330, buyer_count=300):
    return online_batch(
        preset,
        build_timeline(preset),
        reference_for(preset),
        fresh_ids(1000),
        CustomerBatch(index, customer_count, buyer_count),
    )


class SmallBatchTests(unittest.TestCase):
    def test_batches_with_no_or_one_buyer_produce_conforming_tables(self):
        preset = tiny_preset()
        for customer_count, buyer_count in ((5, 0), (1, 0), (1, 1)):
            with self.subTest(customers=customer_count, buyers=buyer_count):
                tables, demand, coupon_usage, customer_cities, order_count = run_batch(
                    preset, customer_count=customer_count, buyer_count=buyer_count
                )
                for name, frame in tables.items():
                    self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)
                self.assertEqual(tables["customers"].height, customer_count)
                self.assertEqual(order_count, tables["orders"].height)
                if buyer_count == 0:
                    self.assertEqual(order_count, 0)
                    self.assertEqual(demand.height, 0)


class InactiveCustomerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # A high inactive rate makes clashes with orders, carts and wishlists certain in a small batch.
        cls.preset = tiny_preset_with("policies.customers.inactive_rate", 0.5)
        cls.tables = run_batch(cls.preset, customer_count=2_000, buyer_count=1_800)[0]
        cls.inactive = (
            cls.tables["customers"]
            .filter(pl.col("status") == "inactive")
            .select("customer_id", pl.col("updated_at").alias("closed_at"))
        )

    def test_inactive_customers_have_no_open_cart(self):
        open_carts = self.tables["carts"].filter(pl.col("status") == "active")
        self.assertGreater(open_carts.height, 0)
        self.assertEqual(open_carts.join(self.inactive, on="customer_id").height, 0)

    def test_account_closes_after_the_customers_last_activity(self):
        activity = pl.concat(
            [
                self.tables[name].select("customer_id", pl.col(column).alias("at"))
                for name, column in (
                    ("orders", "updated_at"),
                    ("wishlist_items", "updated_at"),
                    ("carts", "updated_at"),
                )
            ]
        )
        late = activity.join(self.inactive, on="customer_id").filter(pl.col("at") > pl.col("closed_at"))
        self.assertGreater(self.inactive.height, 0)
        self.assertEqual(late.height, 0)
        self.assertTrue((self.inactive["closed_at"] <= self.preset.anchor_time).all())


class OnlineBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        cls.result = run_batch(cls.preset)

    def test_online_batch_is_reproducible(self):
        again = run_batch(self.preset)
        for name, frame in self.result[0].items():
            self.assertTrue(frame.equals(again[0][name]), name)

    def test_other_seed_or_batch_differs(self):
        self.assertFalse(self.result[0]["orders"].equals(run_batch(tiny_preset_with("seed", 1))[0]["orders"]))
        self.assertFalse(self.result[0]["orders"].equals(run_batch(self.preset, index=2)[0]["orders"]))

    def test_tables_conform_and_link(self):
        tables, demand, coupon_usage, customer_cities, order_count = self.result
        for name, frame in tables.items():
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)
        orders = tables["orders"]
        self.assertEqual(orders.height, order_count)
        self.assertEqual(orders["cart_id"].null_count(), 0)
        self.assertEqual(orders["cart_id"].n_unique(), orders.height)
        self.assertTrue(set(tables["order_items"]["order_id"].to_list()) <= set(orders["order_id"].to_list()))
        self.assertEqual(orders["generation_run_id"].unique().to_list(), [self.preset.run_id])
        self.assertEqual(tables["customers"].height, 330)
        self.assertTrue(set(customer_cities.columns) == {"customer_id", "city_id"})
        self.assertEqual(
            int(coupon_usage["used_count"].sum()),
            tables["coupon_redemptions"].filter(pl.col("status") == "redeemed").height,
        )
        self.assertEqual(set(demand.columns), {"variant_id", "store_id", "yearmonth", "outbound", "net"})

    def test_cod_counters_reach_customers(self):
        tables = self.result[0]
        failures = tables["orders"].filter(pl.col("status") == "failed_delivery").group_by("customer_id").len()
        joined = tables["customers"].join(failures, on="customer_id")
        self.assertTrue((joined["boom_count"] == joined["len"]).all())


if __name__ == "__main__":
    unittest.main()
