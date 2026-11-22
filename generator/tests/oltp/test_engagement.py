import unittest

import polars as pl

from datagen.oltp.engagement import cart_tables, wishlist_table
from datagen.oltp.tables import SCHEMAS
from fixtures import tiny_preset, tiny_preset_with
from oltp.helpers import online_orders


class NoActiveCartTests(unittest.TestCase):
    def test_batch_without_active_carts_keeps_conforming_tables(self):
        preset = tiny_preset_with("policies.carts.active_cart_rate", 0)
        b = online_orders(preset)
        carts, order_cart = cart_tables(
            preset, b["lifecycle"], b["items"], b["customers"], b["reference"], b["rng"], b["ids"]
        )
        for name, frame in carts.items():
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)
        self.assertEqual(carts["carts"].filter(pl.col("status") == "active").height, 0)
        self.assertEqual(order_cart.height, b["lifecycle"].height)


class WishlistAfterSignupTests(unittest.TestCase):
    def test_wishlist_items_are_added_after_the_account_exists(self):
        # Customers join the day before history starts and add items up to 60 days before buying,
        # so without clamping, early buyers would add items before their account existed.
        preset = tiny_preset_with("policies.customers.created_before_history_days", [1, 1])
        preset = preset.model_copy(
            update={
                "policies": preset.policies.model_copy(
                    update={
                        "wishlist": preset.policies.wishlist.model_copy(
                            update={"creation_rate": 1.0, "added_before_purchase_days": (30, 60)}
                        )
                    }
                )
            }
        )
        b = online_orders(preset, customer_count=2_000, buyer_count=1_800)
        wishlist = wishlist_table(
            preset, b["lifecycle"], b["items"], b["customers"], b["reference"], b["rng"], b["ids"]
        )
        joined = wishlist.join(
            b["customers"].select("customer_id", pl.col("created_at").alias("joined")), on="customer_id"
        )
        self.assertGreater(joined.height, 0)
        self.assertTrue((joined["first_added_at"] >= joined["joined"]).all())
        self.assertTrue((joined["last_added_at"] >= joined["first_added_at"]).all())


class EngagementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        b = online_orders(cls.preset, customer_count=3_000, buyer_count=2_700)
        cls.lc, cls.customers = b["lifecycle"], b["customers"]
        cls.carts, cls.order_cart = cart_tables(
            cls.preset, cls.lc, b["items"], cls.customers, b["reference"], b["rng"], b["ids"]
        )
        cls.wishlist = wishlist_table(cls.preset, cls.lc, b["items"], cls.customers, b["reference"], b["rng"], b["ids"])

    def test_tables_conform(self):
        for name, frame in self.carts.items():
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)
        self.assertEqual(self.wishlist.schema, pl.Schema(SCHEMAS["wishlist_items"]))

    def test_every_online_order_has_its_own_checked_out_cart(self):
        self.assertEqual(self.order_cart.height, self.lc.height)
        self.assertEqual(self.order_cart["cart_id"].n_unique(), self.lc.height)
        checked = self.carts["carts"].filter(pl.col("status") == "checked_out")
        self.assertEqual(checked.height, self.lc.height)
        self.assertTrue((checked["created_at"] < checked["checked_out_at"]).all())
        joined = checked.join(
            self.customers.select("customer_id", pl.col("created_at").alias("joined")), on="customer_id"
        )
        self.assertTrue((joined["created_at"] >= joined["joined"]).all())

    def test_at_most_one_active_cart_per_customer_and_unique_items(self):
        active = self.carts["carts"].filter(pl.col("status") == "active")
        self.assertGreater(active.height, 0)
        self.assertEqual(int(active["customer_id"].is_duplicated().sum()), 0)
        self.assertEqual(active["checked_out_at"].null_count(), active.height)
        self.assertTrue((active["updated_at"] <= self.preset.anchor_time).all())
        self.assertEqual(int(self.carts["cart_items"].select("cart_id", "variant_id").is_duplicated().sum()), 0)
        self.assertTrue(set(active["cart_id"].to_list()) <= set(self.carts["cart_items"]["cart_id"].to_list()))

    def test_wishlist_constraints(self):
        w = self.wishlist
        self.assertGreater(w.height, 0)
        self.assertEqual(int(w.select("customer_id", "product_id").is_duplicated().sum()), 0)
        self.assertTrue((w["last_added_at"] >= w["first_added_at"]).all())
        removed = w.filter(~pl.col("is_present"))
        self.assertGreater(removed.height, 0)
        self.assertTrue((removed["removed_at"] >= removed["last_added_at"]).all())
        present = w.filter(pl.col("is_present"))
        self.assertEqual(present["removed_at"].null_count(), present.height)
        self.assertTrue((w["updated_at"] <= self.preset.anchor_time).all())

    def test_wishlists_include_purchased_products(self):
        share = self.wishlist.join(self.purchased(), on=["customer_id", "product_id"], how="semi").height
        self.assertGreater(share / self.wishlist.height, 0.2)

    def purchased(self):
        b = online_orders(self.preset, customer_count=3_000, buyer_count=2_700)
        return (
            b["items"]
            .join(b["lifecycle"].select("order_id", "customer_id"), on="order_id")
            .select("customer_id", "product_id")
            .unique()
        )


if __name__ == "__main__":
    unittest.main()
