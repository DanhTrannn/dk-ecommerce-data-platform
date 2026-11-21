import unittest

import numpy as np
import polars as pl

from datagen.oltp.order_tables import order_tables
from datagen.oltp.plan import build_timeline
from datagen.oltp.pos import draw_pos_lifecycle, pos_order_counts
from datagen.randomness import POS, stream
from fixtures import tiny_preset
from oltp.helpers import fresh_ids, online_orders, reference_for


class PosTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        cls.timeline = build_timeline(cls.preset)
        cls.reference = reference_for(cls.preset)
        cls.store_id = int(cls.reference.stores["store_id"][0])
        cls.city_customers = np.arange(5000, 5100)
        cls.lifecycle, cls.items = draw_pos_lifecycle(
            cls.preset, cls.timeline, cls.reference, stream(1, POS), cls.store_id, 202510, 4_000, cls.city_customers
        )
        cls.tables, _, _ = order_tables(
            cls.preset, cls.lifecycle, cls.items, cls.reference, stream(2, POS), fresh_ids()
        )

    def test_pos_counts_sum_exactly_and_follow_shares(self):
        counts = pos_order_counts(self.preset, self.timeline, self.reference, 100_000)
        self.assertEqual(int(counts["order_count"].sum()), 15_000)
        self.assertEqual(counts["store_id"].n_unique(), self.preset.scale.stores)
        self.assertEqual(counts["yearmonth"].n_unique(), 12)
        oct_share = counts.filter(pl.col("yearmonth") == 202510)["order_count"].sum() / 15_000
        self.assertAlmostEqual(oct_share, 31 / 364, delta=0.01)

    def test_same_columns_as_online_lifecycle(self):
        online = online_orders(self.preset, customer_count=200, buyer_count=180)["lifecycle"]
        self.assertEqual(set(self.lifecycle.columns), set(online.columns) - {"order_id", "payment_id", "order_number"})

    def test_pos_orders_satisfy_pos_constraints(self):
        o = self.tables["orders"]
        self.assertEqual(o.height, 4_000)
        self.assertEqual(o["channel"].unique().to_list(), ["pos"])
        self.assertEqual(o["status"].unique().to_list(), ["completed"])
        for column in ("store_id", "staff_id", "paid_at", "completed_at"):
            self.assertEqual(o[column].null_count(), 0, column)
        self.assertTrue(set(o["payment_method"].to_list()) <= {"cash", "card", "vietqr"})
        self.assertTrue((o["shipping_fee_vnd"] == 0).all())
        self.assertEqual(o["coupon_id"].null_count(), o.height)
        hours = o["created_at"].dt.convert_time_zone("Asia/Ho_Chi_Minh").dt.hour()
        self.assertTrue(hours.is_between(9, 20).all())
        months = o["created_at"].dt.convert_time_zone("Asia/Ho_Chi_Minh").dt.month()
        self.assertEqual(months.unique().to_list(), [10])
        staff = set(self.reference.store_staff.filter(pl.col("store_id") == self.store_id)["customer_id"].to_list())
        self.assertTrue(set(o["staff_id"].to_list()) <= staff)

    def test_pos_ledger_and_history(self):
        tx = self.tables["inventory_transactions"]
        self.assertEqual(tx["movement_type"].unique().to_list(), ["outbound_pos"])
        self.assertEqual(tx["store_id"].unique().to_list(), [self.store_id])
        self.assertEqual(tx["location_type"].unique().to_list(), ["store"])
        history = self.tables["order_status_history"]
        self.assertEqual(history.height, 4_000)
        self.assertEqual(history["from_status"].null_count(), 4_000)
        self.assertTrue((self.tables["payments"]["status"] == "succeeded").all())

    def test_walk_in_share_and_payment_mix(self):
        self.assertAlmostEqual(float(self.lifecycle["customer_id"].is_null().mean()), 0.7, delta=0.03)
        self.assertAlmostEqual(float((self.lifecycle["payment_method"] == "cash").mean()), 0.5, delta=0.03)
        self.assertTrue(set(self.lifecycle["customer_id"].drop_nulls().to_list()) <= set(self.city_customers.tolist()))

    def test_empty_city_means_walk_ins_only(self):
        lifecycle, _ = draw_pos_lifecycle(
            self.preset,
            self.timeline,
            self.reference,
            stream(3, POS),
            self.store_id,
            202510,
            50,
            np.array([], dtype=np.int64),
        )
        self.assertEqual(lifecycle["customer_id"].null_count(), 50)


if __name__ == "__main__":
    unittest.main()
