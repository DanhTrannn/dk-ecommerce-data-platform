import unittest

import polars as pl

from datagen.oltp.customers import generate_customers
from datagen.oltp.plan import CustomerBatch, IdAllocator
from datagen.oltp.reference import build_reference
from datagen.oltp.tables import ID_COLUMNS
from datagen.randomness import ONLINE, REFERENCE, stream
from fixtures import tiny_preset


def reference_for(preset):
    return build_reference(preset, stream(preset.seed, REFERENCE), IdAllocator({t: 1 for t in ID_COLUMNS}), {})[1]


class CustomerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        cls.reference = reference_for(cls.preset)
        ids = IdAllocator({t: 1000 for t in ID_COLUMNS})
        cls.customers = generate_customers(
            cls.preset, cls.reference, stream(1, ONLINE, 0), ids, CustomerBatch(0, 330, 300)
        )

    def test_batch_has_exact_buyer_count_and_order_counts(self):
        c = self.customers
        self.assertEqual(c.height, 330)
        self.assertEqual(int(c["is_buyer"].sum()), 300)
        self.assertTrue((c.filter(pl.col("is_buyer"))["order_count"] >= 1).all())
        self.assertEqual(int(c.filter(~pl.col("is_buyer"))["order_count"].sum()), 0)
        self.assertEqual(c["customer_id"].to_list(), list(range(1000, 1330)))

    def test_lineage_dates_and_cities(self):
        c = self.customers
        self.assertEqual(c["data_origin"].unique().to_list(), ["synthetic"])
        self.assertEqual(c["generation_run_id"].unique().to_list(), [self.preset.run_id])
        self.assertEqual(c["role"].unique().to_list(), ["customer"])
        self.assertTrue((c["created_at"] < self.preset.history_start).all())
        self.assertTrue((c["updated_at"] >= c["created_at"]).all())
        self.assertTrue((c["updated_at"] <= self.preset.anchor_time).all())
        self.assertTrue(set(c["city_id"].to_list()) <= set(self.reference.cities["city_id"].to_list()))
        self.assertEqual(c["public_id"].n_unique(), c.height)
        self.assertTrue(c["display_name"].str.len_chars().is_between(5, 120).all())

    def test_inactive_customers_change_before_anchor(self):
        inactive = self.customers.filter(pl.col("status") == "inactive")
        self.assertTrue((inactive["updated_at"] > self.preset.history_start).all())


if __name__ == "__main__":
    unittest.main()
