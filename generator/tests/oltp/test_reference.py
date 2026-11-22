import unittest

import polars as pl

from datagen.oltp.plan import IdAllocator
from datagen.oltp.reference import build_reference
from datagen.oltp.tables import ID_COLUMNS, SCHEMAS
from datagen.randomness import REFERENCE, stream
from fixtures import tiny_preset, tiny_preset_with


def build(preset, existing_category_ids=None):
    ids = IdAllocator({table: 1 for table in ID_COLUMNS})
    return build_reference(preset, stream(preset.seed, REFERENCE), ids, existing_category_ids or {})


class ReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        cls.tables, cls.ref = build(cls.preset)

    def test_tables_conform(self):
        for name, frame in self.tables.items():
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)

    def test_counts(self):
        t = self.tables
        self.assertEqual(t["cities"].height, 6)
        self.assertEqual(t["stores"].height, 2)
        self.assertEqual(t["products"].height, 40)
        self.assertEqual(t["product_variants"].height, 160)
        self.assertEqual(t["delivery_staff"].height, 20)
        self.assertEqual(t["categories"].height, 8)
        self.assertEqual(t["customers"].filter(pl.col("role") == "store_manager").height, 6)
        self.assertEqual(t["customers"].filter(pl.col("role") == "operations_manager").height, 1)

    def test_largest_city_has_a_store(self):
        hcm = self.tables["cities"].filter(pl.col("code") == "HCM")["city_id"][0]
        self.assertIn(hcm, self.tables["stores"]["city_id"].to_list())

    def test_coupons(self):
        coupons = self.tables["coupons"]
        self.assertEqual(
            coupons["code_normalized"].to_list(), ["SYN10", "SALE-11-11", "SALE-12-12", "SALE-BLACK_FRIDAY"]
        )
        self.assertEqual(coupons["discount_value"].to_list(), [10, 15, 15, 15])
        self.assertEqual(coupons["per_customer_usage_limit"].to_list(), [10, 1, 1, 1])
        self.assertTrue((coupons["used_count"] == 0).all())
        self.assertEqual(self.ref.coupons["sale_event"].to_list(), [None, "11-11", "12-12", "black_friday"])

    def test_staff_and_back_office_are_synthetic(self):
        customers = self.tables["customers"]
        self.assertEqual(customers["data_origin"].unique().to_list(), ["synthetic"])
        self.assertEqual(customers["generation_run_id"].unique().to_list(), [self.preset.run_id])
        staff = customers.filter(pl.col("role") == "store_manager")
        self.assertEqual(staff["store_id"].null_count(), 0)
        self.assertEqual(self.ref.store_staff.height, 6)

    def test_master_data_predates_history(self):
        for name, frame in self.tables.items():
            if "created_at" in frame.columns:
                self.assertTrue((frame["created_at"] < self.preset.history_start).all(), name)

    def test_prices_costs_and_unique_keys(self):
        v = self.tables["product_variants"]
        self.assertTrue((v["price_vnd"] % 1000 == 0).all())
        self.assertTrue(v["price_vnd"].is_between(79000, 799000).all())
        ratio = v["cost_price_vnd"] / v["price_vnd"]
        self.assertTrue(ratio.is_between(0.349, 0.451).all())
        self.assertEqual(v["sku"].str.to_lowercase().n_unique(), v.height)
        self.assertEqual(self.tables["products"]["slug"].n_unique(), 40)
        self.assertEqual(self.tables["delivery_staff"]["phone"].n_unique(), 20)

    def test_archived_products_are_excluded_from_sales(self):
        tables, ref = build(tiny_preset(products=400))
        archived = tables["products"].filter(~pl.col("is_active"))
        self.assertEqual(archived.height, 20)
        self.assertEqual(archived["archived_by_customer_id"].unique().to_list(), [ref.back_office_customer_id])
        self.assertTrue(set(ref.variants["product_id"].to_list()).isdisjoint(archived["product_id"].to_list()))
        self.assertEqual(ref.products.height, 380)
        self.assertAlmostEqual(ref.products["weight"].sum(), 1.0)

    def test_existing_categories_are_reused(self):
        tables, ref = build(self.preset, existing_category_ids={"ao": 901})
        self.assertEqual(tables["categories"].height, 7)
        self.assertNotIn("ao", tables["categories"]["code"].to_list())
        self.assertIn(901, tables["products"]["category_id"].to_list())

    def test_same_seed_same_reference_and_other_seed_differs(self):
        again, _ = build(self.preset)
        for name in self.tables:
            self.assertTrue(self.tables[name].equals(again[name]), name)
        other, _ = build(tiny_preset_with("seed", 1))
        self.assertFalse(self.tables["product_variants"].equals(other["product_variants"]))


if __name__ == "__main__":
    unittest.main()
