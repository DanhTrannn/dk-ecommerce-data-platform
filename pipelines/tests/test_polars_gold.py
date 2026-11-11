"""Native financial, empty-data, time-window and publication-gate contracts."""

import importlib.util
import unittest
from datetime import UTC, datetime, timedelta

AVAILABLE = all(importlib.util.find_spec(name) for name in ("polars", "pyiceberg"))


@unittest.skipUnless(AVAILABLE, "install batch-pipeline[polars] for native Gold checks")
class NativeGoldTests(unittest.TestCase):
    def setUp(self):
        import polars as pl
        import pyarrow as pa
        from lakehouse.utils.config import SOURCE_TABLES
        from lakehouse.oltp.polars.schema import silver_schema

        self.pl, self.pa = pl, pa
        self.now = datetime(2026, 11, 10, 12, tzinfo=UTC)
        self.source = {t.name: pl.from_arrow(pa.Table.from_pylist([], schema=silver_schema(t))) for t in SOURCE_TABLES}

    def test_empty_gold_all_27_schemas_and_55_checks(self):
        from lakehouse.oltp.polars.gold.builders import build_all, TABLES, schema
        from lakehouse.oltp.polars.gold.quality import evaluate

        gold = build_all(self.source, "empty", self.now, ingested_at=self.now)
        self.assertEqual(set(gold), set(TABLES))
        for name, frame in gold.items():
            self.assertEqual(frame.columns, schema(name).names)
        self.assertEqual(gold["dim_date"].height, 1095)
        row = gold["mart_inventory_summary"].to_dicts()[0]
        self.assertEqual(row["inventory_rows"], 0)
        self.assertIsNone(row["total_inventory_value_vnd"])
        self.assertIsNone(row["stockout_count"])
        result = evaluate({**self.source, **gold}, "empty")
        self.assertEqual(result["total_checks"], 55)
        self.assertEqual(result["failed_checks"], 0)

    def test_allocation_conserves_large_integer_vnd_and_is_order_stable(self):
        from lakehouse.oltp.polars.gold.sales import allocate

        money = 9007199254740993
        rows = self.pl.DataFrame(
            {"order_id": [1, 1, 1], "order_item_id": [3, 1, 2], "amount": [money] * 3, "weight": [7, 2, 3]}
        )
        result = allocate(rows, "amount", "weight", "allocated")
        self.assertEqual(sum(result["allocated"]), money)
        cumulative = 0
        expected = []
        for weight in [2, 3, 7]:
            expected.append(money * (cumulative + weight) // 12 - money * cumulative // 12)
            cumulative += weight
        self.assertEqual(result["allocated"].to_list(), expected)
        zero = allocate(rows.with_columns(self.pl.lit(0).alias("weight")), "amount", "weight", "allocated")
        self.assertEqual(zero["allocated"].to_list(), [0, 0, 0])

    def test_half_up_rates_do_not_use_bankers_rounding(self):
        from lakehouse.oltp.polars.gold.core import round_money_rate

        actual = round_money_rate(self.pl.DataFrame({"x": [1.125, -1.125]}), "x")
        self.assertEqual(actual["x"].to_list(), [1.13, -1.13])

    def test_discount_refund_and_cost_snapshots(self):
        from lakehouse.oltp.polars.gold.sales import fact_order_item

        frames = dict(self.source)
        frames["orders"] = self.pl.DataFrame(
            {
                "order_id": [1],
                "created_at": [self.now],
                "customer_id": [1],
                "store_id": [None],
                "channel": ["online"],
                "status": ["returned"],
                "discount_amount_vnd": [1],
            },
            schema_overrides={"store_id": self.pl.Int64},
        )
        frames["order_items"] = self.pl.DataFrame(
            {
                "order_item_id": [1, 2],
                "order_id": [1, 1],
                "variant_id": [1, 2],
                "quantity": [1, 1],
                "unit_price_vnd": [10, 20],
                "cost_price_vnd": [4, 5],
                "category_code_snapshot": ["x", "x"],
                "category_name_snapshot": ["X", "X"],
            }
        )
        frames["product_variants"] = self.pl.DataFrame(
            {"variant_id": [1, 2], "product_id": [1, 1], "cost_price_vnd": [999, 999]}
        )
        frames["payments"] = self.pl.DataFrame({"payment_id": [1], "order_id": [1]})
        frames["refunds"] = self.pl.DataFrame({"payment_id": [1], "amount_vnd": [7], "status": ["succeeded"]})
        result = fact_order_item(frames)
        self.assertEqual(result["allocated_discount_vnd"].sum(), 1)
        self.assertEqual(result["gross_revenue_vnd"].sum(), 29)
        self.assertEqual(result["refunded_amount_vnd"].sum(), 7)
        self.assertEqual(result["net_revenue_vnd"].sum(), 22)
        self.assertEqual(result["item_cost_vnd"].sum(), 9)
        frames["orders"] = frames["orders"].with_columns(self.pl.lit("confirmed").alias("status"))
        result = fact_order_item(frames)
        self.assertEqual(result["gross_revenue_vnd"].sum(), 0)
        self.assertEqual(result["item_cost_vnd"].sum(), 0)

    def test_windows_are_inclusive_and_future_events_are_excluded(self):
        from lakehouse.oltp.polars.gold.observations import build_observation

        frames = {**self.source, "clock": self.pl.DataFrame({"observed_at": [self.now]})}
        frames["coupon_redemptions"] = self.pl.DataFrame(
            {
                "coupon_id": [1] * 4,
                "coupon_redemption_id": [1, 2, 3, 4],
                "status": ["redeemed"] * 4,
                "redeemed_at": [
                    self.now - timedelta(minutes=15),
                    self.now,
                    self.now + timedelta(seconds=1),
                    self.now - timedelta(minutes=15, seconds=1),
                ],
            }
        )
        frames["coupons"] = self.pl.DataFrame(
            {
                "coupon_id": [1],
                "code_normalized": ["X"],
                "used_count": [0],
                "total_usage_limit": [None],
                "is_active": [True],
                "archived_at": [None],
                "starts_at": [self.now - timedelta(days=1)],
                "ends_at": [self.now],
            },
            schema_overrides={"total_usage_limit": self.pl.Int64, "archived_at": self.pl.Datetime("us", "UTC")},
        )
        row = build_observation("mart_coupon_usage", frames).to_dicts()[0]
        self.assertEqual(row["recent_redemptions_count"], 2)
        self.assertTrue(row["is_available"])
        self.assertIsNone(row["usage_limit"])
        frames["product_reviews"] = self.pl.DataFrame(
            {
                "product_id": [1] * 3,
                "review_id": [1, 2, 3],
                "status": ["approved"] * 3,
                "rating": [1, 2, 1],
                "created_at": [self.now - timedelta(minutes=45), self.now, self.now + timedelta(seconds=1)],
            }
        )
        frames["dim_product"] = self.pl.DataFrame({"product_id": [1], "product_name": ["X"]})
        row = build_observation("mart_product_review_summary", frames).to_dicts()[0]
        self.assertEqual(row["recent_negative_count"], 2)

    def test_gate_rejects_corrupted_mart_and_plain_pii(self):
        from lakehouse.oltp.polars.gold.builders import build_all
        from lakehouse.oltp.polars.gold.quality import evaluate

        gold = build_all(self.source, "run", self.now, ingested_at=self.now)
        gold["mart_inventory_summary"] = gold["mart_inventory_summary"].with_columns(
            self.pl.lit(123).alias("inventory_rows")
        )
        result = evaluate({**self.source, **gold}, "run")
        from lakehouse.oltp.polars.gold.builders import schema

        row = {field.name: None for field in schema("dim_customer")}
        row.update(customer_key=1, customer_id=1, email="plain@example.test", full_name="Raw name")
        gold["dim_customer"] = self.pl.from_arrow(self.pa.Table.from_pylist([row], schema=schema("dim_customer")))
        result = evaluate({**self.source, **gold}, "run")
        self.assertEqual(result["status"], "FAILED")
        self.assertIn("inventory_summary_parity", [r["check_name"] for r in result["results"] if not r["passed"]])

    def test_inventory_history_does_not_inflate_current_stock(self):
        from lakehouse.utils.config import SOURCE_TABLES
        from lakehouse.oltp.polars.schema import silver_schema
        from lakehouse.oltp.polars.gold.builders import build_all
        from lakehouse.oltp.polars.gold.quality import evaluate

        def frame(name, row):
            spec = next(t for t in SOURCE_TABLES if t.name == name)
            return self.pl.from_arrow(self.pa.Table.from_pylist([row], schema=silver_schema(spec)))

        source = dict(self.source)
        source["categories"] = frame("categories", dict(category_id=1, code="x", name="X"))
        source["products"] = frame(
            "products", dict(product_id=1, category_id=1, name="X", slug="x", is_active=True, created_at=self.now)
        )
        source["product_variants"] = frame(
            "product_variants",
            dict(
                variant_id=1,
                product_id=1,
                sku="x",
                color_code="R",
                size_code="M",
                price_vnd=100,
                cost_price_vnd=40,
                is_active=True,
            ),
        )
        source["inventory"] = frame(
            "inventory", dict(variant_id=1, on_hand=10, opening_on_hand=10, updated_at=self.now)
        )
        first = build_all(source, "one", self.now, ingested_at=self.now)
        source["product_variants"] = source["product_variants"].with_columns(self.pl.lit(70).alias("cost_price_vnd"))
        later = self.now + timedelta(days=1)
        second = build_all(
            source, "two", later, ingested_at=later, inventory_history=first["fact_inventory_daily_snapshot"]
        )
        self.assertEqual(second["fact_inventory_daily_snapshot"].height, 2)
        self.assertEqual(second["fact_inventory_current"].height, 1)
        self.assertEqual(second["mart_inventory_summary"]["total_inventory_value_vnd"][0], 700)
        self.assertEqual(evaluate({**source, **second}, "two")["failed_checks"], 0)

    def test_publication_preserves_the_audited_snapshot_ids(self):
        import json
        from lakehouse.utils.config import SOURCE_TABLES
        from lakehouse.oltp.polars.gold.builders import TABLES
        from lakehouse.oltp.polars.gold.publication import publish, PUBLICATION_SCHEMA

        prefix = "polars_batch_test"
        snapshots = {f"lakehouse.{prefix}_silver.{t.silver_table}": 12 for t in SOURCE_TABLES}
        snapshots.update({f"lakehouse.{prefix}_gold.{name}": None for name in TABLES})

        class Output:
            row = None

            def schema(inner):
                return type("Schema", (), {"as_arrow": lambda inner: PUBLICATION_SCHEMA})()

            def upsert(inner, data, **kwargs):
                inner.row = data.to_pylist()[0]

        output = Output()

        class Catalog:
            def load_table(inner, name):
                return type("Table", (), {"snapshot_by_id": lambda inner, value: object() if value == 12 else None})()

            def create_table_if_not_exists(inner, *args, **kwargs):
                return output

        row = publish(Catalog(), prefix, "run", snapshots, {"status": "PASSED", "failed_checks": 0, "total_checks": 55})
        self.assertEqual(json.loads(row["snapshots_json"]), snapshots)
        self.assertEqual(row["schema_version"], 3)
        self.assertEqual(json.loads(output.row["snapshots_json"]), snapshots)
        wrong = {f"lakehouse.{prefix}_gold.wrong_{n}": None for n in range(54)}
        with self.assertRaises(ValueError):
            publish(None, prefix, "run", wrong, {"status": "PASSED", "failed_checks": 0, "total_checks": 55})

    def test_publication_refuses_incomplete_or_failed_gate(self):
        from lakehouse.oltp.polars.gold.publication import publish

        for summary in (
            {"status": "FAILED", "failed_checks": 1, "total_checks": 55},
            {"status": "PASSED", "failed_checks": 0, "total_checks": 54},
        ):
            with self.assertRaises(ValueError):
                publish(None, "polars_batch_local", "run", {}, summary)
