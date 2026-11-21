import unittest
from datetime import timedelta

import polars as pl

from datagen.oltp.lifecycle import customer_cod_stats
from datagen.oltp.tables import SCHEMAS
from fixtures import tiny_preset, tiny_preset_with
from oltp.helpers import online_orders

ALLOWED = {
    (None, "pending_payment"),
    (None, "confirmed"),
    ("pending_payment", "paid"),
    ("paid", "confirmed"),
    ("paid", "cancelled"),
    ("confirmed", "cancelled"),
    ("confirmed", "shipping"),
    ("shipping", "delivered"),
    ("shipping", "failed_delivery"),
    ("delivered", "completed"),
    ("completed", "returned"),
}


class LifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset()
        cls.built = online_orders(cls.preset, customer_count=12_000, buyer_count=11_000)
        cls.tables = cls.built["tables"]
        cls.lifecycle = cls.built["lifecycle"]

    def test_tables_conform(self):
        for name, frame in self.tables.items():
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)
        self.assertEqual(
            set(self.tables),
            {
                "orders",
                "order_items",
                "payments",
                "order_status_history",
                "coupon_redemptions",
                "inventory_transactions",
            },
        )

    def test_status_history_follows_allowed_transitions_and_is_unique(self):
        h = self.tables["order_status_history"].sort("order_id", "transitioned_at", "order_status_history_id")
        self.assertTrue(set(zip(h["from_status"].to_list(), h["to_status"].to_list())) <= ALLOWED)
        self.assertEqual(int(h.select("order_id", "to_status").is_duplicated().sum()), 0)
        last = h.group_by("order_id", maintain_order=True).last().join(self.tables["orders"], on="order_id")
        self.assertTrue((last["to_status"] == last["status"]).all())
        self.assertEqual(h["transition_source"].unique().to_list(), ["generator"])
        self.assertEqual(h.filter(pl.col("to_status") == "cancelled")["reason"].null_count(), 0)

    def test_no_event_after_anchor(self):
        for name, frame in self.tables.items():
            for column, dtype in frame.schema.items():
                if isinstance(dtype, pl.Datetime):
                    self.assertTrue((frame[column].drop_nulls() <= self.preset.anchor_time).all(), f"{name}.{column}")

    def test_recent_orders_are_still_in_progress(self):
        # The last whole local day ends 7 hours before the anchor (00:00 UTC is 07:00 in Vietnam).
        last_day_end = self.preset.anchor_time - timedelta(hours=7)
        recent = self.tables["orders"].filter(pl.col("created_at") > last_day_end - timedelta(hours=2))
        self.assertGreater(recent.height, 0)
        self.assertTrue(
            set(recent["status"].to_list()) <= {"pending_payment", "paid", "confirmed", "cancelled", "shipping"}
        )

    def test_payment_and_cod_constraints(self):
        o = self.tables["orders"]
        cod_open = o.filter((pl.col("payment_method") == "cod") & pl.col("status").is_in(["confirmed", "shipping"]))
        self.assertEqual(cod_open["paid_at"].null_count(), cod_open.height)
        self.assertTrue(set(o["payment_method"].to_list()) <= {"cod", "vietqr"})
        pay = self.tables["payments"].join(o, on="order_id")
        self.assertEqual(pay["order_id"].n_unique(), pay.height)
        self.assertTrue((pay["amount_vnd"] == pay["total_vnd"]).all())
        self.assertTrue(((pay["status"] == "succeeded") == pay["paid_at"].is_not_null()).all())
        self.assertEqual(o["shipping_city_id"].null_count(), 0)
        self.assertEqual(o["order_number"].n_unique(), o.height)

    def test_coupon_snapshots_and_redemptions(self):
        o = self.tables["orders"]
        with_coupon = o.filter(pl.col("coupon_id").is_not_null())
        self.assertEqual(with_coupon["coupon_code_snapshot"].null_count(), 0)
        self.assertEqual(with_coupon["coupon_type_snapshot"].unique().to_list(), ["percentage"])
        r = self.tables["coupon_redemptions"].join(o, on="order_id")
        self.assertEqual(r.height, with_coupon.height)
        self.assertTrue(((r["status"] == "released") == (r["status_right"] == "cancelled")).all())

    def test_ledger_rows_per_outcome(self):
        tx = self.tables["inventory_transactions"]
        self.assertEqual(tx["order_id"].null_count(), 0)
        self.assertEqual(tx["store_id"].null_count(), tx.height)
        net = tx.group_by("order_id").agg(pl.col("quantity_delta").sum()).join(self.tables["orders"], on="order_id")
        self.assertTrue(
            (net.filter(pl.col("status").is_in(["cancelled", "failed_delivery"]))["quantity_delta"] == 0).all()
        )
        self.assertTrue((net.filter(pl.col("status") == "completed")["quantity_delta"] < 0).all())

    def test_outcome_rates(self):
        old = self.lifecycle.filter(pl.col("created_at") < self.preset.anchor_time - timedelta(days=45))
        self.assertAlmostEqual(float((old["payment_method"] == "cod").mean()), 0.6, delta=0.02)
        self.assertAlmostEqual(float(old["is_cancelled"].mean()), 0.08, delta=0.01)
        delivered = old.filter(pl.col("delivered_at").is_not_null())
        self.assertAlmostEqual(float(delivered["return_action"].is_not_null().mean()), 0.06, delta=0.01)
        self.assertAlmostEqual(float(delivered["review_at"].is_not_null().mean()), 0.35, delta=0.02)

    def test_customer_cod_stats(self):
        stats = customer_cod_stats(self.preset, self.lifecycle)
        failures = self.lifecycle.filter(pl.col("failed_at").is_not_null()).group_by("customer_id").len()
        joined = stats.join(failures, on="customer_id")
        self.assertTrue((joined["boom_count"] == joined["len"]).all())
        self.assertTrue(((stats["boom_count"] >= 3) == stats["is_cod_blocked"]).all())


class DeliveryTimeTests(unittest.TestCase):
    """R07 with no late orders: pickup the dispatch day before 10h30, else the next day; then the zone's range."""

    @classmethod
    def setUpClass(cls):
        cls.preset = tiny_preset_with("fulfilment.late_rate", 0.0)
        built = online_orders(cls.preset, customer_count=4_000, buyer_count=4_000)
        zone = cls.preset.business_timezone
        codes = built["reference"].cities.select("city_id", "code")
        cls.delivered = (
            built["lifecycle"]
            .filter(pl.col("delivered_at").is_not_null())
            .join(codes, on="city_id", how="left")
            .with_columns(
                dispatched=pl.col("dispatched_at").dt.convert_time_zone(zone),
                delivered=pl.col("delivered_at").dt.convert_time_zone(zone),
            )
            .with_columns(
                pickup_day=pl.col("dispatched").dt.date()
                + pl.duration(days=(pl.col("dispatched").dt.hour().cast(pl.Int64) * 60 + pl.col("dispatched").dt.minute() >= 630).cast(pl.Int64)),
            )
            .with_columns(days=(pl.col("delivered").dt.date() - pl.col("pickup_day")).dt.total_days())
        )

    def test_days_after_pickup_follow_the_city_zone(self):
        for code, (low, high) in {"HCM": (1, 2), "BD": (2, 4), "CT": (2, 4), "HN": (3, 4), "DN": (4, 6), "HP": (4, 6)}.items():
            days = self.delivered.filter(pl.col("code") == code)["days"]
            self.assertGreater(days.len(), 0, code)
            self.assertEqual((days.min(), days.max()), (low, high), code)

    def test_delivery_happens_in_local_working_hours(self):
        hours = self.delivered["delivered"].dt.hour()
        self.assertEqual((hours.min(), hours.max()), (8, 20))


class CodBlockTests(unittest.TestCase):
    def test_cod_block_stops_further_cod_orders(self):
        preset = tiny_preset_with("outcomes.cod_failure_rate", 1.0)
        preset = preset.model_copy(
            update={"outcomes": preset.outcomes.model_copy(update={"cod_rate": 1.0, "cancellation_rate": 0.0})}
        )
        lifecycle = online_orders(preset, customer_count=2_000, buyer_count=2_000)["lifecycle"]
        per_customer = lifecycle.group_by("customer_id").agg(pl.col("is_failed_delivery").sum(), pl.len())
        self.assertTrue((per_customer["is_failed_delivery"] <= 3).all())
        heavy = per_customer.filter(pl.col("len") > 3)
        self.assertGreater(heavy.height, 0)
        self.assertTrue((heavy["is_failed_delivery"] == 3).all())


if __name__ == "__main__":
    unittest.main()
