import unittest

import polars as pl

from datagen.oltp.fulfilment import cancellation_refunds, return_tables, review_table, shipment_tables
from datagen.oltp.tables import SCHEMAS
from fixtures import tiny_preset, tiny_preset_with
from oltp.helpers import online_orders


def build(preset, customers=12_000):
    built = online_orders(preset, customer_count=customers, buyer_count=customers - 1_000)
    lc, items, ref, rng, ids = built["lifecycle"], built["items"], built["reference"], built["rng"], built["ids"]
    return {
        "built": built,
        "shipments": shipment_tables(lc, rng, ids),
        "returns": return_tables(lc, items, ref, rng, ids),
        "cancel_refunds": cancellation_refunds(lc, rng, ids),
        "reviews": review_table(lc, items, ref, rng, ids),
    }


class FulfilmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.out = build(tiny_preset())
        cls.lc = cls.out["built"]["lifecycle"]
        cls.payments = cls.out["built"]["tables"]["payments"]

    def test_all_tables_conform(self):
        frames = {**self.out["shipments"], **self.out["returns"]}
        frames["product_reviews"] = self.out["reviews"]
        for name, frame in frames.items():
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)
        self.assertEqual(self.out["cancel_refunds"].schema, pl.Schema(SCHEMAS["refunds"]))

    def test_shipments_match_order_outcomes(self):
        ship, att = self.out["shipments"]["shipments"], self.out["shipments"]["shipment_attempts"]
        self.assertEqual(ship.height, self.lc.filter(pl.col("dispatched_at").is_not_null()).height)
        self.assertTrue((ship["cod_collected_vnd"] <= ship["cod_amount_vnd"]).all())
        self.assertTrue(ship["attempt_count"].is_between(1, 3).all())
        self.assertEqual(int(att.select("shipment_id", "attempt_number").is_duplicated().sum()), 0)
        failed = ship.filter(pl.col("status") == "failed")
        self.assertGreater(failed.height, 0)
        per_failed = att.join(failed.select("shipment_id"), on="shipment_id").group_by("shipment_id").len()
        self.assertTrue((per_failed["len"] == 3).all())
        delivered = ship.filter(pl.col("status") == "delivered")
        self.assertTrue((delivered["cod_collected_vnd"] == delivered["cod_amount_vnd"]).all())

    def test_refunds_never_exceed_payment_and_link_correctly(self):
        refunds = pl.concat([self.out["returns"]["refunds"], self.out["cancel_refunds"]])
        joined = refunds.join(self.payments.select("payment_id", pl.col("amount_vnd").alias("paid")), on="payment_id")
        self.assertEqual(joined.height, refunds.height)
        self.assertTrue((joined["amount_vnd"] <= joined["paid"]).all())
        self.assertEqual(int(refunds.filter(pl.col("return_id").is_null())["payment_id"].is_duplicated().sum()), 0)
        self.assertEqual(int(refunds.filter(pl.col("return_id").is_not_null())["return_id"].is_duplicated().sum()), 0)
        self.assertGreater(self.out["cancel_refunds"].height, 0)

    def test_cancellation_refunds_only_for_paid_cancelled_orders(self):
        refunded = self.out["cancel_refunds"].join(self.payments, on="payment_id")
        self.assertTrue((refunded["status_right"] == "succeeded").all())

    def test_exchange_and_refund_ledger_rows(self):
        r = self.out["returns"]
        tx, requests, items = r["inventory_transactions"], r["return_requests"], r["return_items"]
        self.assertGreater(requests.height, 0)
        self.assertEqual(tx["return_id"].null_count(), 0)
        self.assertEqual(set(tx["movement_type"].to_list()), {"return_customer", "exchange_out"})
        rows = (
            tx.group_by("return_id")
            .agg(pl.len(), pl.col("quantity_delta").sum())
            .join(requests.select("return_id", "action_type"), on="return_id")
        )
        self.assertTrue((rows.filter(pl.col("action_type") == "refund")["len"] == 1).all())
        self.assertTrue((rows.filter(pl.col("action_type") == "exchange")["len"] == 2).all())
        self.assertTrue((rows.filter(pl.col("action_type") == "exchange")["quantity_delta"] == 0).all())
        exchanged = items.filter(pl.col("exchange_variant_id").is_not_null())
        self.assertTrue((exchanged["exchange_variant_id"] != exchanged["variant_id"]).all())
        self.assertEqual(requests["return_code"].n_unique(), requests.height)

    def test_zero_return_rate_gives_empty_conformed_tables(self):
        out = build(tiny_preset_with("outcomes.return_rate", 0.0), customers=2_000)
        for name, frame in out["returns"].items():
            self.assertEqual(frame.height, 0, name)
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)

    def test_reviews_one_per_order_with_valid_moderation(self):
        rv = self.out["reviews"]
        self.assertGreater(rv.height, 0)
        self.assertEqual(int(rv["order_item_id"].is_duplicated().sum()), 0)
        self.assertTrue(rv["rating"].is_between(1, 5).all())
        rejected = rv.filter(pl.col("status") == "rejected")
        self.assertEqual(rejected["moderated_by_customer_id"].null_count(), 0)
        self.assertEqual(rejected["moderation_reason"].null_count(), 0)
        approved = rv.filter(pl.col("status") == "approved")
        self.assertEqual(approved["moderation_reason"].null_count(), approved.height)
        self.assertEqual(approved["moderated_at"].null_count(), approved.height)


if __name__ == "__main__":
    unittest.main()
