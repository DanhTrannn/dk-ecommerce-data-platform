import unittest

import numpy as np
import polars as pl

from datagen.oltp.fulfilment import return_tables
from datagen.oltp.inventory import demand_of, inventory_tables
from datagen.oltp.order_tables import order_tables
from datagen.oltp.pos import draw_pos_lifecycle
from datagen.oltp.tables import SCHEMAS
from datagen.randomness import INVENTORY, POS, stream
from fixtures import tiny_preset
from oltp.helpers import online_orders


class InventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = p = tiny_preset()
        b = online_orders(p, customer_count=3_000, buyer_count=2_700)
        cls.ref, timeline, ids = b["reference"], b["timeline"], b["ids"]
        returns = return_tables(b["lifecycle"], b["items"], cls.ref, b["rng"], ids)
        store_id = int(cls.ref.stores["store_id"][0])
        pos_lc, pos_items = draw_pos_lifecycle(
            p, timeline, cls.ref, stream(1, POS), store_id, 202511, 300, np.array([1000])
        )
        pos_tables, _, _ = order_tables(p, pos_lc, pos_items, cls.ref, stream(2, POS), ids)
        cls.batch_ledger = pl.concat(
            [
                b["tables"]["inventory_transactions"],
                returns["inventory_transactions"],
                pos_tables["inventory_transactions"],
            ]
        )
        demand = demand_of(p, cls.batch_ledger)
        cls.out = inventory_tables(p, timeline, cls.ref, stream(p.seed, INVENTORY), ids, demand)
        cls.ledger = pl.concat([cls.batch_ledger, cls.out["inventory_transactions"]])

    def test_tables_conform(self):
        for name, frame in self.out.items():
            self.assertEqual(frame.schema, pl.Schema(SCHEMAS[name]), name)

    def test_balances_equal_ledger_sum(self):
        central = (
            self.ledger.filter(pl.col("store_id").is_null()).group_by("variant_id").agg(pl.col("quantity_delta").sum())
        )
        joined = self.out["inventory"].join(central, on="variant_id")
        self.assertEqual(joined.height, self.out["inventory"].height)
        self.assertTrue((joined["on_hand"] == joined["quantity_delta"]).all())
        store = (
            self.ledger.filter(pl.col("store_id").is_not_null())
            .group_by("store_id", "variant_id")
            .agg(pl.col("quantity_delta").sum())
        )
        joined = self.out["store_inventory"].join(store, on=["store_id", "variant_id"])
        self.assertEqual(joined.height, self.out["store_inventory"].height)
        self.assertTrue((joined["on_hand"] == joined["quantity_delta"]).all())
        self.assertTrue((self.out["inventory"]["opening_on_hand"] == 0).all())

    def test_running_balance_is_never_negative(self):
        running = self.ledger.sort("created_at", "transaction_id").with_columns(
            balance=pl.col("quantity_delta")
            .cum_sum()
            .over("variant_id", "store_id", order_by=["created_at", "transaction_id"])
        )
        self.assertGreaterEqual(running["balance"].min(), 0)

    def test_every_sellable_variant_has_a_central_balance_with_buffer(self):
        self.assertEqual(self.out["inventory"].height, self.ref.variants.height)
        self.assertGreaterEqual(self.out["inventory"]["on_hand"].min(), 80)

    def test_receipts_are_internally_consistent(self):
        items, receipts = self.out["inbound_receipt_items"], self.out["inbound_receipts"]
        self.assertTrue((items["total_cost_vnd"] == items["quantity"] * items["unit_cost_vnd"]).all())
        sums = items.group_by("receipt_id").agg(
            pl.col("quantity").sum().alias("units"), pl.col("total_cost_vnd").sum().alias("cost")
        )
        joined = receipts.join(sums, on="receipt_id")
        self.assertEqual(joined.height, receipts.height)
        self.assertTrue((joined["total_items_count"] == joined["units"]).all())
        self.assertTrue((joined["total_cost_vnd"] == joined["cost"]).all())
        inbound = self.out["inventory_transactions"].filter(pl.col("movement_type") == "inbound")
        self.assertEqual(inbound["receipt_id"].null_count(), 0)
        self.assertEqual(inbound.height, items.height)
        self.assertEqual(receipts["receipt_code"].n_unique(), receipts.height)

    def test_transfers_come_in_matching_pairs(self):
        tr = self.out["inventory_transactions"].filter(pl.col("movement_type").str.starts_with("transfer"))
        self.assertGreater(tr.height, 0)
        pairs = tr.group_by("reference_code", "variant_id").agg(pl.col("quantity_delta").sum())
        self.assertEqual(pairs["quantity_delta"].abs().max(), 0)
        self.assertEqual(tr["order_id"].null_count(), tr.height)

    def test_stock_arrives_before_the_month_starts(self):
        inbound = self.out["inventory_transactions"].filter(pl.col("movement_type") == "inbound")
        local = inbound["created_at"].dt.convert_time_zone("Asia/Ho_Chi_Minh")
        self.assertTrue((local.dt.day() >= 28).all())

    def test_inventory_stream_is_reproducible(self):
        again = inventory_tables(
            self.preset,
            online_orders(self.preset, 10, 9)["timeline"],
            self.ref,
            stream(self.preset.seed, INVENTORY),
            online_orders(self.preset, 10, 9)["ids"],
            demand_of(self.preset, self.batch_ledger),
        )
        self.assertTrue(
            again["inventory"]
            .select("variant_id", "on_hand")
            .equals(self.out["inventory"].select("variant_id", "on_hand"))
        )


if __name__ == "__main__":
    unittest.main()
