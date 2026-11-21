import json
import unittest
from datetime import UTC, datetime

import polars as pl

from datagen.logs.events import ROUTES, to_payloads
from datagen.logs.sessions import REQUESTS_SCHEMA

TS = datetime(2025, 11, 28, 13, 5, 12, 345678, tzinfo=UTC)


def requests(rows):
    return pl.DataFrame(rows, schema=REQUESTS_SCHEMA, orient="row")


SAMPLE = requests(
    [
        ("s-1", "r-1", TS, "catalog_search", "anonymous", None, None, None, None),
        ("s-1", "r-2", TS, "product_detail", "customer", "cust-1", "ao-thun", None, None),
        ("s-1", "r-3", TS, "cart_item_set", "customer", "cust-1", None, "v-a", None),
        ("s-1", "r-4", TS, "cart_read", "customer", "cust-1", None, None, None),
        ("s-1", "r-5", TS, "checkout_quote", "customer", "cust-1", None, None, None),
        ("s-1", "r-6", TS, "checkout_submit", "customer", "cust-1", None, None, "ORD-1"),
    ]
)


def decoded(frame):
    return [json.loads(text) for text in frame["raw_payload"]]


class PayloadTests(unittest.TestCase):
    def test_payload_matches_contract_1_1_0(self):
        payloads = to_payloads(SAMPLE.lazy()).collect()
        self.assertEqual(payloads.schema, pl.Schema({"raw_payload": pl.String, "event_at": pl.Datetime("us", "UTC")}))
        submit = decoded(payloads)[-1]
        self.assertEqual(submit["schema"], {"name": "ecommerce.access", "version": "1.1.0"})
        self.assertEqual(submit["timestamp"], "2025-11-28T13:05:12.345678Z")
        self.assertEqual(submit["timestamp"], submit["observed_timestamp"])
        self.assertEqual((submit["severity_text"], submit["severity_number"]), ("INFO", 9))
        self.assertEqual(submit["request"], {"id": "r-6"})
        self.assertEqual(submit["session"], {"id": "s-1"})
        self.assertIsNone(submit["trace_id"])
        self.assertIsNone(submit["span_id"])
        self.assertEqual(
            submit["service"],
            {"name": "ecommerce-api", "version": "0.1.0", "environment": "local", "instance_id": "datagen"},
        )
        self.assertEqual(
            submit["event"],
            {
                "name": "http.server.request",
                "category": "web",
                "kind": "event",
                "outcome": "success",
                "duration_ns": 0,
            },
        )
        self.assertEqual(submit["http"], {"request_method": "POST", "route": "/api/v1/checkout", "status_code": 200})
        self.assertEqual(submit["actor"], {"type": "customer", "key": "cust-1"})
        self.assertEqual(submit["client"], {})
        self.assertEqual(submit["ecommerce"], {"action": "checkout_submit", "order_number": "ORD-1"})
        self.assertEqual(submit["error"], {"code": None, "type": None})
        self.assertEqual(submit["data_origin"], "synthetic")
        self.assertEqual(payloads["event_at"].to_list(), [TS] * 6)

    def test_ecommerce_keys_depend_on_the_action(self):
        by_action = {p["ecommerce"]["action"]: p for p in decoded(to_payloads(SAMPLE.lazy()).collect())}
        self.assertEqual(by_action["catalog_search"]["ecommerce"], {"action": "catalog_search"})
        self.assertEqual(
            by_action["product_detail"]["ecommerce"], {"action": "product_detail", "product_key": "ao-thun"}
        )
        self.assertEqual(by_action["cart_item_set"]["ecommerce"], {"action": "cart_item_set", "variant_key": "v-a"})
        self.assertEqual(by_action["cart_read"]["ecommerce"], {"action": "cart_read"})
        self.assertEqual(by_action["catalog_search"]["actor"], {"type": "anonymous", "key": None})

    def test_routes_follow_the_api_action_table(self):
        payloads = decoded(to_payloads(SAMPLE.lazy()).collect())
        self.assertEqual(
            {p["ecommerce"]["action"]: (p["http"]["request_method"], p["http"]["route"]) for p in payloads},
            {action: ROUTES[action] for action in SAMPLE["action"]},
        )
        self.assertEqual(ROUTES["product_detail"], ("GET", "/api/v1/products/{slug}"))
        self.assertEqual(ROUTES["cart_item_set"], ("PUT", "/api/v1/cart/items/{variant_public_id}"))
        self.assertEqual(ROUTES["checkout_quote"], ("POST", "/api/v1/checkout/quote"))


class PayloadOrderTests(unittest.TestCase):
    def test_rows_keep_the_order_of_the_requests(self):
        shuffled = SAMPLE.reverse()
        actions = [p["ecommerce"]["action"] for p in decoded(to_payloads(shuffled.lazy()).collect())]
        self.assertEqual(actions, shuffled["action"].to_list())

    def test_product_key_with_null_text_is_kept(self):
        tricky = SAMPLE.with_columns(product_key=pl.when(pl.col("action") == "product_detail").then(pl.lit("null")))
        by_action = {p["ecommerce"]["action"]: p for p in decoded(to_payloads(tricky.lazy()).collect())}
        self.assertEqual(by_action["product_detail"]["ecommerce"], {"action": "product_detail", "product_key": "null"})


if __name__ == "__main__":
    unittest.main()
