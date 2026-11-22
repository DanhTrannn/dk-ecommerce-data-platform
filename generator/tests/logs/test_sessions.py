import unittest
from datetime import UTC, datetime

import polars as pl

from datagen.logs.sessions import REQUESTS_SCHEMA, buying_requests, non_buying_count, non_buying_requests
from datagen.presets import load_log_preset, log_preset_path
from datagen.randomness import LOG_SESSIONS, stream

PRESET = load_log_preset(log_preset_path("1m"))
ZONE = "Asia/Ho_Chi_Minh"
ORDER_TS = datetime(2025, 11, 28, 13, 5, 12, 345678, tzinfo=UTC)
UTC_MICROS = pl.Datetime("us", "UTC")

PRODUCTS = pl.DataFrame(
    {"product_id": [1, 2, 3], "product_slug": ["ao-thun", "quan-jean", "vay-hoa"], "weight": [0.5, 0.3, 0.2]}
)
VARIANTS = pl.DataFrame(
    {"product_id": [1, 1, 2, 3], "variant_key": ["v-a", "v-b", "v-c", "v-d"]},
)


def rng():
    return stream(7, LOG_SESSIONS, 0)


def orders(count=1, at=ORDER_TS):
    return pl.DataFrame(
        {
            "order_number": [f"ORD-{i + 1}" for i in range(count)],
            "created_at": [at] * count,
            "customer_key": [f"cust-{i + 1}" for i in range(count)],
        },
        schema={"order_number": pl.String, "created_at": UTC_MICROS, "customer_key": pl.String},
    )


def lines(rows):
    return pl.DataFrame(
        rows,
        schema={"order_number": pl.String, "product_id": pl.Int64, "product_slug": pl.String, "variant_key": pl.String},
        orient="row",
    )


TWO_PRODUCTS = lines([("ORD-1", 1, "ao-thun", "v-a"), ("ORD-1", 2, "quan-jean", "v-c")])
SAME_PRODUCT_TWO_VARIANTS = lines([("ORD-1", 1, "ao-thun", "v-a"), ("ORD-1", 1, "ao-thun", "v-b")])


def session_flags(requests):
    return requests.group_by("session_id").agg(
        has_cart=(pl.col("action") == "cart_item_set").any(),
        anonymous=pl.col("actor_key").is_null().all(),
        logged_in=pl.col("actor_key").is_not_null().all(),
        views=(pl.col("action") == "product_detail").sum(),
    )


class BuyingSessionTests(unittest.TestCase):
    def test_buying_session_ends_at_order_with_its_number(self):
        requests = buying_requests(orders(), TWO_PRODUCTS, PRODUCTS, PRESET, rng())
        self.assertEqual(requests.schema, REQUESTS_SCHEMA)
        ordered = requests.sort("ts")
        last = ordered.row(-1, named=True)
        self.assertEqual((last["action"], last["order_number"], last["ts"]), ("checkout_submit", "ORD-1", ORDER_TS))
        actions = ordered["action"].to_list()
        self.assertEqual(actions[0], "catalog_search")
        self.assertEqual(actions[-3:], ["cart_read", "checkout_quote", "checkout_submit"])
        self.assertEqual(requests["actor_key"].unique().to_list(), ["cust-1"])
        self.assertEqual(requests["actor_type"].unique().to_list(), ["customer"])
        self.assertEqual(requests["session_id"].n_unique(), 1)
        self.assertEqual(requests["request_id"].n_unique(), requests.height)
        self.assertEqual(requests["order_number"].drop_nulls().to_list(), ["ORD-1"])

    def test_every_bought_product_is_viewed_once_before_its_cart_add(self):
        requests = buying_requests(orders(), TWO_PRODUCTS, PRODUCTS, PRESET, rng()).sort("ts")
        for slug, variant in (("ao-thun", "v-a"), ("quan-jean", "v-c")):
            viewed = requests.filter((pl.col("action") == "product_detail") & (pl.col("product_key") == slug))["ts"]
            added = requests.filter(pl.col("variant_key") == variant)["ts"]
            self.assertGreaterEqual(viewed.len(), 1)
            self.assertLess(viewed.min(), added.min())

    def test_two_variants_of_one_product(self):
        requests = buying_requests(orders(), SAME_PRODUCT_TWO_VARIANTS, PRODUCTS, PRESET, rng())
        added = requests.filter(pl.col("action") == "cart_item_set")["variant_key"].sort().to_list()
        self.assertEqual(added, ["v-a", "v-b"])
        first = requests.filter(pl.col("product_key") == "ao-thun").sort("ts")["action"][0]
        self.assertEqual(first, "product_detail")

    def test_views_follow_the_geometric_rule_on_average(self):
        count = 4000
        many = orders(count)
        many_lines = lines([(f"ORD-{i + 1}", 1, "ao-thun", "v-a") for i in range(count)])
        views = session_flags(buying_requests(many, many_lines, PRODUCTS, PRESET, rng()))["views"]
        self.assertAlmostEqual(views.mean(), 1 / PRESET.sessions.views_per_session.buying.p, delta=0.5)

    def test_gaps_are_positive_and_capped(self):
        count = 2000
        many = orders(count)
        many_lines = lines([(f"ORD-{i + 1}", 2, "quan-jean", "v-c") for i in range(count)])
        requests = buying_requests(many, many_lines, PRODUCTS, PRESET, rng())
        gaps = (
            requests.sort("session_id", "ts")
            .select(pl.col("ts").diff().over("session_id").dt.total_microseconds())
            .drop_nulls()
            .to_series()
        )
        self.assertTrue((gaps > 0).all())
        self.assertTrue((gaps <= 1800 * 1_000_000).all())

    def test_same_stream_same_requests(self):
        first = buying_requests(orders(), TWO_PRODUCTS, PRODUCTS, PRESET, rng())
        self.assertTrue(first.equals(buying_requests(orders(), TWO_PRODUCTS, PRODUCTS, PRESET, rng())))


class NonBuyingSessionTests(unittest.TestCase):
    CUSTOMERS = pl.Series("customer_key", ["cust-a", "cust-b", "cust-c"])

    def browse(self, count, customers=None):
        return non_buying_requests(
            count, orders(), ZONE, PRODUCTS, VARIANTS, self.CUSTOMERS if customers is None else customers, PRESET, rng()
        )

    def test_shares_match_preset(self):
        requests = self.browse(20_000)
        self.assertEqual(requests.schema, REQUESTS_SCHEMA)
        flags = session_flags(requests)
        self.assertEqual(flags.height, 20_000)
        self.assertAlmostEqual(flags["has_cart"].mean(), PRESET.funnel.non_buying_cart_rate, delta=0.01)
        self.assertTrue((flags["has_cart"] == flags["logged_in"]).all())
        self.assertTrue((flags["has_cart"] != flags["anonymous"]).all())
        theta = PRESET.sessions.views_per_session.non_buying.theta
        expected_views = -theta / ((1 - theta) * __import__("math").log1p(-theta))
        self.assertAlmostEqual(flags["views"].mean(), expected_views, delta=0.3)

    def test_sessions_start_in_the_hour_of_an_order(self):
        requests = self.browse(500)
        starts = requests.group_by("session_id").agg(pl.col("ts").min())["ts"]
        hour = datetime(2025, 11, 28, 13, tzinfo=UTC)
        self.assertTrue(((starts >= hour) & (starts < datetime(2025, 11, 28, 14, tzinfo=UTC))).all())
        self.assertTrue(requests.filter(pl.col("action") != "catalog_search")["order_number"].is_null().all())

    def test_cart_variant_belongs_to_a_viewed_product(self):
        requests = self.browse(3000)
        owner = dict(zip(VARIANTS["variant_key"], VARIANTS["product_id"]))
        slugs = dict(zip(PRODUCTS["product_id"], PRODUCTS["product_slug"]))
        for session in requests.filter(pl.col("action") == "cart_item_set").iter_rows(named=True):
            viewed = requests.filter(
                (pl.col("session_id") == session["session_id"]) & (pl.col("action") == "product_detail")
            ).sort("ts")
            self.assertEqual(slugs[owner[session["variant_key"]]], viewed["product_key"][-1])

    def test_no_active_customer_means_no_cart(self):
        requests = self.browse(500, customers=pl.Series("customer_key", [], dtype=pl.String))
        self.assertTrue(requests["actor_key"].is_null().all())
        self.assertFalse((requests["action"] == "cart_item_set").any())

    def test_count_scales_with_buying_sessions_and_multiplier(self):
        generator = rng()
        draws = [non_buying_count(100, 0.35, PRESET, generator) for _ in range(200)]
        expected = 100 * PRESET.sessions.non_buying_per_buying.value * 0.35
        self.assertTrue(all(int(expected) <= draw <= int(expected) + 1 for draw in draws))
        self.assertAlmostEqual(sum(draws) / len(draws), expected, delta=0.2)
        self.assertEqual(non_buying_count(0, 1.0, PRESET, generator), 0)


if __name__ == "__main__":
    unittest.main()
