"""Demand sampling stays at product grain before choosing a variant."""

import random
import unittest
from pathlib import Path
from types import SimpleNamespace

from generator.catalog import popularity_weights
from generator.config import load_config
from generator.transactions import select_order_variants


def catalog(weights, variant_counts):
    variants = {
        product: [dict(product_id=product, variant_id=product * 100 + index) for index in range(count)]
        for product, count in enumerate(variant_counts)
    }
    return SimpleNamespace(variants_by_product=variants, product_weights=dict(enumerate(weights)))


class ProductDemandTests(unittest.TestCase):
    def test_rank_mapping_preserves_probability_for_different_catalog_sizes(self):
        counts = (64, 18, 9, 4, 2, 1, 1, 0.5, 0.4, 0.1)
        for size in (1, 7, 10, 23, 60):
            with self.subTest(products=size):
                weights = popularity_weights(size, counts)
                self.assertEqual(len(weights), size)
                self.assertAlmostEqual(sum(weights), 1)
                self.assertTrue(all(weight >= 0 for weight in weights))
        self.assertAlmostEqual(popularity_weights(10, counts)[0], 0.64)

    def test_first_product_demand_is_not_multiplied_by_variant_count(self):
        config = load_config(Path(__file__).resolve().parents[1] / "configs/small.yml")
        weights = popularity_weights(10, config.distributions.product_popularity)
        for variants in ([1] * 10, list(range(1, 11))):
            with self.subTest(variants=variants):
                source = catalog(weights, variants)
                rng = random.Random(20261108)
                selections = [select_order_variants(source, rng, 1)[0]["product_id"] for _ in range(5000)]
                self.assertLess(abs(selections.count(0) / len(selections) - weights[0]), 0.025)

    def test_exhaustion_never_falls_back_to_zero_weight_products(self):
        source = catalog([0, 1], [20, 2])
        first = select_order_variants(source, random.Random(42), 10)
        second = select_order_variants(source, random.Random(42), 10)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 2)
        self.assertEqual(len({row["variant_id"] for row in first}), 2)
        self.assertTrue(all(row["product_id"] == 1 for row in first))
        self.assertEqual(len(source.variants_by_product[1]), 2)
