import unittest

import numpy as np

from datagen.distributions import (
    expected_orders_per_buyer,
    log_series_theta,
    lognormal_top_share,
    repeat_gap_mean_days,
    sample_frequency_table,
    single_order_share,
    split_by_shares,
)
from datagen.randomness import ONLINE, stream


class DistributionTests(unittest.TestCase):
    def test_formulas_match_preset_comments(self):
        theta = log_series_theta(0.1141, 1.0)
        self.assertAlmostEqual(expected_orders_per_buyer(theta), 3.846, places=3)
        self.assertAlmostEqual(single_order_share(theta), 0.394, places=3)
        self.assertAlmostEqual(
            float(repeat_gap_mean_days(365, np.array([2]), 0.518)[0]), 365 * (1 - 0.259) / 3, places=6
        )
        self.assertAlmostEqual(lognormal_top_share(1.35, 0.10), 0.527, places=2)

    def test_logseries_sample_mean_matches_formula(self):
        theta = log_series_theta(0.1141, 1.0)
        sample = stream(1, ONLINE).logseries(theta, 400_000)
        self.assertAlmostEqual(sample.mean() / expected_orders_per_buyer(theta), 1.0, delta=0.03)
        self.assertAlmostEqual((sample == 1).mean(), single_order_share(theta), delta=0.005)

    def test_frequency_table_sample(self):
        sample = sample_frequency_table(stream(1, ONLINE), [92.1, 7.0, 1.0], 200_000)
        self.assertEqual(set(np.unique(sample).tolist()), {1, 2, 3})
        self.assertAlmostEqual((sample == 1).mean(), 0.921 / 1.001, delta=0.005)

    def test_split_by_shares_is_exact_and_uses_largest_remainders(self):
        self.assertEqual(split_by_shares(2, [0.4, 0.3, 0.1, 0.07, 0.07, 0.06]).tolist(), [1, 1, 0, 0, 0, 0])
        self.assertEqual(split_by_shares(10, [1, 1, 1]).tolist(), [4, 3, 3])
        self.assertEqual(int(split_by_shares(1234, [0.2, 0.5, 0.3]).sum()), 1234)
        self.assertEqual(split_by_shares(0, [1, 2]).tolist(), [0, 0])


if __name__ == "__main__":
    unittest.main()
