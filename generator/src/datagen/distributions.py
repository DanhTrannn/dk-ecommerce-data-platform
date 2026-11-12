"""Closed-form expectations of the preset rules and small sampling helpers.

Generators and the verify command share these formulas, so "expected" means the same thing in both.
"""

import math
from collections.abc import Sequence
from statistics import NormalDist

import numpy as np


def log_series_theta(alpha_years: float, history_years: float) -> float:
    """R01: theta = T / (alpha + T)."""
    return history_years / (alpha_years + history_years)


def expected_orders_per_buyer(theta: float) -> float:
    """Mean of the zero-truncated log-series distribution."""
    return -theta / ((1 - theta) * math.log(1 - theta))


def single_order_share(theta: float) -> float:
    """P(k = 1) of the zero-truncated log-series distribution."""
    return -theta / math.log(1 - theta)


def repeat_gap_mean_days(history_days: float, order_count: np.ndarray, shrink_c: float) -> np.ndarray:
    """R02: mean gap between consecutive orders of a customer with k orders, T(1 - c/k)/(k + 1)."""
    k = np.asarray(order_count, dtype=np.float64)
    return history_days * (1 - shrink_c / k) / (k + 1)


def lognormal_top_share(sigma: float, top_fraction: float) -> float:
    """R03: share of total demand held by the top fraction of products under LogNormal(0, sigma)."""
    normal = NormalDist()
    return 1 - normal.cdf(normal.inv_cdf(1 - top_fraction) - sigma)


def sample_frequency_table(rng: np.random.Generator, percentages: Sequence[float], size: int) -> np.ndarray:
    """Values 1..len(percentages) drawn with the given (not necessarily normalised) weights."""
    weights = np.asarray(percentages, dtype=np.float64)
    return rng.choice(np.arange(1, len(weights) + 1), size=size, p=weights / weights.sum())
