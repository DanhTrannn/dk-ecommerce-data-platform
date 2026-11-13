"""Verify results: one tolerance table for every check, and a plain-text report."""

from dataclasses import dataclass

# name -> (tolerance, relative); relative tolerances are a fraction of the expected value.
TOLERANCES: dict[str, tuple[float, bool]] = {
    "ledger.central_balance_mismatches": (0, False),
    "ledger.store_balance_mismatches": (0, False),
    "ledger.negative_running_balances": (0, False),
    "ledger.rows_without_required_document": (0, False),
    "r01.orders_per_buyer": (0.03, True),
    "r01.single_order_share": (0.01, False),
    "r02.mean_gap_days_two_orders": (0.15, True),
    "r03.top_decile_unit_share": (0.08, False),
    "r04.products_per_order": (0.03, True),
    "r05.sale_day_ratio": (0.12, True),
    "outcome.cod_share": (0.01, False),
    "outcome.cancellation_rate": (0.005, False),
    "outcome.cod_failure_rate": (0.01, False),
    "outcome.return_rate": (0.01, False),
    "outcome.exchange_share": (0.03, False),
    "coupon.usage_rate": (0.02, False),
    "r06.review_write_rate": (0.02, False),
    "r06.on_time_five_star_share": (0.03, False),
    "pos.orders_per_online_order": (0.005, False),
    "pos.walk_in_rate": (0.02, False),
    "pos.cash_share": (0.02, False),
    "location.largest_city_customer_share": (0.02, False),
}


@dataclass(frozen=True)
class Check:
    name: str
    expected: float
    measured: float
    tolerance: float
    relative: bool

    @property
    def passed(self) -> bool:
        allowed = self.tolerance * abs(self.expected) if self.relative else self.tolerance
        return abs(self.measured - self.expected) <= allowed


def check(name: str, expected: float, measured: float) -> Check:
    tolerance, relative = TOLERANCES[name]
    return Check(name, expected, measured, tolerance, relative)


def render(checks: list[Check]) -> str:
    lines = [f"{'Kiểm tra':<40}{'Mong đợi':>12}{'Đo được':>12}{'Sai số':>10}  Kết quả"]
    for c in checks:
        tolerance = f"{c.tolerance:.0%}" if c.relative else f"{c.tolerance:g}"
        verdict = "ĐẠT" if c.passed else "KHÔNG ĐẠT"
        lines.append(f"{c.name:<40}{c.expected:>12.4f}{c.measured:>12.4f}{tolerance:>10}  {verdict}")
    return "\n".join(lines)
