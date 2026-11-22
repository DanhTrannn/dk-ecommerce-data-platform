import unittest

from datagen.verify.report import check, render


class ReportTests(unittest.TestCase):
    def test_check_passes_within_absolute_and_relative_tolerance(self):
        self.assertTrue(check("outcome.cod_share", 0.6, 0.605).passed)
        self.assertFalse(check("outcome.cod_share", 0.6, 0.62).passed)
        self.assertTrue(check("r01.orders_per_buyer", 3.846, 3.90).passed)
        self.assertFalse(check("r01.orders_per_buyer", 3.846, 4.10).passed)
        self.assertFalse(check("ledger.negative_running_balances", 0, 1).passed)

    def test_render_lists_every_check_with_verdict(self):
        text = render([check("outcome.cod_share", 0.6, 0.62)])
        self.assertIn("outcome.cod_share", text)
        self.assertIn("KHÔNG ĐẠT", text)
