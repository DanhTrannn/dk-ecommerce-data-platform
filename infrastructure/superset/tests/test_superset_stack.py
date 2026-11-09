"""Opt-in actual publication-pinned sales dashboard acceptance."""

import os
import unittest

from fixture import BIStack


@unittest.skipUnless(os.environ.get("RUN_SUPERSET_STACK_TESTS") == "1", "opt-in private BI stack")
class SupersetStackTests(unittest.TestCase):
    def test_pinned_dashboard_queries_publication_and_retains_last_good(self):
        stack = BIStack()
        try:
            print("BI_REPORT " + str(stack.report), flush=True)
            stack.start_source()
            stack.up("trino")
            stack.initialize()
            stack.initialize()
            stack.up("superset")
            self.assertIsNone(stack.sync(check=False))
            self.assertEqual(stack.inspect("counts"), dict(dashboards=0, charts=0, datasets=0))
            stack.evidence["batch"] = stack.batch("bootstrap")
            first = stack.sync()
            self.assertFalse(first["reused"])
            self.assertEqual(stack.sync()["dashboard_id"], first["dashboard_id"])
            self.assertEqual(stack.inspect("counts"), dict(dashboards=1, charts=8, datasets=2))
            initial = stack.inspect("query")
            self.assertEqual(len(initial[0]["charts"]), 8)
            self.assertTrue(
                all("FOR VERSION AS OF" in query for chart in initial[0]["charts"] for query in chart["queries"])
            )
            expected = stack.runtime(
                "query",
                "--sql",
                "SELECT SUM(total_orders) total_orders, SUM(order_total_vnd) order_total_vnd, SUM(net_revenue_vnd) net_revenue_vnd, SUM(gross_profit_vnd) gross_profit_vnd FROM lakehouse.gold.mart_order_daily",
            )[0]
            for chart, metric in zip(initial[0]["charts"][:4], expected):
                self.assertEqual(int(chart["data"][0][0][metric]), int(expected[metric]))
            stack.evidence["initial_totals"] = expected
            stack.evidence["empty_range"] = stack.inspect("empty-filter")
            stack.evidence["read_only"] = stack.inspect("deny-write")
            stack.evidence["http"] = stack.inspect("http", initial[0]["slug"])
            stack.evidence["initial_dashboard"] = first
            stack.evidence["initial_queries"] = initial
            stack.batch("corrupt")
            retained = stack.inspect("query")
            self.assertEqual(
                [chart["data"] for chart in initial[0]["charts"]], [chart["data"] for chart in retained[0]["charts"]]
            )
            self.assertEqual(stack.sync()["dashboard_id"], first["dashboard_id"])
            stack.evidence["purchases"] = stack.runtime("purchases")
            stack.evidence["incremental_batch"] = stack.batch("restore")
            second = stack.sync()
            self.assertNotEqual(second["dashboard_id"], first["dashboard_id"])
            self.assertEqual(stack.inspect("counts"), dict(dashboards=2, charts=16, datasets=4))
            latest = stack.inspect("query")
            self.assertEqual(
                [chart["data"] for chart in latest[0]["charts"]], [chart["data"] for chart in initial[0]["charts"]]
            )
            next_totals = {
                metric: latest[1]["charts"][index]["data"][0][0][metric] for index, metric in enumerate(expected)
            }
            self.assertEqual(int(next_totals["total_orders"]), int(expected["total_orders"]) + 2)
            self.assertEqual(int(next_totals["net_revenue_vnd"]), int(expected["net_revenue_vnd"]))
            self.assertEqual(int(next_totals["gross_profit_vnd"]), int(expected["gross_profit_vnd"]))
            self.assertEqual(
                int(next_totals["order_total_vnd"]),
                int(expected["order_total_vnd"])
                + sum(order["total_vnd"] for order in stack.evidence["purchases"]["orders"]),
            )
            stack.evidence["next_totals"] = next_totals
            stack.command(["restart", "superset"])
            stack.up("superset")
            self.assertEqual(stack.inspect("counts"), dict(dashboards=2, charts=16, datasets=4))
            stack.evidence["next_dashboard"] = second
            stack.evidence["unpublished_change_ignored"] = True
            stack.evidence["restart_persistence"] = True
            stack.evidence["browser"] = stack.browser(initial[0]["slug"], second["url"])
            stack.evidence["status"] = "passed"
            print("SUPERSET_STACK_OK " + str(stack.report / "report.json"), flush=True)
        finally:
            stack.close()
