import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard import chart_payload, layout
from sales import COLUMNS, DATE_COLUMNS, METRICS, TABLES, chart_specs, dataset_sql, fingerprint


class SalesDatasetTests(unittest.TestCase):
    def test_datasets_read_current_gold_tables_without_snapshot_pins(self):
        for name, table in TABLES.items():
            sql = dataset_sql(name)
            self.assertTrue(table.startswith("lakehouse.gold.mart_"))
            self.assertTrue(sql.endswith("FROM " + table))
            self.assertNotIn("FOR VERSION AS OF", sql)
            self.assertNotIn("JOIN", sql)
            self.assertIn(DATE_COLUMNS[name], COLUMNS[name])

    def test_online_orders_get_a_store_label(self):
        self.assertIn("coalesce(store_name, 'Online') AS store_name", dataset_sql("sales"))

    def test_each_chart_uses_its_dataset_metrics_and_date_column(self):
        specs = chart_specs()
        self.assertEqual(len(specs), 8)
        for spec in specs:
            for metric in [spec.get("metric")] if "metric" in spec else spec["metrics"]:
                self.assertIn(metric, COLUMNS[spec["dataset"]], spec["key"])
                self.assertIn(metric, METRICS)
            params, context = chart_payload(spec, 7)
            self.assertEqual(params["granularity_sqla"], DATE_COLUMNS[spec["dataset"]])
        self.assertEqual({s["dataset"] for s in specs if s["key"] in ("orders", "order_value")}, {"orders"})
        self.assertEqual({s["dataset"] for s in specs if s["key"] in ("net_revenue", "gross_profit")}, {"sales"})

    def test_fingerprint_follows_the_asset_definitions(self):
        first = fingerprint()
        self.assertEqual(first, fingerprint())
        with patch.dict(TABLES, {"orders": "lakehouse.gold.mart_other"}):
            self.assertNotEqual(first, fingerprint())

    def test_layout_places_all_charts(self):
        charts = [MagicMock(id=i, slice_name=f"c{i}", uuid=i) for i in range(8)]
        position = layout(charts)
        self.assertEqual(sum(key.startswith("CHART-") for key in position), 8)

    def test_unreadable_mart_stops_before_superset_metadata_initialization(self):
        import sync_dashboard

        connection = MagicMock()
        connection.cursor.return_value.execute.side_effect = RuntimeError("table missing")
        with patch.object(sync_dashboard, "trino_connection", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "table missing"):
                sync_dashboard.sync()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
