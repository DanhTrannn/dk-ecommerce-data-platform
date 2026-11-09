"""Sales publication contracts without Superset, a database or external network."""

import json
import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sales import ORDER_TABLE, PRODUCT_TABLE, Publication, chart_specs


class SalesPublicationTests(unittest.TestCase):
    def row(self, **overrides):
        return dict(
            run_id="batch'quoted",
            published_at="2026-11-09 04:00:00",
            schema_version=3,
            source_mode="batch",
            snapshots_json=json.dumps({ORDER_TABLE: 123, PRODUCT_TABLE: 456}),
            **overrides,
        )

    def test_every_dataset_uses_the_same_publication_and_its_own_snapshot(self):
        publication = Publication.parse(self.row())
        self.assertIn("FOR VERSION AS OF 123", publication.dataset_sql("orders"))
        self.assertIn("FOR VERSION AS OF 456", publication.dataset_sql("products"))
        for name in ["orders", "products"]:
            self.assertIn("'batch''quoted' AS publication_run_id", publication.dataset_sql(name))
            self.assertNotIn("JOIN", publication.dataset_sql(name))

    def test_empty_published_table_stays_empty_even_when_current_table_has_rows(self):
        row = self.row()
        row["snapshots_json"] = json.dumps({ORDER_TABLE: None, PRODUCT_TABLE: 456})
        sql = Publication.parse(row).dataset_sql("orders")
        self.assertIn("WHERE false", sql)
        self.assertNotIn("COALESCE", sql)

    def test_malformed_or_incompatible_publications_are_rejected(self):
        for changes in [
            dict(schema_version=1),
            dict(source_mode="cdc"),
            dict(run_id=""),
            dict(snapshots_json="{}"),
            dict(snapshots_json="[]"),
            dict(snapshots_json=json.dumps({ORDER_TABLE: True, PRODUCT_TABLE: 456})),
            dict(snapshots_json=json.dumps({ORDER_TABLE: "123; DROP TABLE x", PRODUCT_TABLE: 456})),
            dict(published_at="unknown"),
        ]:
            with self.subTest(changes=changes):
                row = self.row()
                row.update(changes)
                with self.assertRaises((ValueError, KeyError)):
                    Publication.parse(row)

    def test_idempotent_fingerprint_changes_only_with_run_or_snapshot_manifest(self):
        first = self.row()
        second = {**first, "published_at": "2026-11-09 05:00:00"}
        self.assertEqual(Publication.parse(first).fingerprint, Publication.parse(second).fingerprint)
        second["snapshots_json"] = json.dumps({ORDER_TABLE: 124, PRODUCT_TABLE: 456})
        self.assertNotEqual(Publication.parse(first).fingerprint, Publication.parse(second).fingerprint)

    def test_unreadable_snapshot_stops_before_superset_metadata_initialization(self):
        import sync_dashboard

        connection = MagicMock()
        cursor = connection.cursor.return_value
        row = self.row()
        columns = list(row)
        cursor.description = [(column,) for column in columns]
        cursor.fetchall.side_effect = [[[row[column] for column in columns]]]
        cursor.execute.side_effect = [None, RuntimeError("snapshot expired")]
        with patch.object(sync_dashboard, "trino_connection", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "snapshot expired"):
                sync_dashboard.sync()
        connection.close.assert_called_once()

    def test_chart_metrics_do_not_sum_category_level_order_counts_or_join_grains(self):
        specs = chart_specs()
        self.assertEqual(len(specs), 8)
        self.assertEqual(len([s for s in specs if s["viz"] == "big_number_total"]), 4)
        self.assertTrue(all(s["dataset"] == "orders" for s in specs if s.get("metric") == "total_orders"))
        self.assertEqual([s["dataset"] for s in specs if s["key"] == "product"], ["products"])
