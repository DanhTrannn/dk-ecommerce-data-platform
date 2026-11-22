import json
import unittest

from platform_app.api.core.errors import AppError
from platform_app.api.modules.analytics.reader import PublishedReader


class FakeTrino:
    def __init__(self, publication):
        self.publication, self.queries = publication, []

    def execute_query(self, sql):
        self.queries.append(sql)
        return [self.publication] if "logs_analytics_publications" in sql else [{"total": 1}]


def publication(**changes):
    return dict(
        run_id="logs-run",
        published_at="2026-11-20 03:00:00 UTC",
        source_event_at=None,
        snapshots_json=json.dumps({"lakehouse.gold.mart_hourly_route_metrics": 12}),
        schema_version=1,
        source_mode="logs_rollup",
        observed_at="2026-11-20 03:00:00 UTC",
    ) | changes


class AnalyticsReaderTests(unittest.TestCase):
    def test_reads_only_the_log_publication_and_pins_its_tables(self):
        client = FakeTrino(publication())
        reader = PublishedReader(client)
        reader.query("SELECT count(*) total FROM lakehouse.gold.mart_hourly_route_metrics")
        self.assertNotIn("system.analytics_publications", client.queries[0])
        self.assertIn("FOR VERSION AS OF 12", client.queries[1])

    def test_rejects_a_publication_of_another_contract(self):
        for changes in (dict(schema_version=4), dict(source_mode="batch")):
            with self.subTest(changes=changes), self.assertRaises(AppError):
                PublishedReader(FakeTrino(publication(**changes)))


if __name__ == "__main__":
    unittest.main()
