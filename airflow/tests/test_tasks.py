import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tasks import oltp

RUN_AFTER = datetime(2026, 11, 21, 3, tzinfo=UTC)
START, END = datetime(2026, 11, 20, tzinfo=UTC), datetime(2026, 11, 21, tzinfo=UTC)
DAG_RUN = SimpleNamespace(run_after=RUN_AFTER)


class TaskTests(unittest.TestCase):
    def call(self, name, task, **kwargs):
        fake = MagicMock(return_value=0)
        with (
            patch.object(oltp, name, fake),
            patch.object(oltp, "iceberg_catalog"),
            patch.object(oltp, "PostgresHook") as hook,
            patch.object(oltp, "source_read_transaction"),
            patch.object(oltp, "source_reconciliation", return_value={"rows": 0}),
        ):
            task(**kwargs)
        return fake, hook

    def test_source_is_the_airflow_connection(self):
        _, hook = self.call("extract_table", oltp.staging_full, table="orders", dag_run=DAG_RUN)
        hook.assert_called_once_with(postgres_conn_id="oltp_postgres")
        hook.return_value.get_conn.return_value.close.assert_called_once()

    def test_full_run_reads_by_the_run_start(self):
        ingest, _ = self.call("extract_table", oltp.staging_full, table="orders", dag_run=DAG_RUN)
        self.assertEqual(ingest.call_args.kwargs, {"created_before": RUN_AFTER.isoformat()})

    def test_incremental_run_reads_the_data_interval(self):
        ingest, _ = self.call(
            "extract_table", oltp.staging_incremental, table="orders",
            data_interval_start=START, data_interval_end=END,
        )
        self.assertEqual(ingest.call_args.kwargs, {"start": START.isoformat(), "end": END.isoformat()})

    def test_incremental_run_without_interval_is_refused(self):
        with self.assertRaisesRegex(ValueError, "backfill"):
            self.call("extract_table", oltp.staging_incremental, table="orders")

    def test_reconcile_compares_the_extract_with_the_source_at_the_cutoff(self):
        reconciled, _ = self.call("reconcile_table", oltp.reconcile, table="orders", dag_run=DAG_RUN)
        self.assertEqual(reconciled.call_args.args[2], {"rows": 0})

    def test_bronze_replaces_on_full_and_upserts_on_incremental(self):
        merged, _ = self.call("merge_bronze", oltp.bronze_full, table="orders")
        self.assertTrue(merged.call_args.kwargs["full"])
        merged, _ = self.call("merge_bronze", oltp.bronze_incremental, table="orders")
        self.assertFalse(merged.call_args.kwargs["full"])

    def test_unknown_table_is_rejected(self):
        with self.assertRaises(KeyError):
            oltp.bronze_full("customer_credentials")

    def test_maintenance_covers_every_oltp_table(self):
        with (
            patch.object(oltp, "maintain_tables", return_value={}) as maintained,
            patch.object(oltp, "iceberg_catalog"),
            patch.object(oltp, "gold_dependencies", return_value={"dim_city": (), "delivery_sla": ()}),
            patch("builtins.print"),
        ):
            oltp.maintain("expire")
            expired = maintained.call_args.args[1]
            oltp.maintain("compact")
            compacted = maintained.call_args.args[1]
        tables = expired
        self.assertEqual(len(expired), 3 * len(oltp.SOURCE_TABLES) + 2)
        self.assertNotIn(("silver", "orders"), compacted)
        self.assertEqual(len(compacted), 2 * len(oltp.SOURCE_TABLES) + 2)
        self.assertIn(("gold", "delivery_sla"), tables)
        self.assertEqual(maintained.call_args.args[2], "compact")


if __name__ == "__main__":
    unittest.main()
