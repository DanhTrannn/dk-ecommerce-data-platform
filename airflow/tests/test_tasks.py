import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tasks import oltp
from tasks.dbt import REFERENCE_GOLD, gold_dependencies

REFERENCE = [table.name for table in oltp.SOURCE_TABLES if table.full_load]

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

    def test_schema_checks_the_named_table(self):
        check, _ = self.call("sync_schema", oltp.schema, table="orders")
        self.assertEqual(check.call_args.args[-1], oltp.TABLES_BY_NAME["orders"])

    def test_detect_flags_deletions_at_the_run_start(self):
        detect, _ = self.call("detect_deletes", oltp.detect, table="orders", dag_run=DAG_RUN)
        self.assertEqual(detect.call_args.args[-1], RUN_AFTER)

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

    def test_bronze_overwrite_calls_overwrite_bronze(self):
        overwritten, _ = self.call("overwrite_bronze", oltp.bronze_overwrite, table="categories")
        self.assertEqual(overwritten.call_args.args[1], oltp.TABLES_BY_NAME["categories"])

    def test_unknown_table_is_rejected(self):
        with self.assertRaises(KeyError):
            oltp.bronze_full("customer_credentials")

    def maintained(self, action, **kwargs):
        with (
            patch.object(oltp, "maintain_tables", return_value={}) as maintained,
            patch.object(oltp, "gold_dependencies", return_value={"dim_city": (), "delivery_sla": (), "fact_order": ()}),
            patch("builtins.print"),
        ):
            oltp.maintain(action, **kwargs)
        return maintained.call_args.args[0]

    def test_maintenance_of_reference_tables(self):
        expired = self.maintained("expire", full_load=True)
        self.assertIn(("silver", "categories"), expired)
        self.assertIn(("gold", "dim_city"), expired)
        self.assertIn(("gold", "delivery_sla"), expired)
        self.assertNotIn(("silver", "orders"), expired)
        self.assertNotIn(("gold", "fact_order"), expired)
        self.assertIn(("staging", "categories"), expired)

    def test_optimize_skips_staging(self):
        optimized = self.maintained("optimize", full_load=True)
        self.assertIn(("bronze", "categories"), optimized)
        self.assertFalse([table for table in optimized if table[0] == "staging"])

    def test_maintenance_of_transactional_tables(self):
        expired = self.maintained("expire", full_load=False)
        self.assertIn(("silver", "orders"), expired)
        self.assertIn(("gold", "fact_order"), expired)
        self.assertNotIn(("silver", "categories"), expired)
        self.assertNotIn(("gold", "dim_city"), expired)

    def test_maintenance_by_default_covers_every_oltp_table(self):
        expired = self.maintained("expire")
        groups = set(self.maintained("expire", full_load=True)) | set(self.maintained("expire", full_load=False))
        self.assertEqual(set(expired), groups)
        self.assertEqual(len(expired), 3 * len(oltp.SOURCE_TABLES) + 3)
        self.assertNotIn(("gold", "_mart_state"), expired)

    def test_reference_gold_reads_only_reference_gold(self):
        graph = gold_dependencies()
        for node in REFERENCE_GOLD:
            self.assertLessEqual(set(graph[node]), REFERENCE_GOLD, node)


if __name__ == "__main__":
    unittest.main()
