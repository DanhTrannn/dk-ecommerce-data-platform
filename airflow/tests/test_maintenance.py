import os
import unittest
from unittest.mock import MagicMock, patch

from lakehouse.utils import maintenance

ENV = {"DBT_TRINO_HOST": "trino", "TRINO_MAINTAINER_PASSWORD": "secret", "TRINO_CA_FILE": "/run/trino-ca/ca.pem"}


class StatementTests(unittest.TestCase):
    def test_optimize_rewrites_small_files(self):
        self.assertEqual(
            maintenance.statement("optimize", ("bronze", "orders")),
            "ALTER TABLE lakehouse_etl.bronze.orders EXECUTE optimize",
        )

    def test_expire_keeps_seven_days_of_snapshots(self):
        self.assertEqual(
            maintenance.statement("expire", ("gold", "fact_order")),
            "ALTER TABLE lakehouse_etl.gold.fact_order EXECUTE expire_snapshots(retention_threshold => '7d')",
        )

    def test_orphan_removes_files_older_than_seven_days(self):
        self.assertEqual(
            maintenance.statement("orphan", ("staging", "orders")),
            "ALTER TABLE lakehouse_etl.staging.orders EXECUTE remove_orphan_files(retention_threshold => '7d')",
        )


class MaintainTablesTests(unittest.TestCase):
    def test_runs_one_statement_per_table_as_the_maintainer(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value
        with patch.dict(os.environ, ENV, clear=True), patch.object(maintenance.trino.dbapi, "connect", return_value=connection) as connect:
            result = maintenance.maintain_tables([("bronze", "orders"), ("gold", "dim_city")], "optimize")
        kwargs = connect.call_args.kwargs
        self.assertEqual((kwargs["host"], kwargs["port"], kwargs["user"]), ("trino", 8443, "lakehouse_maintainer"))
        self.assertEqual((kwargs["http_scheme"], kwargs["verify"], kwargs["catalog"]), ("https", "/run/trino-ca/ca.pem", "lakehouse_etl"))
        self.assertEqual(
            [call.args[0] for call in cursor.execute.call_args_list],
            ["ALTER TABLE lakehouse_etl.bronze.orders EXECUTE optimize", "ALTER TABLE lakehouse_etl.gold.dim_city EXECUTE optimize"],
        )
        self.assertEqual(result, {"bronze.orders": "done", "gold.dim_city": "done"})
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
