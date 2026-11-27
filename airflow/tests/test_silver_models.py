import re
import unittest
from pathlib import Path

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks.dbt import REFERENCE_GOLD, gold_seeds

DBT = Path(__file__).parents[2] / "dbt"


class SilverModelTests(unittest.TestCase):
    def test_silver_models_select_every_source_column(self):
        self.assertEqual(len(SOURCE_TABLES), 19)
        for table in SOURCE_TABLES:
            sql = (Path(__file__).parents[2] / f"dbt/models/silver/{table.name}.sql").read_text()
            if table.full_load:
                self.assertIn("materialized='table'", sql, table.name)
                self.assertNotIn("is_incremental", sql, table.name)
            else:
                self.assertIn(f"unique_key='{table.pk}'", sql)
            self.assertIn(f"source('bronze', '{table.name}')", sql)
            for column in [*table.columns, "_ingested_at"]:
                self.assertIn(f" AS {column}", sql, f"{table.name}.{column}")

    def test_reference_gold_models_read_only_reference_data(self):
        allowed = {table.name for table in SOURCE_TABLES if table.full_load} | gold_seeds() | REFERENCE_GOLD
        for node in REFERENCE_GOLD - gold_seeds():
            refs = set(re.findall(r"ref\('([a-z_]+)'\)", (DBT / f"models/oltp/{node}.sql").read_text()))
            self.assertLessEqual(refs, allowed, node)

    def test_dimensions_are_rebuilt_and_facts_keep_no_previous_dates(self):
        for path in (DBT / "models/oltp").glob("dim_*.sql"):
            self.assertNotIn("incremental", path.read_text(), path.name)
        for path in (DBT / "models/oltp").glob("fact_*.sql"):
            self.assertNotIn("previous_dates", path.read_text(), path.name)
        self.assertIn("month(created_at)", (DBT / "models/oltp/fact_shipment.sql").read_text())

    def test_every_silver_model_is_tested_against_bronze(self):
        tests = (Path(__file__).parents[2] / "dbt/models/silver/silver.yml").read_text()
        for table in SOURCE_TABLES:
            self.assertIn(f"key: {table.pk}\n            bronze: source('bronze', '{table.name}')", tests, table.name)

    def test_dbt_sources_use_plain_table_names(self):
        text = (Path(__file__).parents[2] / "dbt/models/sources.yml").read_text()
        self.assertNotIn("identifier: silver_", text)
        for table in SOURCE_TABLES:
            self.assertIn(f"- name: {table.name}\n", text)


if __name__ == "__main__":
    unittest.main()
