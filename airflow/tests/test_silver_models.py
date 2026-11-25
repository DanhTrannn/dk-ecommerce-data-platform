import unittest
from pathlib import Path

from lakehouse.oltp.sources import SOURCE_TABLES


class SilverModelTests(unittest.TestCase):
    def test_silver_models_select_every_source_column(self):
        self.assertEqual(len(SOURCE_TABLES), 19)
        for table in SOURCE_TABLES:
            sql = (Path(__file__).parents[2] / f"dbt/models/silver/{table.name}.sql").read_text()
            self.assertIn(f"unique_key='{table.pk}'", sql)
            self.assertIn(f"source('bronze', '{table.name}')", sql)
            for column in [*table.columns, "_ingested_at"]:
                self.assertIn(f" AS {column}", sql, f"{table.name}.{column}")

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
