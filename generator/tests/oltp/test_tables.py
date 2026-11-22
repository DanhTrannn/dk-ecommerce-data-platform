import re
import unittest
from pathlib import Path

import polars as pl

from datagen.oltp.tables import ID_COLUMNS, LOAD_ORDER, SCHEMAS, conform, empty

SCHEMA_SQL = Path(__file__).resolve().parents[3] / "database" / "schema.sql"


def schema_sql_columns():
    tables = {}
    for name, body in re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", SCHEMA_SQL.read_text(encoding="utf-8"), re.S):
        tables[name] = {m.group(1) for m in re.finditer(r"^\t(\w+) ", body, re.M) if m.group(1) != "CONSTRAINT"}
    return tables


class TableTests(unittest.TestCase):
    def test_schemas_cover_27_tables_in_load_order(self):
        self.assertEqual(len(SCHEMAS), 27)
        self.assertNotIn("customer_credentials", SCHEMAS)
        self.assertEqual(set(LOAD_ORDER), set(SCHEMAS))
        self.assertEqual(len(LOAD_ORDER), 27)
        self.assertNotIn("active_customer_guard", SCHEMAS["carts"])
        self.assertEqual(set(SCHEMAS) - set(ID_COLUMNS), {"inventory"})

    def test_schema_columns_match_schema_sql(self):
        expected = schema_sql_columns()
        for table, columns in SCHEMAS.items():
            self.assertEqual(set(columns), expected[table] - {"active_customer_guard"}, table)

    def test_conform_orders_columns(self):
        frame = empty("cities").select(list(reversed(SCHEMAS["cities"])))
        self.assertEqual(conform("cities", frame).columns, list(SCHEMAS["cities"]))

    def test_conform_rejects_missing_column(self):
        with self.assertRaisesRegex(ValueError, "name"):
            conform("cities", empty("cities").drop("name"))

    def test_conform_rejects_extra_column(self):
        with self.assertRaisesRegex(ValueError, "extra"):
            conform("cities", empty("cities").with_columns(pl.lit(1).alias("extra")))

    def test_conform_fills_only_the_named_null_columns(self):
        frame = empty("delivery_staff").drop("vehicle_plate")
        filled = conform("delivery_staff", frame, null_columns=("vehicle_plate",))
        self.assertEqual(filled.columns, list(SCHEMAS["delivery_staff"]))
        with self.assertRaisesRegex(ValueError, "phone"):
            conform("delivery_staff", frame.drop("phone"), null_columns=("vehicle_plate",))

    def test_empty_has_the_declared_schema(self):
        frame = empty("orders")
        self.assertEqual(frame.height, 0)
        self.assertEqual(frame.schema, pl.Schema(SCHEMAS["orders"]))


if __name__ == "__main__":
    unittest.main()
