import io
import json
import unittest
import uuid

from platform_app.api.db.generated_sql import iter_statements, read_metadata


class GeneratedSQLTests(unittest.TestCase):
    def test_location_metadata_checks_counts_but_legacy_metadata_remains_valid(self):
        self.assertNotIn("locations", read_metadata(self.artifact()))
        counts = dict(customers=2, products=1, variants=1, orders=0, cities=1, stores=1)
        valid = self.artifact(counts=counts, locations={"store_codes": ["HN-CENTRE"]})
        self.assertEqual(read_metadata(valid)["counts"]["cities"], 1)
        for override in (dict(cities=0), dict(stores=True)):
            with self.assertRaises(ValueError):
                read_metadata(self.artifact(counts={**counts, **override}, locations={"store_codes": ["HN-CENTRE"]}))

    def artifact(self, **changes):
        metadata = dict(
            sql_format="postgresql-v1",
            generator_version="0.9.0",
            generation_run_id=str(uuid.uuid4()),
            logical_identity=str(uuid.uuid4()),
            counts=dict(customers=2, products=1, variants=1, orders=0),
        )
        metadata.update(changes)
        return io.StringIO(
            "-- TLCN deterministic PostgreSQL dataset\n-- import_metadata: "
            + json.dumps(metadata)
            + "\n-- scenario\nBEGIN;\n"
        )

    def test_metadata_contract_and_rejection_of_legacy_or_invalid_counts(self):
        self.assertEqual(read_metadata(self.artifact())["counts"]["orders"], 0)
        for stream in (
            io.StringIO("-- MySQL\n"),
            self.artifact(sql_format="mysql"),
            self.artifact(counts=dict(customers=True, products=1, variants=1, orders=0)),
        ):
            with self.assertRaises(ValueError):
                read_metadata(stream)

    def test_preserves_escaped_strings_and_dollar_blocks(self):
        sql = "INSERT INTO t VALUES(E'O\\'Brien;\\n100%');\nDO $test$\nBEGIN\n NULL;\nEND;\n$test$;\nCOMMIT;\n"
        statements = list(iter_statements(io.StringIO(sql)))
        self.assertEqual(len(statements), 2)
        self.assertIn("100%", statements[0])
        self.assertTrue(statements[1].endswith("$test$;\n"))

    def test_rejects_truncation_repeated_boundaries_and_trailing_sql(self):
        for sql in (
            "SELECT 1;\n",
            "INSERT INTO t VALUES(\nCOMMIT;\n",
            "BEGIN;\nCOMMIT;\n",
            "ROLLBACK;\nCOMMIT;\n",
            "COMMIT;\nSELECT 1;\n",
            "DO $x$\nBEGIN\nCOMMIT;\n",
        ):
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                list(iter_statements(io.StringIO(sql)))
