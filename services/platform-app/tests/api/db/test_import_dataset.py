"""Exercise real generated artifacts and seeds in a disposable PostgreSQL only."""

import importlib
import os
import tempfile
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from generator.config import load_config
from generator.sql_export import export_sql
from psycopg2 import Error as DriverError
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from platform_app.api.db.base import Base
from platform_app.api.db.import_dataset import import_dataset

from .postgres_fixture import PostgresFixture

ROOT = Path(__file__).resolve().parents[5]


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_SCHEMA_TESTS") == "1", "Enable isolated PostgreSQL tests")
class ImportDatasetTests(PostgresFixture):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "dataset.sql"
        self.config = replace(
            load_config(ROOT / "generator/configs/small.yml"),
            scale=dict(customers=80, products=12, variants=48, orders=800),
        )
        with self.engine.begin() as connection:
            tables = ",".join(f'public."{name}"' for name in sorted(Base.metadata.tables))
            connection.exec_driver_sql(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")

    def scalar(self, sql):
        with self.engine.connect() as connection:
            return connection.scalar(text(sql))

    def test_real_presets_import_and_identity_continues_above_loaded_ids(self):
        for preset in ("small", "month-test"):
            config = load_config(ROOT / f"generator/configs/{preset}.yml")
            export_sql(config, self.path)
            receipt = import_dataset(self.engine, self.path)
            self.assertEqual(receipt["orders"], config.scale["orders"])
            self.assertEqual(receipt["customers"], config.scale["customers"])
            self.assertEqual(receipt["sql_format"], "postgresql-v1")
            self.assertGreater(receipt["placed_order_value_vnd"], 0)
        maximum = self.scalar("SELECT MAX(customer_id) FROM customers")
        with self.engine.begin() as connection:
            new_id = connection.scalar(
                text("INSERT INTO customers(public_id,display_name) VALUES(:id,'Fixture') RETURNING customer_id"),
                {"id": str(uuid.uuid4())},
            )
        self.assertGreater(new_id, maximum)
        self.assertGreater(self.scalar("SELECT COUNT(*) FROM refunds"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM refunds WHERE updated_at>timestamp '2026-07-01'"), 0)

    def test_duplicate_rejected_without_rows_or_sequence_changes(self):
        export_sql(self.config, self.path)
        import_dataset(self.engine, self.path)
        before = self.scalar("SELECT last_value FROM customers_customer_id_seq")
        count = self.scalar("SELECT COUNT(*) FROM customers")
        with self.assertRaisesRegex(ValueError, "already imported"):
            import_dataset(self.engine, self.path)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM customers"), count)
        self.assertEqual(self.scalar("SELECT last_value FROM customers_customer_id_seq"), before)

    def test_import_keeps_a_previously_higher_sequence_and_text_roundtrips(self):
        from generator.sql_export import _sql_literal

        value = "O'Brien\\100%;\nĐổi trả\r\t"
        with self.engine.begin() as connection:
            connection.exec_driver_sql("ALTER SEQUENCE customers_customer_id_seq RESTART WITH 9000000000000")
            connection.exec_driver_sql("SET LOCAL standard_conforming_strings=on")
            self.assertEqual(
                connection.exec_driver_sql(
                    "SELECT " + _sql_literal(value), execution_options={"no_parameters": True}
                ).scalar_one(),
                value,
            )
        export_sql(self.config, self.path)
        import_dataset(self.engine, self.path)
        self.assertEqual(self.scalar("SELECT nextval('customers_customer_id_seq')"), 9000000000000)

    def test_failure_after_sequence_restart_rolls_back_rows_and_sequence(self):
        export_sql(self.config, self.path)
        sql = self.path.read_text()
        for suffix, error in (("SELECT 1/0;\nCOMMIT;\n", DriverError), ("", ValueError)):
            with self.subTest(suffix=suffix):
                self.path.write_text(sql.removesuffix("COMMIT;\n") + suffix)
                before = self.scalar("SELECT last_value FROM customers_customer_id_seq")
                with self.assertRaises(error) as failure:
                    import_dataset(self.engine, self.path)
                if error is DriverError:
                    self.assertEqual(failure.exception.pgcode, "22012")
                self.assertEqual(self.scalar("SELECT COUNT(*) FROM customers"), 0)
                self.assertEqual(self.scalar("SELECT last_value FROM customers_customer_id_seq"), before)

    def test_validation_failure_rolls_back_and_missing_trigger_rejects_import(self):
        export_sql(self.config, self.path)
        sql = self.path.read_text()
        self.path.write_text(
            sql.replace("DO $tlcn_verify$", "UPDATE inventory SET opening_on_hand=opening_on_hand+1;\nDO $tlcn_verify$")
        )
        with self.assertRaisesRegex(DriverError, "inventory ledger mismatch"):
            import_dataset(self.engine, self.path)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM products"), 0)
        self.path.write_text(sql)
        with self.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE customers DISABLE TRIGGER trg_customers_updated_at")
        try:
            with self.assertRaisesRegex(ValueError, "trigger"):
                import_dataset(self.engine, self.path)
        finally:
            with self.engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE customers ENABLE TRIGGER trg_customers_updated_at")

    def test_seeds_before_and_after_import_preserve_existing_data_and_repair_partial_seed(self):
        from database.seeds import seed_catalog

        city_seed = importlib.import_module("database.seeds.0010_multi_city_stores")
        factory = sessionmaker(self.engine)
        settings = SimpleNamespace(
            bootstrap_admin_email="fixture@example.invalid",
            bootstrap_admin_password=uuid.uuid4().hex,
            bootstrap_admin_display_name="Fixture Admin",
        )
        with self.engine.begin() as connection:
            connection.exec_driver_sql("INSERT INTO cities(code,name) VALUES('hcm','Preserved')")
        city_seed.seed(factory)
        seed_catalog.seed(factory, settings)
        export_sql(self.config, self.path)
        import_dataset(self.engine, self.path)
        credential = self.scalar(
            "SELECT password_hash FROM customer_credentials WHERE email_normalized='fixture@example.invalid'"
        )
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE inventory SET on_hand=on_hand-1,version=7 WHERE variant_id=(SELECT MIN(variant_id) FROM inventory)"
            )
        stock = self.scalar("SELECT SUM(on_hand) FROM inventory")
        counts = {table: self.scalar(f'SELECT COUNT(*) FROM "{table}"') for table in Base.metadata.tables}
        city_seed.seed(factory)
        seed_catalog.seed(factory, settings)
        self.assertEqual(
            counts, {table: self.scalar(f'SELECT COUNT(*) FROM "{table}"') for table in Base.metadata.tables}
        )
        self.assertEqual(self.scalar("SELECT SUM(on_hand) FROM inventory"), stock)
        self.assertEqual(
            self.scalar(
                "SELECT password_hash FROM customer_credentials WHERE email_normalized='fixture@example.invalid'"
            ),
            credential,
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM cities"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM stores"), 6)
        self.assertEqual(self.scalar("SELECT name FROM cities WHERE lower(code)='hcm'"), "Preserved")

    def test_seeds_after_import_without_prior_catalog(self):
        from database.seeds import seed_catalog

        export_sql(self.config, self.path)
        import_dataset(self.engine, self.path)
        factory = sessionmaker(self.engine)
        settings = SimpleNamespace(
            bootstrap_admin_email="fixture@example.invalid",
            bootstrap_admin_password=uuid.uuid4().hex,
            bootstrap_admin_display_name="Fixture Admin",
        )
        seed_catalog.seed(factory, settings)
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM customers JOIN customer_credentials USING(customer_id) WHERE role='admin' AND email_normalized='fixture@example.invalid'"
            ),
            1,
        )
        self.assertIsNotNone(self.scalar("SELECT product_id FROM products WHERE slug='ao-thun-basic'"))
