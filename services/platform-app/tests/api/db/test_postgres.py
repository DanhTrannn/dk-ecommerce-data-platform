"""Opt-in integration tests: own PostgreSQL container, Unix socket, no network."""

import os
import unittest
import uuid
from datetime import UTC, datetime

from sqlalchemy import CheckConstraint, inspect, insert, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .postgres_fixture import PostgresFixture

from platform_app.api.db.base import Base
from platform_app.api.models import (
    Cart,
    Category,
    City,
    Customer,
    Order,
    Product,
    ProductVariant,
    ReturnRequest,
    Store,
    StoreInventory,
)


@unittest.skipUnless(
    os.environ.get("RUN_POSTGRES_SCHEMA_TESTS") == "1", "Set RUN_POSTGRES_SCHEMA_TESTS=1 for isolated Docker tests"
)
class PostgreSQLSchemaTests(PostgresFixture):
    def setUp(self):
        self.connection = self.engine.connect()
        self.addCleanup(self.connection.close)
        transaction = self.connection.begin()
        self.addCleanup(transaction.rollback)

    def add(self, model, **values):
        return self.connection.execute(insert(model).values(**values).returning(model.__table__)).mappings().one()

    def customer(self, **values):
        return self.add(Customer, public_id=uuid.uuid4(), display_name="Schema fixture", **values)

    def product(self):
        category = self.add(Category, public_id=uuid.uuid4(), code=uuid.uuid4().hex, name="Fixture")
        product = self.add(
            Product, public_id=uuid.uuid4(), category_id=category["category_id"], slug=uuid.uuid4().hex, name="Fixture"
        )
        return self.add(
            ProductVariant,
            public_id=uuid.uuid4(),
            product_id=product["product_id"],
            sku=uuid.uuid4().hex,
            size_code="M",
            color_code="black",
            price_vnd=100000,
        )

    def order_values(self):
        customer = self.customer()
        return dict(
            order_number=uuid.uuid4().hex,
            customer_id=customer["customer_id"],
            checkout_idempotency_key=uuid.uuid4().hex,
            status="paid",
            subtotal_vnd=100000,
            shipping_fee_vnd=30000,
            total_vnd=130000,
            receiver_name="Fixture",
            receiver_phone="000",
            shipping_address_text="Fixture",
        )

    def reject(self, statement, parameters=None):
        with self.assertRaises(IntegrityError):
            with self.connection.begin_nested():
                self.connection.execute(statement, parameters or {})

    def test_all_tables_columns_foreign_keys_indexes_and_native_types_exist(self):
        inspector = inspect(self.connection)
        self.assertEqual(set(inspector.get_table_names()), set(Base.metadata.tables))
        for name, model in Base.metadata.tables.items():
            columns = {column["name"]: column for column in inspector.get_columns(name)}
            self.assertEqual(set(columns), set(model.c.keys()))
            self.assertEqual(len(inspector.get_foreign_keys(name)), len(model.foreign_key_constraints))
            checks = [c for c in model.constraints if isinstance(c, CheckConstraint)]
            checks.extend(c for column in model.c for c in column.constraints if isinstance(c, CheckConstraint))
            self.assertEqual(len(inspector.get_check_constraints(name)), len(checks))
            reflected_indexes = {index["name"] for index in inspector.get_indexes(name)}
            self.assertTrue({index.name for index in model.indexes} <= reflected_indexes)
            for column in model.c:
                self.assertEqual(columns[column.name]["nullable"], column.nullable)
                if column.identity is not None:
                    self.assertIn("identity", columns[column.name])
                if column.name == "public_id":
                    self.assertEqual(str(columns[column.name]["type"]), "UUID")

    def test_uuid_roundtrip_supports_api_string_identifiers(self):
        value = uuid.uuid4()
        customer = self.add(Customer, public_id=str(value), display_name="Fixture")
        found = self.connection.execute(select(Customer.public_id).where(Customer.public_id == str(value))).scalar_one()
        self.assertEqual(customer["public_id"], value)
        self.assertEqual(found, value)

    def test_orm_roundtrip_and_server_generated_update_timestamp(self):
        with Session(bind=self.connection, join_transaction_mode="create_savepoint") as session:
            customer = Customer(public_id=str(uuid.uuid4()), display_name="Before")
            session.add(customer)
            session.flush()
            self.assertIsInstance(customer.public_id, str)
            session.refresh(customer)
            self.assertIsInstance(customer.public_id, uuid.UUID)
            before = customer.updated_at
            customer.display_name = "After"
            session.flush()
            session.refresh(customer)
            self.assertGreaterEqual(customer.updated_at, before)

    def test_boolean_presence_check_and_active_cart_uniqueness(self):
        customer = self.customer()
        cart = self.add(Cart, public_id=uuid.uuid4(), customer_id=customer["customer_id"])
        self.reject(insert(Cart).values(public_id=uuid.uuid4(), customer_id=customer["customer_id"], status="active"))
        self.connection.execute(update(Cart).where(Cart.cart_id == cart["cart_id"]).values(status="checked_out"))
        second = self.add(Cart, public_id=uuid.uuid4(), customer_id=customer["customer_id"])
        self.assertNotEqual(second["cart_id"], cart["cart_id"])
        variant = self.product()
        self.reject(
            text(
                "INSERT INTO cart_items(cart_id,variant_id,quantity,is_present,removed_at) VALUES(:cart,:variant,1,false,NULL)"
            ),
            {"cart": second["cart_id"], "variant": variant["variant_id"]},
        )

    def test_negative_and_unsigned_32_bit_overflow_are_rejected(self):
        customer = self.customer(boom_count=4294967295)
        self.assertEqual(customer["boom_count"], 4294967295)
        for value in (-1, 4294967296):
            self.reject(
                update(Customer).where(Customer.customer_id == customer["customer_id"]).values(boom_count=value)
            )
        variant = self.product()
        self.reject(
            update(ProductVariant).where(ProductVariant.variant_id == variant["variant_id"]).values(cost_price_vnd=-1)
        )

    def test_foreign_keys_and_case_insensitive_business_key_uniqueness(self):
        self.reject(insert(Customer).values(public_id=uuid.uuid4(), display_name="Fixture", city_id=999999999))
        self.add(City, code="HCM", name="Fixture")
        self.reject(insert(City).values(code="hcm", name="Duplicate"))

    def test_order_money_arithmetic_and_staff_foreign_key(self):
        values = self.order_values()
        self.reject(insert(Order).values(**{**values, "total_vnd": 1}))
        self.reject(insert(Order).values(**{**values, "staff_id": 999999999}))
        order = self.add(Order, **values)
        self.assertEqual(order["total_vnd"], 130000)
        self.assertEqual(order["channel"], "online")
        self.reject(
            update(Order)
            .where(Order.order_id == order["order_id"])
            .values(payment_method="cod", status="confirmed", paid_at=datetime.now(UTC).replace(tzinfo=None))
        )

    def test_nonprimary_identity_and_inventory_balance_checks(self):
        city = self.add(City, code=uuid.uuid4().hex, name="Fixture")
        store = self.add(
            Store, city_id=city["city_id"], code=uuid.uuid4().hex, name="Fixture", address="Fixture", phone="000"
        )
        variant = self.product()
        balance = self.add(
            StoreInventory, store_id=store["store_id"], variant_id=variant["variant_id"], on_hand=2, opening_on_hand=3
        )
        self.assertGreater(balance["store_inventory_id"], 0)
        self.reject(update(StoreInventory).values(on_hand=4))

    def test_defaults_are_utc_and_raw_updates_preserve_explicit_history(self):
        self.connection.execute(text("SET LOCAL TIME ZONE 'Asia/Ho_Chi_Minh'"))
        row = (
            self.connection.execute(
                text(
                    "INSERT INTO customers(public_id,display_name) VALUES(:id,'Before') RETURNING customer_id,created_at,updated_at"
                ),
                {"id": str(uuid.uuid4())},
            )
            .mappings()
            .one()
        )
        self.assertLess(abs((row["created_at"] - datetime.now(UTC).replace(tzinfo=None)).total_seconds()), 5)
        before = datetime(2010, 1, 1)
        self.connection.execute(
            text("UPDATE customers SET updated_at=:at WHERE customer_id=:id"), {"at": before, "id": row["customer_id"]}
        )
        self.connection.execute(
            text("UPDATE customers SET display_name='Changed' WHERE customer_id=:id"), {"id": row["customer_id"]}
        )
        changed = self.connection.execute(
            text("SELECT updated_at FROM customers WHERE customer_id=:id"), {"id": row["customer_id"]}
        ).scalar_one()
        self.assertGreater(changed, before)
        explicit = datetime(2011, 1, 1)
        self.connection.execute(
            text("UPDATE customers SET display_name='History',updated_at=:at WHERE customer_id=:id"),
            {"at": explicit, "id": row["customer_id"]},
        )
        actual = self.connection.execute(
            text("SELECT updated_at FROM customers WHERE customer_id=:id"), {"id": row["customer_id"]}
        ).scalar_one()
        self.assertEqual(actual, explicit)

    def test_noop_update_with_generated_column_does_not_change_timestamp(self):
        customer = self.customer()
        cart = self.add(Cart, public_id=uuid.uuid4(), customer_id=customer["customer_id"])
        self.connection.execute(update(Cart).values(status=Cart.status).where(Cart.cart_id == cart["cart_id"]))
        actual = self.connection.execute(select(Cart.updated_at).where(Cart.cart_id == cart["cart_id"])).scalar_one()
        self.assertEqual(actual, cart["updated_at"])

    def test_json_images_roundtrip_without_binary_encoding(self):
        order = self.add(Order, **self.order_values())
        images = {"items": ["https://example.invalid/ảnh.png"], "note": "Đổi trả"}
        returned = self.add(
            ReturnRequest,
            public_id=uuid.uuid4(),
            return_code=uuid.uuid4().hex,
            order_id=order["order_id"],
            customer_id=order["customer_id"],
            action_type="refund",
            status="pending_review",
            customer_reason="Fixture",
            image_urls=images,
        )
        self.assertEqual(returned["image_urls"], images)


if __name__ == "__main__":
    unittest.main()
