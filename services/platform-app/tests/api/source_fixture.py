"""Real HTTP routes and transactions against a disposable source PostgreSQL."""

import os
import uuid
from datetime import UTC, datetime
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from generator.locations import LocationConfig, locations_sql
from generator.postgres_sql import lock_sql, restart_identities_sql
from generator.sql_export import _sql_literal
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .db.postgres_fixture import PostgresFixture


class SourceAPIFixture(PostgresFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.password = uuid.uuid4().hex
        environment = patch.dict(
            os.environ,
            {
                "API_DATABASE_URL": cls.engine.url.render_as_string(hide_password=False),
                "API_SECRET_KEY": uuid.uuid4().hex,
                "API_ENVIRONMENT": "test",
                "API_COOKIE_SECURE": "false",
                "API_BOOTSTRAP_ADMIN_EMAIL": "admin@example.com",
                "API_BOOTSTRAP_ADMIN_PASSWORD": cls.password,
            },
        )
        environment.start()
        cls.addClassCleanup(environment.stop)
        from platform_app.api.core.config import get_settings

        get_settings.cache_clear()
        cls.addClassCleanup(get_settings.cache_clear)
        cls.settings = get_settings()
        from platform_app.api.db import deps, session, uow
        from platform_app.api.routes import health
        from platform_app.api.routes.router import api_router, internal_router
        from platform_app.api.core.handlers import register_exception_handlers

        cls.addClassCleanup(session.engine.dispose)
        cls.factory = sessionmaker(cls.engine, autoflush=False, expire_on_commit=False)
        for module, attribute, value in (
            (session, "SessionLocal", cls.factory),
            (deps, "SessionLocal", cls.factory),
            (uow, "SessionLocal", cls.factory),
            (health, "engine", cls.engine),
        ):
            binding = patch.object(module, attribute, value)
            binding.start()
            cls.addClassCleanup(binding.stop)
        cls.app = FastAPI()
        register_exception_handlers(cls.app)
        cls.app.include_router(api_router)
        cls.app.include_router(internal_router)

    def setUp(self):
        from database.seeds.seed_catalog import seed
        from platform_app.api.db.base import Base

        with self.engine.begin() as connection:
            tables = ",".join(f'public."{name}"' for name in sorted(Base.metadata.tables))
            connection.exec_driver_sql(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
            connection.exec_driver_sql(lock_sql())
            connection.exec_driver_sql(
                locations_sql(LocationConfig(), datetime.now(UTC), _sql_literal),
                execution_options={"no_parameters": True},
            )
            connection.exec_driver_sql(restart_identities_sql(), execution_options={"no_parameters": True})
        seed(self.factory, self.settings)
        self.admin = self.client()
        self.call(self.admin, "POST", "/api/v1/auth/login", {"email": "admin@example.com", "password": self.password})
        self.customer = self.client()
        self.call(
            self.customer,
            "POST",
            "/api/v1/auth/register",
            {
                "email": "customer@example.com",
                "password": self.password,
                "display_name": "API fixture",
            },
            expected=201,
        )
        products = self.call(self.admin, "GET", "/api/v1/admin/products?search=ao-thun-basic")
        self.variant = {**products[0]["variants"][0], "slug": products[0]["slug"]}
        stores = self.call(self.admin, "GET", "/api/v1/locations/cities/HCM/stores")
        self.store_id = stores[0]["store_id"]

    def client(self):
        client = TestClient(self.app)
        self.addCleanup(client.close)
        return client

    def scalar(self, sql, parameters=None):
        with self.engine.connect() as connection:
            return connection.scalar(text(sql), parameters or {})

    def call(self, client, method, path, payload=None, expected=200, key=None):
        headers = {}
        if csrf := client.cookies.get(self.settings.csrf_cookie_name):
            headers["X-CSRF-Token"] = csrf
        if key:
            headers["Idempotency-Key"] = key
        response = client.request(method, path, json=payload, headers=headers)
        self.assertEqual(response.status_code, expected, response.text)
        return response.json() if response.content else None

    def checkout(self, method="cod", coupon=None, quantity=2):
        self.call(self.customer, "PUT", f"/api/v1/cart/items/{self.variant['public_id']}", {"quantity": quantity})
        payload = {
            "receiver_name": "Fixture",
            "receiver_phone": "0000000000",
            "shipping_address_text": "Fixture, Quận 1, TP. Hồ Chí Minh",
            "payment_method": method,
        }
        if coupon:
            payload["coupon_code"] = coupon
        key = uuid.uuid4().hex
        result = self.call(self.customer, "POST", "/api/v1/checkout", payload, key=key)
        return result, payload, key

    def deliver(self, number):
        staff = self.call(
            self.admin,
            "POST",
            "/api/v1/admin/delivery-staff",
            {
                "full_name": "Fixture courier",
                "phone": "0000000000",
                "vehicle_plate": "TEST",
            },
            expected=201,
        )
        self.call(
            self.admin,
            "POST",
            f"/api/v1/admin/orders/{number}/dispatch",
            {"delivery_staff_id": staff["staff_id"]},
            key=uuid.uuid4().hex,
        )
        self.call(self.admin, "POST", f"/api/v1/admin/orders/{number}/deliver", key=uuid.uuid4().hex)
        self.call(self.customer, "POST", f"/api/v1/orders/{number}/complete", key=uuid.uuid4().hex)

    def assert_ledger(self):
        for table, scope, join in (
            ("inventory", "central_warehouse", ""),
            ("store_inventory", "store", "AND t.store_id=i.store_id"),
        ):
            bad = self.scalar(
                f"""SELECT COUNT(*) FROM {table} i WHERE i.on_hand <> i.opening_on_hand+
                COALESCE((SELECT SUM(t.quantity_delta) FROM inventory_transactions t
                WHERE t.variant_id=i.variant_id AND t.location_type=:scope {join}),0)""",
                {"scope": scope},
            )
            self.assertEqual(bad, 0, table)
