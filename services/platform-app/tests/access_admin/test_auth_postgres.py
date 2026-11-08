"""Actual PostgreSQL authentication and route authorization, isolated from user data."""

import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import text

from api.source_fixture import SourceAPIFixture
from .test_access import MemoryAccess
from platform_app.access_admin.audit import Audit
from platform_app.access_admin.service import snapshot


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_SCHEMA_TESTS") == "1", "opt-in disposable PostgreSQL auth")
class AccessAuthTests(SourceAPIFixture):
    def setUp(self):
        super().setUp()
        from platform_app.access_admin.app import app

        self.access = TestClient(app)
        self.addCleanup(self.access.close)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.audit = Audit(Path(temporary.name) / "audit.jsonl")
        self.backend = MemoryAccess()

        @contextmanager
        def adapter(provider):
            yield self.backend

        self.adapter = patch("platform_app.access_admin.app.adapter", side_effect=adapter)
        self.adapter_mock = self.adapter.start()
        self.addCleanup(self.adapter.stop)
        audit_patch = patch("platform_app.access_admin.app.configured_audit", return_value=self.audit)
        audit_patch.start()
        self.addCleanup(audit_patch.stop)

    def login_admin(self):
        response = self.access.post(
            "/api/v1/access/login", json={"email": "admin@example.com", "password": self.password}
        )
        self.assertEqual(response.status_code, 200, response.text)
        return {"X-CSRF-Token": self.access.cookies.get(self.settings.csrf_cookie_name)}

    def payload(self):
        return dict(
            kind="policy",
            subject="lakehouse-trino",
            target="lakehouse-spark",
            granted=True,
            revision=snapshot(self.backend)["revision"],
        )

    def test_anonymous_and_customer_cannot_read_or_change_access(self):
        self.assertEqual(self.access.get("/api/v1/access/minio").status_code, 401)
        self.assertEqual(self.access.post("/api/v1/access/minio/bindings", json=self.payload()).status_code, 401)
        denied = self.access.post(
            "/api/v1/access/login", json={"email": "customer@example.com", "password": self.password}
        )
        self.assertEqual(denied.status_code, 403)
        self.assertNotIn("set-cookie", denied.headers)
        self.access.cookies.update(self.customer.cookies)
        self.assertEqual(self.access.get("/api/v1/access/polaris").status_code, 403)
        self.assertEqual(self.access.get("/api/v1/access/audit").status_code, 403)
        self.adapter_mock.assert_not_called()

    def test_admin_csrf_revision_and_verified_mutation(self):
        headers = self.login_admin()
        payload = self.payload()
        self.assertEqual(self.access.post("/api/v1/access/minio/bindings", json=payload).status_code, 403)
        self.assertEqual(self.backend.calls, 0)
        changed = self.access.post("/api/v1/access/minio/bindings", json=payload, headers=headers)
        self.assertEqual(changed.status_code, 200, changed.text)
        self.assertEqual(changed.json()["status"], "verified")
        self.assertEqual(self.backend.calls, 1)
        stale = self.access.post("/api/v1/access/minio/bindings", json=payload, headers=headers)
        self.assertEqual(stale.status_code, 409)
        rows = self.access.get("/api/v1/access/audit").json()
        self.assertEqual(rows[0]["status"], "verified")
        self.assertNotIn(self.password, str(rows))

    def test_role_revocation_and_inactive_account_take_effect_next_request(self):
        self.login_admin()
        with self.engine.begin() as connection:
            connection.execute(text("UPDATE customers SET role='customer' WHERE role='admin'"))
        self.assertEqual(self.access.get("/api/v1/access/minio").status_code, 403)
        self.adapter_mock.assert_not_called()
        with self.engine.begin() as connection:
            connection.execute(text("UPDATE customers SET status='inactive'"))
        self.assertEqual(self.access.get("/api/v1/access/me").status_code, 401)

    def test_system_admin_allowed_but_creation_and_extra_inputs_absent(self):
        with self.engine.begin() as connection:
            connection.execute(text("UPDATE customers SET role='system_admin' WHERE role='admin'"))
        headers = self.login_admin()
        self.assertEqual(self.access.get("/api/v1/access/me").json()["role"], "system_admin")
        for path in ("/api/v1/access/minio/users", "/api/v1/access/polaris/principals", "/api/v1/auth/register"):
            self.assertEqual(self.access.post(path, headers=headers, json={}).status_code, 404)
        payload = {**self.payload(), "password": "private-value"}
        invalid = self.access.post("/api/v1/access/minio/bindings", json=payload, headers=headers)
        self.assertEqual(invalid.status_code, 422)
        self.assertNotIn("private-value", invalid.text)
        self.adapter_mock.assert_not_called()

    def test_provider_mismatch_rejected_before_administrative_adapter(self):
        headers = self.login_admin()
        response = self.access.post("/api/v1/access/polaris/bindings", json=self.payload(), headers=headers)
        self.assertEqual(response.status_code, 400)
        self.adapter_mock.assert_not_called()
