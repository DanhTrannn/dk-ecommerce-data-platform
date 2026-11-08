"""Access checks, audit outcomes and session isolation without external services."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from streamlit.testing.v1 import AppTest

from platform_app.access_admin.audit import Audit
from platform_app.access_admin.minio import MinioAccess
from platform_app.access_admin.polaris import PolarisAccess
from platform_app.access_admin.schemas import BindingChange
from platform_app.access_admin.service import change_binding, snapshot
from platform_app.access_admin.settings import AccessSettings
from platform_app.api.core.errors import AppError
from platform_app.ui.access_client import AccessSession, AccessUIError


class MemoryAccess:
    def __init__(self):
        self.state = {
            "users": [{"name": "lakehouse-trino", "policies": ["lakehouse-trino"]}],
            "policies": [{"name": "lakehouse-trino"}, {"name": "lakehouse-spark"}],
        }
        self.calls = 0
        self.failure = False

    def snapshot(self):
        return copy.deepcopy(self.state)

    def assigned(self, change, state):
        if change.subject != "lakehouse-trino" or change.target not in {"lakehouse-spark", "lakehouse-trino"}:
            raise AppError("INVALID_BINDING", "Unsupported", 400)
        return change.target in state["users"][0]["policies"]

    def apply(self, change):
        self.calls += 1
        policies = self.state["users"][0]["policies"]
        if change.granted:
            policies.append(change.target)
        else:
            policies.remove(change.target)
        if self.failure:
            raise RuntimeError("private-root-secret")


class AccessServiceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.audit = Audit(Path(temporary.name) / "audit/audit.jsonl")
        self.backend = MemoryAccess()
        self.actor = SimpleNamespace(public_id="actor-id", role="system_admin")

    def change(self, granted=True, target="lakehouse-spark"):
        return BindingChange(
            kind="policy",
            subject="lakehouse-trino",
            target=target,
            granted=granted,
            revision=snapshot(self.backend)["revision"],
        )

    def test_apply_readback_audit_and_remove(self):
        change = self.change()
        self.assertEqual(change_binding(self.backend, change, self.actor, self.audit)["status"], "verified")
        self.assertEqual(change_binding(self.backend, self.change(False), self.actor, self.audit)["status"], "verified")
        self.assertEqual(self.backend.calls, 2)
        self.assertEqual([row["status"] for row in self.audit.recent()], ["verified", "started", "verified", "started"])
        self.assertEqual(self.audit.path.stat().st_mode & 0o777, 0o600)

    def test_stale_revision_does_not_write(self):
        change = self.change()
        self.backend.state["users"][0]["policies"].append("another-policy")
        with self.assertRaises(AppError) as error:
            change_binding(self.backend, change, self.actor, self.audit)
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(self.backend.calls, 0)

    def test_noop_does_not_repeat_upstream(self):
        result = change_binding(self.backend, self.change(True, "lakehouse-trino"), self.actor, self.audit)
        self.assertEqual(result["status"], "noop")
        self.assertEqual(self.backend.calls, 0)

    def test_unknown_outcome_redacts_error_and_does_not_retry(self):
        self.backend.failure = True
        with self.assertRaises(AppError) as error:
            change_binding(self.backend, self.change(), self.actor, self.audit)
        self.assertEqual(error.exception.status_code, 502)
        self.assertNotIn("private-root-secret", str(error.exception))
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual(self.audit.recent()[0]["status"], "unknown")

    def test_unwritable_audit_prevents_mutation(self):
        with patch.object(self.audit, "append", side_effect=OSError("private-path")):
            with self.assertRaises(AppError) as error:
                change_binding(self.backend, self.change(), self.actor, self.audit)
        self.assertEqual(error.exception.status_code, 503)
        self.assertEqual(self.backend.calls, 0)

    def test_post_apply_journal_failure_is_not_success(self):
        append = self.audit.append

        def fail_verified(record):
            if record["status"] == "verified":
                raise OSError("disk failure")
            append(record)

        with patch.object(self.audit, "append", side_effect=fail_verified):
            with self.assertRaises(AppError) as error:
                change_binding(self.backend, self.change(), self.actor, self.audit)
        self.assertEqual(error.exception.status_code, 503)
        self.assertEqual(self.backend.calls, 1)

    def test_root_subject_rejected(self):
        change = self.change().model_copy(update={"subject": "root"})
        with self.assertRaises(AppError):
            change_binding(self.backend, change, self.actor, self.audit)
        self.assertEqual(self.backend.calls, 0)


class AdapterTests(unittest.TestCase):
    def test_mc_secret_environment_only_and_error_redacted(self):
        seen = []

        def runner(command, **kwargs):
            seen.append((command, kwargs))
            return SimpleNamespace(returncode=1, stdout="private-root-secret", stderr="private-root-secret")

        settings = AccessSettings(minio_user="root", minio_secret="private-root-secret", _env_file=None)
        backend = MinioAccess(settings, runner=runner)
        with self.assertRaises(AppError) as error:
            backend.command("admin", "policy", "list", "accessadmin")
        self.assertNotIn("private-root-secret", str(error.exception))
        self.assertFalse(any("private-root-secret" in part for part in seen[0][0]))
        self.assertIn("private-root-secret", seen[0][1]["env"]["MC_HOST_accessadmin"])
        self.assertEqual(len(seen), 1)

    def test_minio_policy_order_does_not_create_a_stale_revision(self):
        from platform_app.access_admin.minio import canonical_policy
        from platform_app.access_admin.service import revision

        first = {
            "Statement": [
                {"Action": ["s3:PutObject", "s3:GetObject"], "Resource": ["b", "a"]},
                {"Action": ["s3:ListBucket"]},
            ]
        }
        second = {
            "Statement": [
                {"Action": ["s3:ListBucket"]},
                {"Resource": ["a", "b"], "Action": ["s3:GetObject", "s3:PutObject"]},
            ]
        }
        self.assertEqual(revision(canonical_policy(first)), revision(canonical_policy(second)))

    def test_polaris_filters_secrets_and_protected_roles(self):
        class Client:
            def call(self, method, path):
                if path.endswith("/principals"):
                    return {
                        "principals": [
                            {"name": "root", "clientId": "private"},
                            {"name": "trino_reader", "clientId": "private"},
                        ]
                    }
                if path.endswith("/principal-roles"):
                    return {"roles": [{"name": "service_admin"}, {"name": "trino_reader_role"}]}
                if path.endswith("/catalog-roles"):
                    return {"roles": [{"name": "catalog_admin"}, {"name": "trino_reader_catalog_role"}]}
                if path.endswith("/grants"):
                    return {"grants": [{"type": "catalog", "privilege": "TABLE_READ_DATA"}]}
                return {"roles": [{"name": "trino_reader_catalog_role"}]}

        backend = PolarisAccess(AccessSettings(_env_file=None), client=Client())
        state = backend.snapshot()
        self.assertNotIn("private", json.dumps(state))
        self.assertEqual([item["name"] for item in state["principals"]], ["trino_reader"])
        self.assertNotIn("catalog_admin", [item["name"] for item in state["catalog_roles"]])
        change = BindingChange(
            kind="principal_role", subject="root", target="trino_reader_role", granted=True, revision="0" * 64
        )
        with self.assertRaises(AppError):
            backend.assigned(change, state)


class AccessClientTests(unittest.TestCase):
    def test_cookie_csrf_session_and_no_mutation_retry(self):
        calls = []

        def handle(request):
            calls.append(request)
            if request.url.path.endswith("/login"):
                return httpx.Response(
                    200,
                    json={"role": "admin", "csrf_cookie_name": "csrf"},
                    headers=[("set-cookie", "auth=token; Path=/"), ("set-cookie", "csrf=csrf-value; Path=/")],
                )
            return httpx.Response(503, json={"error": {"message": "Unavailable"}})

        session = AccessSession("http://internal.test", transport=httpx.MockTransport(handle))
        self.addCleanup(session.close)
        session.login("admin@example.com", "password")
        with self.assertRaises(AccessUIError):
            session.request("POST", "minio/bindings", {})
        self.assertEqual(calls[-1].headers["X-CSRF-Token"], "csrf-value")
        self.assertIn("auth=token", calls[-1].headers["cookie"])
        self.assertEqual(len(calls), 2)


class AccessViewTests(unittest.TestCase):
    def test_login_apply_once_clear_password_and_logout(self):
        instances = []

        class Session:
            csrf_cookie = "csrf"

            def __init__(self, *args):
                self.calls = []
                self.closed = False
                instances.append(self)

            def login(self, email, password):
                return {"public_id": "admin", "role": "admin"}

            def close(self):
                self.closed = True

            def request(self, method, path, payload=None):
                self.calls.append((method, path, payload))
                if path == "me":
                    return {"role": "admin"}
                if path == "polaris":
                    return {
                        "revision": "0" * 64,
                        "principals": [{"name": "trino_reader", "roles": []}],
                        "principal_roles": [{"name": "reader", "catalog_roles": []}],
                        "catalog_roles": [{"name": "read", "grants": []}],
                        "privileges": ["TABLE_READ_DATA"],
                    }
                if path.endswith("/bindings"):
                    return {"status": "verified", "operation_id": "operation"}
                return []

        app = AppTest.from_string("from platform_app.ui.views.access import render\nrender()")
        with patch("platform_app.ui.views.access.AccessSession", Session):
            app.run()
            app.text_input[0].set_value("admin@example.com")
            app.text_input[1].set_value("private-password")
            next(button for button in app.button if button.label == "Đăng nhập quản trị").click().run()
            self.assertFalse(app.exception)
            self.assertEqual(app.session_state["access_password"], "")
            next(button for button in app.button if button.label == "Áp dụng Polaris").click().run()
            app.run()
            self.assertEqual(sum(path.endswith("/bindings") for _, path, _ in instances[0].calls), 1)
            next(button for button in app.button if button.label == "Đăng xuất quản trị").click().run()
            self.assertTrue(instances[0].closed)
            self.assertTrue(any(button.label == "Đăng nhập quản trị" for button in app.button))
