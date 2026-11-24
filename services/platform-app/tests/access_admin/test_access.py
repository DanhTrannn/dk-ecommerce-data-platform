import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from pydantic import ValidationError
from streamlit.testing.v1 import AppTest

from platform_app.access_admin.audit import Audit
from platform_app.access_admin.polaris import PolarisAccess
from platform_app.access_admin.schemas import BindingChange, ResourceCreate, ResourceMutation, WorkloadAssignment
from platform_app.access_admin.service import change_binding
from platform_app.access_admin.settings import AccessSettings
from platform_app.api.core.errors import AppError
from platform_app.credential_export.schemas import Assignment
from platform_app.credential_export.storage import ExportSettings, ExportStore
from platform_app.provisioning.state import save_json
from platform_app.ui.access_client import AccessSession, AccessUIError


class MemoryAccess:
    """Fake Polaris holding one principal and its principal roles."""

    def __init__(self):
        self.state = {"principals": [{"name": "trino_reader", "roles": ["reader"]}]}
        self.calls = 0
        self.failure = False

    def snapshot(self):
        return copy.deepcopy(self.state)

    def assigned(self, change, state):
        if change.subject != "trino_reader" or change.target not in {"reader", "writer"}:
            raise AppError("INVALID_BINDING", "Unsupported", 400)
        return change.target in state["principals"][0]["roles"]

    def apply(self, change):
        self.calls += 1
        roles = self.state["principals"][0]["roles"]
        if change.granted:
            roles.append(change.target)
        else:
            roles.remove(change.target)
        if self.failure:
            raise RuntimeError("private-root-secret")


class AccessServiceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.audit = Audit(Path(temporary.name) / "audit/audit.jsonl")
        self.backend = MemoryAccess()
        self.actor = SimpleNamespace(public_id="actor-id", role="system_admin")

    def change(self, granted=True, target="writer", subject="trino_reader"):
        return BindingChange(kind="principal_role", subject=subject, target=target, granted=granted)

    def test_apply_audit_and_remove(self):
        self.assertEqual(change_binding(self.backend, self.change(), self.actor, self.audit)["status"], "done")
        self.assertEqual(change_binding(self.backend, self.change(False), self.actor, self.audit)["status"], "done")
        self.assertEqual(self.backend.calls, 2)
        self.assertEqual([row["status"] for row in self.audit.recent()], ["done", "started", "done", "started"])
        self.assertEqual(self.audit.path.stat().st_mode & 0o777, 0o600)

    def test_existing_binding_is_not_sent_again(self):
        change_binding(self.backend, self.change(True, "reader"), self.actor, self.audit)
        self.assertEqual(self.backend.calls, 0)

    def test_failure_redacts_error_and_does_not_retry(self):
        self.backend.failure = True
        with self.assertRaises(AppError) as error:
            change_binding(self.backend, self.change(), self.actor, self.audit)
        self.assertEqual(error.exception.status_code, 502)
        self.assertNotIn("private-root-secret", str(error.exception))
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual(self.audit.recent()[0]["status"], "failed")

    def test_unwritable_audit_prevents_mutation(self):
        with patch.object(self.audit, "append", side_effect=OSError("private-path")):
            with self.assertRaises(AppError) as error:
                change_binding(self.backend, self.change(), self.actor, self.audit)
        self.assertEqual(error.exception.status_code, 503)
        self.assertEqual(self.backend.calls, 0)

    def test_post_apply_journal_failure_is_not_success(self):
        append = self.audit.append

        def fail_done(record):
            if record["status"] == "done":
                raise OSError("disk failure")
            append(record)

        with patch.object(self.audit, "append", side_effect=fail_done):
            with self.assertRaises(AppError) as error:
                change_binding(self.backend, self.change(), self.actor, self.audit)
        self.assertEqual(error.exception.status_code, 503)
        self.assertEqual(self.backend.calls, 1)

    def test_unknown_subject_is_rejected_without_writing(self):
        with self.assertRaises(AppError):
            change_binding(self.backend, self.change(subject="someone_else"), self.actor, self.audit)
        self.assertEqual(self.backend.calls, 0)
        self.assertEqual(self.audit.recent()[0]["status"], "rejected")

    def test_minio_resources_are_not_accepted(self):
        with self.assertRaises(ValidationError):
            ResourceCreate(kind="user", name="lakehouse-batch")
        with self.assertRaises(ValidationError):
            ResourceCreate(kind="catalog", name="lakehouse")


class AdapterTests(unittest.TestCase):
    def test_polaris_filters_secrets_and_protected_roles(self):
        class Client:
            def call(self, method, path, missing=False):
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
        with self.assertRaises(ValidationError):
            BindingChange(kind="principal_role", subject="root", target="trino_reader_role", granted=True)

    def test_polaris_catalog_vends_credentials_through_minio_sts(self):
        sent = []

        class Client:
            def call(self, method, path, body=None):
                sent.append((method, path, body))
                return {}

        settings = AccessSettings(minio_url="http://minio:9000", _env_file=None)
        change = ResourceCreate(kind="catalog", name="lakehouse", bucket="lakehouse")
        PolarisAccess(settings, client=Client()).create(change)
        method, path, body = sent[0]
        storage = body["catalog"]["storageConfigInfo"]
        self.assertEqual((method, path), ("POST", "/api/management/v1/catalogs"))
        self.assertFalse(storage["stsUnavailable"])
        self.assertEqual(storage["stsEndpoint"], "http://minio:9000")
        self.assertEqual(storage["allowedLocations"], ["s3://lakehouse/"])

    def test_deleting_a_principal_removes_its_saved_credential(self):
        sent = []

        class Client:
            def call(self, method, path, body=None):
                sent.append((method, path))
                return {}

        with tempfile.TemporaryDirectory() as directory:
            settings = AccessSettings(credentials_dir=Path(directory), _env_file=None)
            saved = Path(directory) / "trino_reader.json"
            saved.write_text("{}")
            change = ResourceMutation(kind="principal", name="trino_reader", operation="delete", confirm_name="trino_reader")
            PolarisAccess(settings, client=Client()).mutate(change, None)
            self.assertEqual(sent, [("DELETE", "/api/management/v1/principals/trino_reader")])
            self.assertFalse(saved.exists())


class WorkloadTests(unittest.TestCase):
    def test_legacy_flink_assignment_does_not_appear_in_current_workloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            settings = ExportSettings(sources_dir=root / "sources", state_dir=root, outputs_root=root / "outputs")
            store = ExportStore(settings)
            legacy = {"flink": {"principal": "flink_landing"}}
            save_json(store.manifest, legacy)

            self.assertEqual(
                [row["id"] for row in store.snapshot()["workloads"]],
                ["batch-airflow", "trino-reader", "trino-writer"],
            )
            self.assertEqual(json.loads(store.manifest.read_text()), legacy)

    def test_both_assignment_models_reject_the_removed_workload(self):
        for model in (WorkloadAssignment, Assignment):
            with self.subTest(model=model), self.assertRaises(ValidationError):
                model(action="remove", workload="flink", confirm_remove="flink", revision="0" * 64)


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
            session.request("POST", "polaris/bindings", {})
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
                        "catalog": "lakehouse",
                        "catalog_exists": True,
                        "principals": [{"name": "trino_reader", "roles": []}],
                        "principal_roles": [{"name": "reader", "catalog_roles": []}],
                        "catalog_roles": [{"name": "read", "grants": []}],
                        "privileges": ["TABLE_READ_DATA"],
                    }
                if path == "polaris/workload-credentials":
                    return {
                        "revision": "1" * 64,
                        "workloads": [
                            {
                                "id": "batch-airflow",
                                "label": "Airflow batch",
                                "volume": "polaris-batch-airflow-credentials",
                                "configured": False,
                                "permission_ok": False,
                                "reload_hint": "Restart Airflow",
                            }
                        ],
                    }
                if path.endswith("/bindings"):
                    return {"status": "done", "operation_id": "operation"}
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
