"""Scenario contract/failure handling without external services or credentials."""

import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import httpx
from pydantic import ValidationError
from streamlit.testing.v1 import AppTest

from platform_app.ui.job_store import JobStoreError
from platform_app.ui.scenarios.config import PurchaseConfig, ScenarioSettings
from platform_app.ui.scenarios.store import ScenarioStore
from platform_app.ui.scenarios.worker import run


class PurchaseServer:
    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail
        self.checked_out = False

    def handle(self, request):
        path = request.url.path
        self.calls.append(request)
        if self.fail:
            response = self.fail(request)
            if response is not None:
                return response
        item = {"variant_public_id": "variant", "sku": "SKU", "quantity": 1}
        if path.endswith("/auth/register") or path.endswith("/auth/login"):
            return httpx.Response(
                201 if path.endswith("register") else 200, json={}, headers={"set-cookie": "web_csrf=csrf; Path=/"}
            )
        if path == "/api/v1/products":
            data = {"items": [{"slug": "product"}]}
        elif path == "/api/v1/products/product":
            data = {"variants": [{"public_id": "variant", "sku": "SKU", "in_stock": True, "stock_quantity": 5}]}
        elif path.startswith("/api/v1/cart/items/"):
            data = {"items": [item]}
        elif path == "/api/v1/checkout/quote":
            data = {"total_vnd": 100000}
        elif path == "/api/v1/checkout":
            self.checked_out = True
            data = {"order_number": "ORDER", "total_vnd": 100000, "status": "confirmed"}
        elif path == "/api/v1/orders/ORDER":
            data = {
                "order_number": "ORDER",
                "total_vnd": 100000,
                "status": "confirmed",
                "items": [item],
                "payment": {"status": "pending", "amount_vnd": 100000},
            }
        elif path == "/api/v1/cart":
            data = {"items": [] if self.checked_out else [item]}
        else:
            raise AssertionError(path)
        return httpx.Response(200, json=data)

    def client(self):
        return httpx.Client(base_url="http://scenario.test", transport=httpx.MockTransport(self.handle))


class ScenarioTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = ScenarioStore(self.root, launcher=lambda *_: os.getpid(), alive=lambda *_: True)
        self.settings = ScenarioSettings(api_base_url="http://scenario.test", _env_file=None)

    def execute(self, server=None, count=1):
        server = server or PurchaseServer()
        record = self.store.start(PurchaseConfig(purchases=count, interval_seconds=0), self.settings)
        run(self.root, record["run_id"], server.client, csrf_cookie="web_csrf")
        return self.store.read(record["run_id"]), server

    def test_success_csrf_idempotency_and_no_credentials_in_journal(self):
        record, server = self.execute()
        self.assertEqual(record["status"], "succeeded")
        attempt = record["attempts"][0]
        checkout = next(req for req in server.calls if req.url.path == "/api/v1/checkout")
        self.assertEqual(checkout.headers["Idempotency-Key"], attempt["idempotency_key"])
        self.assertEqual(checkout.headers["X-CSRF-Token"], "csrf")
        self.assertEqual(attempt["stage"], "verified")
        content = self.store.path(record["run_id"]).read_text()
        password = json.loads(server.calls[0].content)["password"]
        self.assertNotIn(password, content)
        self.assertNotIn("web_csrf", content)
        self.assertNotIn("password", content)

    def test_no_replay_after_checkout_timeout(self):
        def fail(request):
            if request.url.path == "/api/v1/checkout":
                raise httpx.ReadTimeout("secret payload", request=request)

        record, server = self.execute(PurchaseServer(fail), count=3)
        self.assertEqual(record["status"], "unknown")
        self.assertEqual(len(record["attempts"]), 1)
        self.assertEqual(sum(req.url.path == "/api/v1/checkout" for req in server.calls), 1)
        self.assertNotIn("secret payload", json.dumps(record))

    def test_5xx_unknown_and_response_body_not_saved(self):
        record, _ = self.execute(
            PurchaseServer(
                lambda req: httpx.Response(503, text="private") if req.url.path == "/api/v1/checkout" else None
            )
        )
        self.assertEqual(record["status"], "unknown")
        self.assertNotIn("private", json.dumps(record))

    def test_business_rejection_does_not_retry(self):
        record, server = self.execute(
            PurchaseServer(lambda req: httpx.Response(409, json={}) if req.url.path == "/api/v1/checkout" else None)
        )
        self.assertEqual(record["status"], "failed")
        self.assertEqual(sum(req.url.path == "/api/v1/checkout" for req in server.calls), 1)

    def test_failed_verification_preserves_order_and_stops(self):
        record, _ = self.execute(
            PurchaseServer(lambda req: httpx.Response(500) if req.url.path.startswith("/api/v1/orders/") else None),
            count=2,
        )
        self.assertEqual(record["status"], "unknown")
        self.assertEqual(record["attempts"][0]["order_number"], "ORDER")
        self.assertEqual(len(record["attempts"]), 1)

    def test_no_stock_never_checkouts(self):
        record, server = self.execute(
            PurchaseServer(
                lambda req: httpx.Response(200, json={"items": []}) if req.url.path == "/api/v1/products" else None
            )
        )
        self.assertEqual(record["status"], "failed")
        self.assertFalse(any(req.url.path == "/api/v1/checkout" for req in server.calls))

    def test_stop_before_first_attempt(self):
        record = self.store.start(PurchaseConfig(), self.settings)
        self.store.stop(record["run_id"])
        server = PurchaseServer()
        run(self.root, record["run_id"], server.client, csrf_cookie="web_csrf")
        self.assertEqual(self.store.read(record["run_id"])["status"], "stopped")
        self.assertEqual(server.calls, [])

    def test_stop_during_attempt_finishes_current_only(self):
        def stop(request):
            if request.url.path == "/api/v1/checkout":
                self.store.stop(self.store.history()[0]["run_id"])

        record, _ = self.execute(PurchaseServer(stop), count=3)
        self.assertEqual(record["status"], "stopped")
        self.assertEqual([row["status"] for row in record["attempts"]], ["succeeded"])

    def test_double_start_launches_one_worker(self):
        def start(_):
            try:
                return self.store.start(PurchaseConfig(), self.settings)
            except JobStoreError:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(start, range(2)))
        self.assertEqual(sum(row is not None for row in results), 1)

    def test_lost_worker_is_unknown(self):
        record = self.store.start(PurchaseConfig(), self.settings)
        ScenarioStore(self.root, alive=lambda *_: False).reconcile()
        self.assertEqual(self.store.read(record["run_id"])["status"], "unknown")

    def test_launch_failure_is_durable(self):
        with patch.object(self.store, "launcher", side_effect=OSError("private")):
            with self.assertRaises(JobStoreError):
                self.store.start(PurchaseConfig(), self.settings)
        self.assertEqual(self.store.history()[0]["status"], "failed")
        self.assertNotIn("private", json.dumps(self.store.history()))

    def test_config_rejects_credentials_and_invalid_inputs(self):
        for url in ("http://user:secret@host", "file:///tmp", "http://host/path", "http://host?secret=a"):
            with self.assertRaises(ValidationError):
                ScenarioSettings(api_base_url=url, _env_file=None)
        for value in (0, 1001):
            with self.assertRaises(ValidationError):
                PurchaseConfig(purchases=value)

    def test_ui_submit_once_and_stop(self):
        app = AppTest.from_string("from platform_app.ui.views.scenarios import render\nrender()")
        with (
            patch.dict(os.environ, {"GENERATOR_CONSOLE_DATA_DIR": str(self.root / "jobs")}),
            patch("platform_app.ui.scenarios.store._launch", return_value=os.getpid()),
            patch("platform_app.ui.scenarios.store._alive", return_value=True),
        ):
            app.run()
            next(button for button in app.button if button.label == "Bắt đầu mô phỏng").click().run()
            self.assertFalse(app.exception)
            self.assertTrue(next(button for button in app.button if button.label == "Bắt đầu mô phỏng").disabled)
            next(button for button in app.button if button.label == "Yêu cầu dừng").click().run()
            store = ScenarioStore(self.root / "scenarios")
            self.assertEqual(len(store.history()), 1)
            self.assertTrue(store.history()[0]["stop_requested"])
