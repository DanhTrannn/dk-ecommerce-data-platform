"""Real detached runner -> local HTTP server -> disposable PostgreSQL."""

import os
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import uvicorn

from api.source_fixture import SourceAPIFixture
from platform_app.ui.scenarios.config import PurchaseConfig, ScenarioSettings
from platform_app.ui.scenarios.store import ScenarioStore


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_SCHEMA_TESTS") == "1", "opt-in disposable PostgreSQL")
class ScenarioPostgresTests(SourceAPIFixture):
    def setUp(self):
        super().setUp()
        temporary = tempfile.TemporaryDirectory(prefix="tlcn-scenario-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        self.addCleanup(listener.close)
        port = listener.getsockname()[1]
        self.server = uvicorn.Server(uvicorn.Config(self.app, log_level="error", access_log=False, lifespan="off"))
        self.thread = threading.Thread(target=self.server.run, kwargs={"sockets": [listener]}, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        for _ in range(100):
            if self.server.started:
                break
            time.sleep(0.02)
        else:
            self.fail("Local HTTP server failed to start")
        self.settings_runner = ScenarioSettings(api_base_url=f"http://127.0.0.1:{port}", _env_file=None)

    def stop_server(self):
        self.server.should_exit = True
        self.thread.join(timeout=10)
        self.assertFalse(self.thread.is_alive())

    def execute(self, method, count=1):
        from platform_app.ui.scenarios import store as store_module

        children = []
        popen = store_module.subprocess.Popen

        def launch(*args, **kwargs):
            process = popen(*args, **kwargs)
            children.append(process)
            return process

        def cleanup():
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=10)

        self.addCleanup(cleanup)
        store = ScenarioStore(self.root)
        with patch.object(store_module.subprocess, "Popen", side_effect=launch):
            record = store.start(
                PurchaseConfig(purchases=count, interval_seconds=0, payment_method=method), self.settings_runner
            )
        self.assertEqual(children[0].wait(timeout=40), 0, store.read(record["run_id"]))
        return store.read(record["run_id"])

    def test_cod_worker_creates_verified_orders_and_ledger(self):
        before = self.scalar("SELECT COUNT(*) FROM orders")
        record = self.execute("cod", count=2)
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders"), before + 2)
        self.assertEqual(len(record["attempts"]), 2)
        self.assertNotEqual(record["attempts"][0]["customer_email"], record["attempts"][1]["customer_email"])
        for attempt in record["attempts"]:
            self.assertEqual(attempt["status"], "succeeded")
            self.assertEqual(
                self.scalar(
                    "SELECT COUNT(*) FROM orders WHERE checkout_idempotency_key=:key",
                    {"key": attempt["idempotency_key"]},
                ),
                1,
            )
            self.assertEqual(
                self.scalar(
                    "SELECT p.status FROM payments p JOIN orders o USING(order_id) WHERE o.order_number=:number",
                    {"number": attempt["order_number"]},
                ),
                "pending",
            )
        self.assert_ledger()

    def test_vietqr_worker_persists_paid_order(self):
        record = self.execute("vietqr")
        number = record["attempts"][0]["order_number"]
        self.assertEqual(
            self.scalar("SELECT status FROM orders WHERE order_number=:number", {"number": number}), "paid"
        )
        self.assertEqual(
            self.scalar(
                "SELECT p.status FROM payments p JOIN orders o USING(order_id) WHERE o.order_number=:number",
                {"number": number},
            ),
            "succeeded",
        )
        self.assert_ledger()
