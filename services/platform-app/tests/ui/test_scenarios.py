import json
import os
import random
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import httpx
from pydantic import ValidationError
from streamlit.testing.v1 import AppTest

from platform_app.ui.scenarios.config import PurchaseConfig, ScenarioSettings
from platform_app.ui.scenarios.flows import SCENARIOS
from platform_app.ui.scenarios.store import ScenarioStore, ScenarioStoreError
from platform_app.ui.scenarios.worker import run


class PurchaseServer:
    """Fake API that follows the requests it receives, enough for every console scenario."""

    def __init__(
        self,
        fail=None,
        products=("product",),
        staff=({"staff_id": 7, "is_active": True},),
        store_stock=3,
        stocked_skus=None,
    ):
        self.calls = []
        self.fail = fail
        self.products = products
        self.staff = list(staff)
        self.checked_out = False
        self.line = None
        self.payment_method = None
        self.store_stock = store_stock
        self.failed_attempts = 0
        self.stocked_skus = stocked_skus

    def handle(self, request):
        path = request.url.path
        self.calls.append(request)
        if self.fail:
            response = self.fail(request)
            if response is not None:
                return response
        body = json.loads(request.content) if request.content else {}
        paid = self.payment_method == "vietqr"
        if path.endswith("/auth/register") or path.endswith("/auth/login"):
            return httpx.Response(
                201 if path.endswith("register") else 200, json={}, headers={"set-cookie": "web_csrf=csrf; Path=/"}
            )
        if path == "/api/v1/products":
            data = {"items": [{"slug": slug} for slug in self.products]}
        elif path.startswith("/api/v1/products/"):
            slug = path.rsplit("/", 1)[1]
            data = {
                "variants": [{"public_id": f"v-{slug}", "sku": f"SKU-{slug}", "in_stock": True, "stock_quantity": 5}]
            }
        elif path.startswith("/api/v1/cart/items/"):
            variant = path.rsplit("/", 1)[1]
            self.line = {"variant_public_id": variant, "sku": "SKU-" + variant[2:], "quantity": body["quantity"]}
            data = {"items": [self.line]}
        elif path == "/api/v1/checkout/quote":
            data = {"total_vnd": 100000}
        elif path == "/api/v1/checkout":
            self.checked_out = True
            self.payment_method = body["payment_method"]
            status = "paid" if self.payment_method == "vietqr" else "confirmed"
            data = {"order_number": "ORDER", "total_vnd": 100000, "status": status}
        elif path == "/api/v1/orders/ORDER":
            data = {
                "order_number": "ORDER",
                "total_vnd": 100000,
                "status": "paid" if paid else "confirmed",
                "items": [{**self.line, "order_item_id": 11, "public_id": "item-1"}],
                "payment": {"status": "succeeded" if paid else "pending", "amount_vnd": 100000},
            }
        elif path == "/api/v1/cart":
            data = {"items": [] if self.checked_out else [self.line]}
        elif path == "/api/v1/admin/delivery-staff":
            data = self.staff
        elif path == "/api/v1/admin/orders/ORDER/failed-delivery":
            # The API keeps the order shipping until the third failed attempt.
            self.failed_attempts += 1
            data = {"order_number": "ORDER", "status": "failed_delivery" if self.failed_attempts == 3 else "shipping"}
        elif path.startswith("/api/v1/admin/orders/ORDER/"):
            status = {"dispatch": "shipping", "deliver": "delivered"}[path.rsplit("/", 1)[1]]
            data = {"order_number": "ORDER", "status": status}
        elif path in ("/api/v1/orders/ORDER/complete", "/api/v1/orders/ORDER/cancel"):
            data = {"order_number": "ORDER", "status": "completed" if path.endswith("complete") else "cancelled"}
        elif path == "/api/v1/orders/ORDER/items/item-1/review":
            data = {"public_id": "review", "rating": body["rating"], "status": "approved"}
        elif path == "/api/v1/orders/ORDER/returns":
            item = body["items"][0]
            data = {"return_code": "RET1", "status": "pending_review", "items": [{**item, "return_item_id": 21}]}
        elif path.startswith("/api/v1/admin/returns/RET1/"):
            action = path.rsplit("/", 1)[1]
            status = {"review": "approved", "receive": "goods_received", "inspect-and-resolve": "completed"}[action]
            data = {"return_code": "RET1", "status": status}
        elif path == "/api/v1/admin/coupons":
            return httpx.Response(201, json={"code": body["code"], "is_active": True})
        elif path == "/api/v1/locations/cities":
            data = [{"code": "HCM", "store_count": 2}, {"code": "EMPTY", "store_count": 0}]
        elif path == "/api/v1/locations/cities/HCM/stores":
            data = [{"store_id": 1}, {"store_id": 2}]
        elif path == "/api/v1/pos/products":
            sku = request.url.params["search"]
            stock = self.store_stock if self.stocked_skus is None or sku in self.stocked_skus else 0
            data = [{"variant_id": 5, "sku": sku, "price_vnd": 250000, "store_stock": stock}]
        elif path == "/api/v1/pos/transactions":
            data = {"order_number": "POS1", "status": "completed", "total_vnd": 250000}
        else:
            raise AssertionError(path)
        return httpx.Response(200, json=data)

    def client(self):
        return httpx.Client(base_url="http://scenario.test", transport=httpx.MockTransport(self.handle))

    def after_checkout(self):
        index = next(i for i, req in enumerate(self.calls) if req.url.path == "/api/v1/checkout")
        skip = {"/api/v1/orders/ORDER", "/api/v1/cart"}
        return [(req.method, req.url.path[7:]) for req in self.calls[index + 1 :] if req.url.path not in skip]


class ScenarioTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = ScenarioStore(self.root, launcher=lambda *_: os.getpid(), alive=lambda *_: True)
        self.settings = ScenarioSettings(
            api_base_url="http://scenario.test",
            admin_email="sim@web.local",
            admin_password="Sim@12345",
            _env_file=None,
        )

    def execute(self, server=None, count=1, scenario="purchase"):
        server = server or PurchaseServer()
        config = PurchaseConfig(purchases=count, interval_seconds=0, scenario=scenario)
        record = self.store.start(config, self.settings)
        run(self.root, record["run_id"], server.client, "web_csrf", random.Random(1), self.settings)
        return self.store.read(record["run_id"]), server

    def key(self, record, step):
        return f"sim-{record['run_id']}-1-{step}"

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

    def test_purchase_picks_random_in_stock_product(self):
        record, _ = self.execute(PurchaseServer(products=("a", "b", "c")), count=6)
        self.assertEqual(record["status"], "succeeded")
        self.assertGreater(len({row["sku"] for row in record["attempts"]}), 1)

    def test_delivered_runs_dispatch_deliver_complete_in_order(self):
        record, server = self.execute(scenario="delivered")
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(
            server.after_checkout(),
            [
                ("GET", "/admin/delivery-staff"),
                ("POST", "/admin/orders/ORDER/dispatch"),
                ("POST", "/admin/orders/ORDER/deliver"),
                ("POST", "/orders/ORDER/complete"),
            ],
        )
        keys = [
            req.headers["Idempotency-Key"] for req in server.calls if req.method == "POST" and "ORDER/" in str(req.url)
        ]
        self.assertEqual(keys, [self.key(record, step) for step in ("disp", "dlvr", "cmpl")])
        dispatch = next(req for req in server.calls if req.url.path.endswith("/dispatch"))
        self.assertEqual(json.loads(dispatch.content), {"delivery_staff_id": 7})

    def test_failed_delivery(self):
        record, server = self.execute(scenario="failed_delivery")
        self.assertEqual(record["status"], "succeeded")
        failed = [req for req in server.calls if req.url.path.endswith("/failed-delivery")]
        self.assertEqual(
            [req.headers["Idempotency-Key"] for req in failed], [self.key(record, f"fail{n}") for n in (1, 2, 3)]
        )
        self.assertTrue(all(json.loads(req.content)["reason"] for req in failed))

    def test_cancelled_uses_vietqr_and_customer_cancel(self):
        record, server = self.execute(scenario="cancelled")
        self.assertEqual(record["status"], "succeeded")
        checkout = next(req for req in server.calls if req.url.path == "/api/v1/checkout")
        self.assertEqual(json.loads(checkout.content)["payment_method"], "vietqr")
        self.assertEqual(server.after_checkout(), [("POST", "/orders/ORDER/cancel")])

    def test_review_after_completion(self):
        record, server = self.execute(scenario="review")
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(
            server.after_checkout()[-2:],
            [("POST", "/orders/ORDER/complete"), ("POST", "/orders/ORDER/items/item-1/review")],
        )
        review = next(req for req in server.calls if req.url.path.endswith("/review"))
        self.assertIn(json.loads(review.content)["rating"], (3, 4, 5))

    def test_return_flow(self):
        record, server = self.execute(scenario="return")
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(record["attempts"][0]["return_code"], "RET1")
        self.assertEqual(
            server.after_checkout()[-4:],
            [
                ("POST", "/orders/ORDER/returns"),
                ("POST", "/admin/returns/RET1/review"),
                ("POST", "/admin/returns/RET1/receive"),
                ("POST", "/admin/returns/RET1/inspect-and-resolve"),
            ],
        )
        created = next(req for req in server.calls if req.url.path.endswith("/ORDER/returns"))
        self.assertEqual(json.loads(created.content)["items"], [{"order_item_id": 11, "quantity": 1}])
        self.assertEqual(created.headers["Idempotency-Key"], self.key(record, "ret"))
        review = next(req for req in server.calls if req.url.path.endswith("/RET1/review"))
        self.assertEqual(json.loads(review.content)["action"], "approved")
        inspect = next(req for req in server.calls if req.url.path.endswith("/inspect-and-resolve"))
        self.assertEqual(json.loads(inspect.content)["items"], [{"return_item_id": 21, "inspection_status": "passed"}])
        self.assertEqual(inspect.headers["Idempotency-Key"], self.key(record, "rinsp"))

    def test_coupon_created_once_per_run_and_applied(self):
        record, server = self.execute(scenario="coupon", count=3)
        self.assertEqual(record["status"], "succeeded")
        created = [req for req in server.calls if req.url.path == "/api/v1/admin/coupons"]
        self.assertEqual(len(created), 1)
        coupon = json.loads(created[0].content)
        self.assertEqual(coupon["code"], "SIM-" + record["run_id"][:8].upper())
        self.assertEqual((coupon["discount_type"], coupon["discount_value"]), ("percentage", 10))
        checkouts = [json.loads(req.content) for req in server.calls if req.url.path == "/api/v1/checkout"]
        self.assertEqual([body["coupon_code"] for body in checkouts], [coupon["code"]] * 3)
        self.assertEqual({row["coupon_code"] for row in record["attempts"]}, {coupon["code"]})

    def test_pos_sells_from_a_random_store_with_stock(self):
        record, server = self.execute(scenario="pos")
        self.assertEqual(record["status"], "succeeded")
        self.assertFalse(any(req.url.path.endswith("/auth/register") for req in server.calls))
        sale = next(req for req in server.calls if req.url.path == "/api/v1/pos/transactions")
        body = json.loads(sale.content)
        self.assertIn(body["store_id"], (1, 2))
        self.assertEqual(body["items"], [{"variant_id": 5, "quantity": 1}])
        self.assertEqual(body["payment_method"], "cash")
        self.assertGreaterEqual(body["amount_received_vnd"], 250000)
        self.assertEqual(record["attempts"][0]["order_number"], "POS1")

    def test_pos_tries_another_sku_when_no_store_has_the_first(self):
        server = PurchaseServer(products=("a", "b", "c"), stocked_skus={"SKU-c"})
        record, server = self.execute(server, scenario="pos")
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(record["attempts"][0]["sku"], "SKU-c")

    def test_pos_store_without_stock_fails_before_write(self):
        record, server = self.execute(PurchaseServer(store_stock=0), scenario="pos")
        self.assertEqual(record["attempts"][0]["status"], "failed")
        self.assertFalse(any(req.url.path == "/api/v1/pos/transactions" for req in server.calls))

    def test_admin_login_failure_stops_before_customer_writes(self):
        def deny_admin(request):
            if request.url.path.endswith("/auth/login") and json.loads(request.content)["email"] == "sim@web.local":
                return httpx.Response(401, json={})

        record, server = self.execute(PurchaseServer(deny_admin), scenario="delivered")
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["attempts"], [])
        self.assertFalse(any(req.url.path.endswith("/auth/register") for req in server.calls))

    def test_no_active_delivery_staff_fails_before_dispatch(self):
        record, server = self.execute(
            PurchaseServer(staff=({"staff_id": 7, "is_active": False},)), scenario="delivered"
        )
        self.assertEqual(record["attempts"][0]["status"], "failed")
        self.assertFalse(any(req.url.path.endswith("/dispatch") for req in server.calls))

    def test_unexpected_status_after_deliver_is_unknown(self):
        def stale(request):
            if request.url.path.endswith("/deliver"):
                return httpx.Response(200, json={"order_number": "ORDER", "status": "confirmed"})

        record, _ = self.execute(PurchaseServer(stale), count=2, scenario="delivered")
        self.assertEqual(record["status"], "unknown")
        self.assertEqual(len(record["attempts"]), 1)

    def test_stop_before_first_attempt(self):
        record = self.store.start(PurchaseConfig(), self.settings)
        self.store.stop(record["run_id"])
        server = PurchaseServer()
        run(self.root, record["run_id"], server.client, "web_csrf", random.Random(1), self.settings)
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
            except ScenarioStoreError:
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
            with self.assertRaises(ScenarioStoreError):
                self.store.start(PurchaseConfig(), self.settings)
        self.assertEqual(self.store.history()[0]["status"], "failed")
        self.assertNotIn("private", json.dumps(self.store.history()))

    def test_admin_scenario_refused_without_admin_settings(self):
        launches = []
        store = ScenarioStore(self.root, launcher=lambda *_: launches.append(1) or os.getpid(), alive=lambda *_: True)
        with self.assertRaises(ScenarioStoreError) as raised:
            store.start(PurchaseConfig(scenario="delivered"), ScenarioSettings(_env_file=None))
        self.assertIn("SCENARIO_ADMIN_EMAIL", str(raised.exception))
        self.assertEqual(launches, [])
        self.assertEqual(store.history(), [])

    def test_config_rejects_credentials_and_invalid_inputs(self):
        for url in ("http://user:secret@host", "file:///tmp", "http://host/path", "http://host?secret=a"):
            with self.assertRaises(ValidationError):
                ScenarioSettings(api_base_url=url, _env_file=None)
        for value in (0, 1001):
            with self.assertRaises(ValidationError):
                PurchaseConfig(purchases=value)

    def test_ui_reports_unwritable_scenarios_dir(self):
        app = AppTest.from_string("from platform_app.ui.views.scenarios import render\nrender()")
        with patch.object(ScenarioStore, "reconcile", side_effect=PermissionError("denied")):
            app.run()
        self.assertEqual(len(app.error), 1)
        self.assertIn("/data/scenarios", app.error[0].value)
        self.assertNotIn("SCENARIO_API_BASE_URL", app.error[0].value)

    def test_ui_scenario_picker(self):
        app = AppTest.from_string("from platform_app.ui.views.scenarios import render\nrender()")
        env = {
            "PLATFORM_APP_SCENARIOS_DIR": str(self.root / "scenarios"),
            "SCENARIO_ADMIN_EMAIL": "sim@web.local",
            "SCENARIO_ADMIN_PASSWORD": "Sim@12345",
        }
        with (
            patch.dict(os.environ, env),
            patch("platform_app.ui.scenarios.store._launch", return_value=os.getpid()),
            patch("platform_app.ui.scenarios.store._alive", return_value=True),
        ):
            app.run()
            picker = next(box for box in app.selectbox if box.label == "Kịch bản")
            self.assertEqual(list(picker.options), [label for label, _, _ in SCENARIOS.values()])
            picker.select("pos").run()
            self.assertFalse(any(box.label == "Thanh toán" for box in app.selectbox))
            next(button for button in app.button if button.label == "Bắt đầu mô phỏng").click().run()
            self.assertFalse(app.exception)
            record = ScenarioStore(self.root / "scenarios").history()[0]
            self.assertEqual(record["config"]["scenario"], "pos")
            self.assertTrue(any("Bán tại quầy" in text.value for text in app.text))

    def test_ui_explains_missing_scenario_admin(self):
        app = AppTest.from_string("from platform_app.ui.views.scenarios import render\nrender()")
        env = {"PLATFORM_APP_SCENARIOS_DIR": str(self.root / "scenarios"), "SCENARIO_ADMIN_EMAIL": ""}
        with patch.dict(os.environ, env), patch("platform_app.ui.scenarios.store._launch", return_value=os.getpid()):
            app.run()
            next(box for box in app.selectbox if box.label == "Kịch bản").select("delivered").run()
            next(button for button in app.button if button.label == "Bắt đầu mô phỏng").click().run()
        self.assertTrue(any("SCENARIO_ADMIN_EMAIL" in error.value for error in app.error))

    def test_ui_submit_once_and_stop(self):
        app = AppTest.from_string("from platform_app.ui.views.scenarios import render\nrender()")
        with (
            patch.dict(os.environ, {"PLATFORM_APP_SCENARIOS_DIR": str(self.root / "scenarios")}),
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
