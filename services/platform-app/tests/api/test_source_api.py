"""Opt-in PostgreSQL business workflows with real auth, CSRF and persistence."""

import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from sqlalchemy import event

from .source_fixture import SourceAPIFixture


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_SCHEMA_TESTS") == "1", "Enable isolated PostgreSQL tests")
class SourceAPITests(SourceAPIFixture):
    def test_admin_screen_lists_are_removed_but_write_methods_remain(self):
        paths = self.app.openapi()["paths"]
        for suffix in (
            "/admin/orders",
            "/admin/shipments",
            "/admin/shipments/{order_number}",
            "/admin/inbound/receipts",
            "/admin/returns",
            "/admin/reviews",
            "/admin/coupons",
            "/admin/store/orders",
            "/admin/store/inventory",
            "/admin/store/staff",
        ):
            self.assertNotIn("get", paths.get("/api/v1" + suffix, {}), suffix)
        for method, suffix in (
            ("put", "/cart/items/{variant_public_id}"),
            ("delete", "/cart/items/{variant_public_id}"),
            ("post", "/checkout"),
            ("post", "/admin/products"),
            ("post", "/admin/coupons"),
            ("post", "/admin/inbound/receipts"),
            ("patch", "/admin/reviews/{public_id}"),
            ("post", "/admin/store/orders/{order_number}/confirm"),
            ("post", "/admin/store/orders/{order_number}/cancel"),
        ):
            self.assertIn(method, paths["/api/v1" + suffix])

    def test_lookup_ids_support_member_pos_without_sql_discovery(self):
        cities = self.call(self.admin, "GET", "/api/v1/locations/cities")
        city = next(city for city in cities if city["code"] == "HCM")
        stores = self.call(self.admin, "GET", "/api/v1/locations/cities/HCM/stores")
        store = next(store for store in stores if store["store_id"] == self.store_id)
        self.assertEqual(store["city_id"], city["city_id"])
        self.assertEqual(self.admin.get("/api/v1/admin/customers").status_code, 422)
        self.call(self.customer, "GET", "/api/v1/admin/customers?email=customer@example.com", expected=403)
        members = self.call(self.admin, "GET", "/api/v1/admin/customers?email=CUSTOMER@EXAMPLE.COM")
        self.assertEqual(len(members), 1)
        member = members[0]
        self.assertGreater(member["customer_id"], 0)
        self.assertEqual(self.call(self.admin, "GET", "/api/v1/admin/customers?email=missing@example.com"), [])
        receipt = self.call(
            self.admin,
            "POST",
            "/api/v1/admin/inbound/receipts",
            {
                "batch_name": "Lookup scenario",
                "items": [{"variant_id": self.variant["variant_id"], "quantity": 3, "unit_cost_vnd": 100000}],
            },
            key="lookup-inbound",
            expected=201,
        )
        self.call(self.admin, "GET", f"/api/v1/admin/inbound/receipts/{receipt['receipt_code']}")
        self.call(
            self.admin,
            "POST",
            "/api/v1/admin/inventory/refill",
            {
                "store_id": store["store_id"],
                "variant_id": self.variant["variant_id"],
                "quantity": 3,
            },
            expected=204,
        )
        balances = self.call(self.admin, "GET", f"/api/v1/admin/branch-inventory?store_id={store['store_id']}")
        self.assertEqual(balances[0]["on_hand"], 3)
        sale = self.call(
            self.admin,
            "POST",
            "/api/v1/pos/transactions",
            {
                "store_id": store["store_id"],
                "customer_id": member["customer_id"],
                "items": [{"variant_id": self.variant["variant_id"], "quantity": 1}],
                "payment_method": "cash",
            },
        )
        self.assertEqual(sale["customer_id"], member["customer_id"])
        detail = self.call(self.admin, "GET", f"/api/v1/admin/orders/{sale['order_number']}")
        self.assertEqual(detail["status"], "completed")
        self.assert_ledger()

    def test_reference_writes_keep_ids_from_create_responses(self):
        from datetime import UTC, datetime, timedelta

        product = self.call(
            self.admin,
            "POST",
            "/api/v1/admin/products",
            {
                "category_code": "ao",
                "slug": "fixture-top",
                "name": "Fixture top",
                "variants": [
                    {
                        "sku": "FIXTURE-TOP-M",
                        "size_code": "M",
                        "color_code": "black",
                        "price_vnd": 100000,
                        "opening_on_hand": 2,
                    }
                ],
            },
            expected=201,
        )
        self.call(
            self.admin,
            "PATCH",
            f"/api/v1/admin/variants/{product['variants'][0]['public_id']}",
            {"price_vnd": 120000},
            expected=204,
        )
        self.call(
            self.admin,
            "PATCH",
            f"/api/v1/admin/products/{product['public_id']}",
            {"name": "Fixture renamed"},
            expected=204,
        )
        self.call(
            self.admin,
            "DELETE",
            f"/api/v1/admin/products/{product['public_id']}",
            {"reason": "Fixture archive"},
            expected=204,
        )
        now = datetime.now(UTC)
        coupon = self.call(
            self.admin,
            "POST",
            "/api/v1/admin/coupons",
            {
                "code": "FIXTURE10",
                "discount_type": "percentage",
                "discount_value": 10,
                "starts_at": (now - timedelta(days=1)).isoformat(),
                "ends_at": (now + timedelta(days=1)).isoformat(),
            },
            expected=201,
        )
        self.call(
            self.admin, "PATCH", f"/api/v1/admin/coupons/{coupon['public_id']}", {"is_active": False}, expected=204
        )
        self.call(
            self.admin,
            "DELETE",
            f"/api/v1/admin/coupons/{coupon['public_id']}",
            {"reason": "Fixture archive"},
            expected=204,
        )
        member = self.call(self.admin, "GET", "/api/v1/admin/customers?email=customer@example.com")[0]
        self.call(
            self.admin, "PATCH", f"/api/v1/admin/customers/{member['public_id']}", {"status": "inactive"}, expected=204
        )
        self.call(self.customer, "GET", "/api/v1/auth/me", expected=401)
        self.call(
            self.admin, "PATCH", f"/api/v1/admin/customers/{member['public_id']}", {"status": "active"}, expected=204
        )
        self.call(self.customer, "GET", "/api/v1/auth/me")

    def test_contract_removes_bi_only_and_keeps_source_and_http_routes(self):
        paths = self.app.openapi()["paths"]
        for suffix in (
            "/admin/overview",
            "/admin/store/dashboard",
            "/admin/analytics/overview",
            "/admin/analytics/role-metrics",
            "/admin/analytics/sales-trend",
            "/admin/analytics/stores",
        ):
            self.assertNotIn("/api/v1" + suffix, paths)
            self.assertEqual(self.admin.get("/api/v1" + suffix).status_code, 404)
        for path in (
            "/api/v1/auth/login",
            "/api/v1/checkout",
            "/api/v1/pos/transactions",
            "/api/v1/locations/cities",
            "/api/v1/admin/analytics/http-metrics",
            "/api/v1/admin/analytics/http-realtime",
        ):
            self.assertIn(path, paths)
        for suffix in ("http-metrics", "http-realtime"):
            self.assertEqual(self.customer.get("/api/v1/admin/analytics/" + suffix).status_code, 403)

    def test_auth_lookup_wishlist_and_csrf(self):
        self.call(self.customer, "GET", "/health/live")
        self.call(self.customer, "GET", "/health/ready")
        self.call(self.customer, "GET", "/api/v1/auth/me")
        found = self.call(self.customer, "GET", "/api/v1/products?q=ÁO")
        self.assertTrue(found["items"])
        self.assertTrue(all("áo" in item["name"].lower() for item in found["items"]))
        self.call(self.admin, "GET", f"/api/v1/pos/products?store_id={self.store_id}&search=ao")
        product = self.call(self.customer, "GET", f"/api/v1/products/{self.variant['slug']}")
        self.call(self.customer, "PUT", f"/api/v1/wishlist/products/{product['public_id']}", expected=204)
        self.call(self.customer, "GET", "/api/v1/wishlist")
        response = self.customer.put(f"/api/v1/cart/items/{self.variant['public_id']}", json={"quantity": 1})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM carts"), 0)

    def test_checkout_replay_cancel_and_coupon_release_are_persistent(self):
        before = self.scalar("SELECT SUM(on_hand) FROM inventory")
        result, payload, key = self.checkout("vietqr", "WELCOME10")
        number = result["order_number"]
        replay = self.call(self.customer, "POST", "/api/v1/checkout", payload, key=key)
        self.assertEqual(replay["order_number"], number)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders"), 1)
        self.assertEqual(self.scalar("SELECT SUM(on_hand) FROM inventory"), before - 2)
        for _ in range(2):
            self.call(
                self.customer,
                "POST",
                f"/api/v1/orders/{number}/cancel",
                {"reason": "Fixture cancellation"},
                key="cancel-fixture",
            )
        self.assertEqual(self.scalar("SELECT SUM(on_hand) FROM inventory"), before)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM refunds"), 1)
        self.assertEqual(self.scalar("SELECT used_count FROM coupons WHERE code_normalized='WELCOME10'"), 0)
        self.assert_ledger()

    def test_cod_delivery_review_and_return_refund(self):
        result, _, _ = self.checkout()
        number = result["order_number"]
        self.assertEqual(result["payment_status"], "pending")
        self.assertIsNone(self.scalar("SELECT paid_at FROM orders"))
        self.deliver(number)
        self.assertEqual(self.scalar("SELECT status FROM payments"), "succeeded")
        detail = self.call(self.customer, "GET", f"/api/v1/orders/{number}")
        item = detail["items"][0]
        review = self.call(
            self.customer,
            "POST",
            f"/api/v1/orders/{number}/items/{item['public_id']}/review",
            {"rating": 2, "content": "Fixture review"},
            expected=201,
        )
        self.call(self.admin, "PATCH", f"/api/v1/admin/reviews/{review['public_id']}", {"status": "approved"})
        returned = self.call(
            self.customer,
            "POST",
            f"/api/v1/orders/{number}/returns",
            {
                "customer_reason": "Fixture return",
                "bank_name": "Fixture bank",
                "bank_account_number": "00000000",
                "bank_account_holder": "Fixture holder",
                "items": [{"order_item_id": item["order_item_id"], "quantity": 2}],
            },
            key=uuid.uuid4().hex,
        )
        code = returned["return_code"]
        self.call(self.admin, "POST", f"/api/v1/admin/returns/{code}/review", {"action": "approved"})
        self.call(self.admin, "POST", f"/api/v1/admin/returns/{code}/receive")
        inspection = {
            "items": [{"return_item_id": returned["items"][0]["return_item_id"], "inspection_status": "passed"}]
        }
        for _ in range(2):
            self.call(
                self.admin,
                "POST",
                f"/api/v1/admin/returns/{code}/inspect-and-resolve",
                inspection,
                key="return-fixture",
            )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM refunds"), 1)
        self.assertEqual(self.scalar("SELECT amount_vnd FROM refunds"), result["subtotal_vnd"])
        self.assert_ledger()

    def test_failed_cod_delivery_restores_inventory_without_refund(self):
        before = self.scalar("SELECT SUM(on_hand) FROM inventory")
        result, _, _ = self.checkout()
        number = result["order_number"]
        staff = self.call(
            self.admin,
            "POST",
            "/api/v1/admin/delivery-staff",
            {"full_name": "Fixture courier", "phone": "0000000000"},
            expected=201,
        )
        self.call(
            self.admin,
            "POST",
            f"/api/v1/admin/orders/{number}/dispatch",
            {"delivery_staff_id": staff["staff_id"]},
            key="dispatch-fixture",
        )
        for attempt in range(3):
            self.call(
                self.admin,
                "POST",
                f"/api/v1/admin/orders/{number}/failed-delivery",
                {"reason": "buyer_refused"},
                key=f"failure-{attempt}",
            )
        self.assertEqual(self.scalar("SELECT status FROM orders"), "failed_delivery")
        self.assertIsNone(self.scalar("SELECT paid_at FROM orders"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM refunds"), 0)
        self.assertEqual(self.scalar("SELECT SUM(on_hand) FROM inventory"), before)
        self.assert_ledger()

    def test_inbound_transfer_adjustment_and_pos_reconcile(self):
        variant = self.variant["variant_id"]
        before = self.scalar("SELECT on_hand FROM inventory WHERE variant_id=:id", {"id": variant})
        payload = {
            "batch_name": "Fixture inbound",
            "items": [{"variant_id": variant, "quantity": 10, "unit_cost_vnd": 100000}],
        }
        for _ in range(2):
            self.call(
                self.admin, "POST", "/api/v1/admin/inbound/receipts", payload, key="inbound-fixture", expected=201
            )
        self.assertEqual(
            self.scalar("SELECT on_hand FROM inventory WHERE variant_id=:id", {"id": variant}), before + 10
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM inbound_receipts"), 1)
        self.call(
            self.admin,
            "POST",
            "/api/v1/admin/inventory/refill",
            {"store_id": self.store_id, "variant_id": variant, "quantity": 5},
            expected=204,
        )
        for _ in range(2):
            self.call(
                self.admin,
                "PATCH",
                "/api/v1/admin/branch-inventory",
                {"store_id": self.store_id, "variant_id": variant, "on_hand": 7},
                expected=204,
            )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM inventory_transactions WHERE movement_type='adjustment'"), 1)
        self.call(
            self.admin,
            "POST",
            "/api/v1/pos/transactions",
            {"store_id": self.store_id, "items": [{"variant_id": variant, "quantity": 2}], "payment_method": "cash"},
        )
        self.assertEqual(self.scalar("SELECT on_hand FROM store_inventory"), 5)
        self.assertEqual(self.scalar("SELECT opening_on_hand FROM store_inventory"), 0)
        self.assert_ledger()

    def test_transfer_failure_rolls_back_balance_version_and_both_ledger_sides(self):
        from platform_app.api.modules.admin import branch_inventory_service

        before = self.scalar("SELECT SUM(on_hand) FROM inventory")

        def reject_ledger(mapper, connection, target):
            raise ValueError("Fixture ledger failure")

        event.listen(branch_inventory_service.InventoryTransaction, "before_insert", reject_ledger)
        try:
            with self.assertLogs("ecommerce_api.error", level="ERROR"):
                with self.assertRaisesRegex(ValueError, "Fixture ledger failure"):
                    self.call(
                        self.admin,
                        "POST",
                        "/api/v1/admin/inventory/refill",
                        {"store_id": self.store_id, "variant_id": self.variant["variant_id"], "quantity": 5},
                        expected=204,
                    )
        finally:
            event.remove(branch_inventory_service.InventoryTransaction, "before_insert", reject_ledger)
        self.assertEqual(self.scalar("SELECT SUM(on_hand) FROM inventory"), before)
        self.assertEqual(self.scalar("SELECT SUM(version) FROM inventory"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM store_inventory"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM inventory_transactions"), 0)

    def test_concurrent_checkouts_cannot_oversell_the_last_unit(self):
        from sqlalchemy import text

        other = self.client()
        self.call(
            other,
            "POST",
            "/api/v1/auth/register",
            {
                "email": "other@example.com",
                "password": self.password,
                "display_name": "Concurrent fixture",
            },
            expected=201,
        )
        with self.engine.begin() as connection:
            connection.execute(
                text("UPDATE inventory SET opening_on_hand=1,on_hand=1 WHERE variant_id=:id"),
                {"id": self.variant["variant_id"]},
            )
        for client in (self.customer, other):
            self.call(client, "PUT", f"/api/v1/cart/items/{self.variant['public_id']}", {"quantity": 1})
        barrier = Barrier(2)

        def buy(client):
            barrier.wait(timeout=5)
            return client.post(
                "/api/v1/checkout",
                json={
                    "receiver_name": "Fixture",
                    "receiver_phone": "000",
                    "shipping_address_text": "Fixture",
                    "payment_method": "cod",
                },
                headers={
                    "X-CSRF-Token": client.cookies[self.settings.csrf_cookie_name],
                    "Idempotency-Key": uuid.uuid4().hex,
                },
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(buy, (self.customer, other)))
        self.assertEqual(
            sorted(response.status_code for response in responses), [200, 409], [r.text for r in responses]
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM payments"), 1)
        self.assert_ledger()

    def test_real_postgres_serialization_state_retries_with_a_fresh_transaction(self):
        from sqlalchemy import text
        from platform_app.api.db.uow import run_in_transaction

        before = self.scalar("SELECT COUNT(*) FROM customers")
        attempts = []

        def work(session):
            attempts.append(session)
            session.execute(
                text("INSERT INTO customers(public_id,display_name) VALUES(:id,'Retry fixture')"),
                {"id": str(uuid.uuid4())},
            )
            if len(attempts) == 1:
                session.execute(
                    text("DO $$ BEGIN RAISE EXCEPTION 'fixture serialization abort' USING ERRCODE='40001'; END $$;")
                )
            return "committed"

        self.assertEqual(run_in_transaction(work), "committed")
        self.assertEqual(len(attempts), 2)
        self.assertIsNot(attempts[0], attempts[1])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM customers"), before + 1)

    def test_insufficient_stock_and_store_permissions_leave_no_writes(self):
        variant = self.variant["variant_id"]
        self.call(
            self.customer,
            "POST",
            "/api/v1/admin/inventory/refill",
            {"store_id": self.store_id, "variant_id": variant, "quantity": 1},
            expected=403,
        )
        self.call(
            self.admin,
            "POST",
            "/api/v1/admin/inventory/refill",
            {"store_id": self.store_id, "variant_id": variant, "quantity": 1000000},
            expected=409,
        )
        for quantity in (0, -1):
            self.call(
                self.admin,
                "POST",
                "/api/v1/admin/inventory/refill",
                {"store_id": self.store_id, "variant_id": variant, "quantity": quantity},
                expected=422,
            )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM inventory_transactions"), 0)

    def test_store_manager_cannot_write_another_store_and_store_posts_require_csrf(self):
        from sqlalchemy import text

        assigned = self.scalar("SELECT MAX(store_id) FROM stores")
        with self.engine.begin() as connection:
            connection.execute(
                text("""UPDATE customers SET role='store_manager',store_id=:store
                WHERE customer_id=(SELECT customer_id FROM customer_credentials WHERE email_normalized='customer@example.com')"""),
                {"store": assigned},
            )
        self.call(
            self.customer,
            "PATCH",
            "/api/v1/admin/branch-inventory",
            {
                "store_id": self.store_id,
                "variant_id": self.variant["variant_id"],
                "on_hand": 1,
            },
            expected=403,
        )
        self.call(self.customer, "GET", f"/api/v1/admin/store/orders/fixture?store_id={self.store_id}", expected=403)
        self.call(
            self.customer,
            "POST",
            "/api/v1/pos/transactions",
            {
                "store_id": self.store_id,
                "items": [{"variant_id": self.variant["variant_id"], "quantity": 1}],
                "payment_method": "cash",
            },
            expected=403,
        )
        for action in ("confirm", "cancel"):
            response = self.customer.post(f"/api/v1/admin/store/orders/fixture/{action}", json={"reason": "Fixture"})
            self.assertEqual(response.status_code, 403)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM inventory_transactions"), 0)
