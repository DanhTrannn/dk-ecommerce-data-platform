"""HTTP steps shared by console scenarios, called only through the public API."""

import random
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import quote

import httpx

from platform_app.ui.scenarios.config import PurchaseConfig


class ScenarioFailure(Exception):
    def __init__(self, message, *, unknown=False):
        super().__init__(message)
        self.unknown = unknown


def request(client, method, path, payload=None, *, key=None, csrf_cookie="web_csrf"):
    headers = {}
    if method not in {"GET", "HEAD"} and (csrf := client.cookies.get(csrf_cookie)):
        headers["X-CSRF-Token"] = csrf
    if key:
        headers["Idempotency-Key"] = key
    try:
        response = client.request(method, "/api/v1" + path, json=payload, headers=headers)
    except httpx.HTTPError:
        raise ScenarioFailure(f"{method} {path}: lỗi kết nối; không tự gửi lại.", unknown=method != "GET") from None
    if not 200 <= response.status_code < 300:
        raise ScenarioFailure(
            f"{method} {path}: HTTP {response.status_code}; không tự gửi lại.",
            unknown=method != "GET" and (response.status_code >= 500 or response.status_code == 408),
        )
    try:
        return response.json()
    except ValueError:
        raise ScenarioFailure(f"{method} {path}: phản hồi không hợp lệ.", unknown=method != "GET") from None


@dataclass
class Attempt:
    """One scenario attempt: a fresh customer client plus the run's shared admin client."""

    customer: httpx.Client
    admin: httpx.Client | None
    config: PurchaseConfig
    record: dict
    checkpoint: Callable
    rng: random.Random
    run_state: dict
    csrf_cookie: str

    def call(self, method, path, payload=None, *, key=None):
        self.checkpoint(stage=path)
        return request(self.customer, method, path, payload, key=key, csrf_cookie=self.csrf_cookie)

    def admin_call(self, method, path, payload=None, *, key=None):
        self.checkpoint(stage="admin " + path)
        return request(self.admin, method, path, payload, key=key, csrf_cookie=self.csrf_cookie)

    def key(self, step):
        return f"{self.record['idempotency_key']}-{step}"


def expect(result, status, action):
    # A write already happened, so a wrong state needs reconciliation, never a resend.
    if result.get("status") != status:
        raise ScenarioFailure(f"{action}: trạng thái {result.get('status')}, cần {status}; cần đối soát.", unknown=True)
    return result


def admin_login(client, email, password, csrf_cookie):
    request(client, "POST", "/auth/login", {"email": email, "password": password}, csrf_cookie=csrf_cookie)
    if not client.cookies.get(csrf_cookie):
        raise ScenarioFailure("Admin không nhận được CSRF cookie; kiểm tra tài khoản SCENARIO_ADMIN_EMAIL.")


def register_and_login(ctx: Attempt):
    # Credentials and cookies never enter the journal.
    password = secrets.token_urlsafe(32)
    email = ctx.record["customer_email"]
    ctx.call("POST", "/auth/register", {"email": email, "password": password, "display_name": ctx.config.receiver_name})
    ctx.call("POST", "/auth/login", {"email": email, "password": password})
    if not ctx.customer.cookies.get(ctx.csrf_cookie):
        raise ScenarioFailure("Không nhận được CSRF cookie; kiểm tra cookie domain/Secure và API URL.")


def pick_variant(ctx: Attempt):
    """Return a random in-stock variant, reading at most 20 catalog pages."""
    slugs, cursor = [], None
    for _ in range(20):
        suffix = "?in_stock=true" + ("&cursor=" + quote(cursor, safe="") if cursor else "")
        listing = ctx.call("GET", "/products" + suffix)
        slugs += [product["slug"] for product in listing["items"]]
        cursor = listing.get("next_cursor")
        if not cursor:
            break
    ctx.rng.shuffle(slugs)
    quantity = ctx.config.quantity
    for slug in slugs:
        detail = ctx.call("GET", "/products/" + quote(slug, safe=""))
        variants = [item for item in detail["variants"] if item["in_stock"] and item["stock_quantity"] >= quantity]
        if variants:
            return ctx.rng.choice(variants)
    raise ScenarioFailure("Không tìm thấy variant đủ tồn trong tối đa 20 trang catalog.")


def buy(ctx: Attempt, payment_method, coupon_code=None):
    """Register a customer, buy one random variant and verify the order; return the order detail."""
    config = ctx.config
    register_and_login(ctx)
    variant = pick_variant(ctx)
    ctx.checkpoint(variant_public_id=variant["public_id"], sku=variant["sku"])
    cart = ctx.call("PUT", "/cart/items/" + quote(variant["public_id"], safe=""), {"quantity": config.quantity})
    if (
        len(cart["items"]) != 1
        or cart["items"][0]["variant_public_id"] != variant["public_id"]
        or cart["items"][0]["quantity"] != config.quantity
    ):
        raise ScenarioFailure("Giỏ hàng không khớp kịch bản; dừng trước checkout.")
    coupon = {"coupon_code": coupon_code} if coupon_code else {}
    quote_result = ctx.call("POST", "/checkout/quote", coupon)
    payload = {
        "receiver_name": config.receiver_name,
        "receiver_phone": config.receiver_phone,
        "shipping_address_text": config.shipping_address_text,
        "payment_method": payment_method,
        **coupon,
    }
    if config.shipping_city_code is not None:
        payload["shipping_city_code"] = config.shipping_city_code
    result = ctx.call("POST", "/checkout", payload, key=ctx.record["idempotency_key"])
    # Persist the known order before the verification GET; never replay a checkout.
    ctx.checkpoint(order_number=result["order_number"], total_vnd=result["total_vnd"], checkout_status=result["status"])
    try:
        detail = ctx.call("GET", "/orders/" + quote(result["order_number"], safe=""))
        cart_after = ctx.call("GET", "/cart")
        expected_status, expected_payment = (
            ("confirmed", "pending") if payment_method == "cod" else ("paid", "succeeded")
        )
        valid = (
            detail["order_number"] == result["order_number"]
            and detail["total_vnd"] == result["total_vnd"] == quote_result["total_vnd"]
            and detail["status"] == expected_status
            and detail["payment"]["status"] == expected_payment
            and detail["payment"]["amount_vnd"] == result["total_vnd"]
            and len(detail["items"]) == 1
            and detail["items"][0]["sku"] == variant["sku"]
            and detail["items"][0]["quantity"] == config.quantity
            and not cart_after["items"]
        )
        if not valid:
            raise ScenarioFailure("Đơn đã tạo nhưng hậu điều kiện chưa khớp; cần đối soát.", unknown=True)
    except ScenarioFailure as error:
        raise ScenarioFailure(str(error), unknown=True) from None
    return detail


def dispatch(ctx: Attempt, order_number):
    staff = [row for row in ctx.admin_call("GET", "/admin/delivery-staff") if row["is_active"]]
    if not staff:
        raise ScenarioFailure("Không có nhân viên giao đang hoạt động; dừng trước khi giao.")
    path = f"/admin/orders/{quote(order_number, safe='')}"
    body = {"delivery_staff_id": ctx.rng.choice(staff)["staff_id"]}
    expect(ctx.admin_call("POST", path + "/dispatch", body, key=ctx.key("disp")), "shipping", "dispatch")


def deliver(ctx: Attempt, order_number):
    """Dispatch, deliver and let the customer confirm receipt."""
    dispatch(ctx, order_number)
    order = quote(order_number, safe="")
    expect(ctx.admin_call("POST", f"/admin/orders/{order}/deliver", key=ctx.key("dlvr")), "delivered", "deliver")
    expect(ctx.call("POST", f"/orders/{order}/complete", key=ctx.key("cmpl")), "completed", "complete")
