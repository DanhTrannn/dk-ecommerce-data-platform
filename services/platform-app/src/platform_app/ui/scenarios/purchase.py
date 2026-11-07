"""One isolated customer cart/checkout flow through the public HTTP contract."""

import secrets
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


def purchase(client, config: PurchaseConfig, attempt, checkpoint, *, csrf_cookie="web_csrf"):
    def call(method, path, payload=None, **kwargs):
        checkpoint(stage=path)
        return request(client, method, path, payload, csrf_cookie=csrf_cookie, **kwargs)

    # Each attempt owns a fresh cart. Credentials and cookies never enter the journal.
    password = secrets.token_urlsafe(32)
    call(
        "POST",
        "/auth/register",
        {
            "email": attempt["customer_email"],
            "password": password,
            "display_name": config.receiver_name,
        },
    )
    call("POST", "/auth/login", {"email": attempt["customer_email"], "password": password})
    if not client.cookies.get(csrf_cookie):
        raise ScenarioFailure("Không nhận được CSRF cookie; kiểm tra cookie domain/Secure và API URL.")
    variant = None
    cursor = None
    # Bound discovery traffic; no assumptions about database IDs or seeded catalog.
    for _ in range(20):
        suffix = "?in_stock=true" + ("&cursor=" + quote(cursor, safe="") if cursor else "")
        listing = call("GET", "/products" + suffix)
        for product in listing["items"]:
            detail = call("GET", "/products/" + quote(product["slug"], safe=""))
            variant = next(
                (item for item in detail["variants"] if item["in_stock"] and item["stock_quantity"] >= config.quantity),
                None,
            )
            if variant:
                break
        if variant or not listing.get("next_cursor"):
            break
        cursor = listing["next_cursor"]
    if not variant:
        raise ScenarioFailure("Không tìm thấy variant đủ tồn trong tối đa 20 trang catalog.")
    checkpoint(variant_public_id=variant["public_id"], sku=variant["sku"])
    cart = call("PUT", "/cart/items/" + quote(variant["public_id"], safe=""), {"quantity": config.quantity})
    if (
        len(cart["items"]) != 1
        or cart["items"][0]["variant_public_id"] != variant["public_id"]
        or cart["items"][0]["quantity"] != config.quantity
    ):
        raise ScenarioFailure("Giỏ hàng không khớp kịch bản; dừng trước checkout.")
    quote_result = call("POST", "/checkout/quote", {})
    payload = {
        name: getattr(config, name)
        for name in ("receiver_name", "receiver_phone", "shipping_address_text", "payment_method")
    }
    result = call("POST", "/checkout", payload, key=attempt["idempotency_key"])
    # Persist the known order before the verification GET; never replay a checkout.
    checkpoint(order_number=result["order_number"], total_vnd=result["total_vnd"], checkout_status=result["status"])
    try:
        detail = call("GET", "/orders/" + quote(result["order_number"], safe=""))
        cart_after = call("GET", "/cart")
        expected_status, expected_payment = (
            ("confirmed", "pending") if config.payment_method == "cod" else ("paid", "succeeded")
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
    checkpoint(status="succeeded", stage="verified")
