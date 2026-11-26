"""Console scenarios: each one drives the source through the API so that one Gold mart changes."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import NamedTuple
from urllib.parse import quote, urlencode

from platform_app.ui.scenarios.steps import Attempt, ScenarioFailure, buy, deliver, dispatch, expect, pick_variant


def purchase(ctx: Attempt):
    buy(ctx, ctx.config.payment_method)


def delivered(ctx: Attempt):
    deliver(ctx, buy(ctx, "cod")["order_number"])


def failed_delivery(ctx: Attempt):
    order_number = buy(ctx, "cod")["order_number"]
    dispatch(ctx, order_number)
    path = f"/admin/orders/{quote(order_number, safe='')}/failed-delivery"
    body = {"reason": "Khách không nghe máy (mô phỏng)"}
    # The API allows three delivery attempts; the order stays shipping until the last one fails.
    for attempt in (1, 2, 3):
        status = "failed_delivery" if attempt == 3 else "shipping"
        expect(ctx.admin_call("POST", path, body, key=ctx.key(f"fail{attempt}")), status, "failed-delivery")


def cancelled(ctx: Attempt):
    # Customers may cancel only paid orders, so this scenario pays by VietQR.
    order_number = buy(ctx, "vietqr")["order_number"]
    body = {"reason": "Đổi ý (mô phỏng)"}
    path = f"/orders/{quote(order_number, safe='')}/cancel"
    expect(ctx.call("POST", path, body, key=ctx.key("cncl")), "cancelled", "cancel")


def review(ctx: Attempt):
    order = buy(ctx, "cod")
    deliver(ctx, order["order_number"])
    item = order["items"][0]["public_id"]
    path = f"/orders/{quote(order['order_number'], safe='')}/items/{quote(item, safe='')}/review"
    body = {"rating": ctx.rng.randint(3, 5), "content": "Đánh giá mô phỏng"}
    expect(ctx.call("POST", path, body), "approved", "review")


def return_refund(ctx: Attempt):
    order = buy(ctx, "cod")
    deliver(ctx, order["order_number"])
    line = order["items"][0]
    body = {
        "customer_reason": "Sản phẩm không vừa (mô phỏng)",
        "bank_name": "Mô phỏng",
        "bank_account_number": "0000000000",
        "bank_account_holder": "SCENARIO CUSTOMER",
        "items": [{"order_item_id": line["order_item_id"], "quantity": line["quantity"]}],
    }
    path = f"/orders/{quote(order['order_number'], safe='')}/returns"
    created = expect(ctx.call("POST", path, body, key=ctx.key("ret")), "pending_review", "return")
    ctx.checkpoint(return_code=created["return_code"])
    admin_path = f"/admin/returns/{quote(created['return_code'], safe='')}"
    expect(ctx.admin_call("POST", admin_path + "/review", {"action": "approved"}), "approved", "return review")
    expect(ctx.admin_call("POST", admin_path + "/receive"), "goods_received", "return receive")
    inspect = {
        "items": [{"return_item_id": row["return_item_id"], "inspection_status": "passed"} for row in created["items"]]
    }
    result = ctx.admin_call("POST", admin_path + "/inspect-and-resolve", inspect, key=ctx.key("rinsp"))
    expect(result, "completed", "return resolve")


def coupon(ctx: Attempt):
    # One coupon per run, created by the first attempt.
    if "coupon_code" not in ctx.run_state:
        now = datetime.now(UTC)
        body = {
            "code": "SIM-" + ctx.record["idempotency_key"].split("-")[1][:8].upper(),
            "discount_type": "percentage",
            "discount_value": 10,
            "starts_at": (now - timedelta(minutes=1)).isoformat(),
            "ends_at": (now + timedelta(days=1)).isoformat(),
        }
        ctx.run_state["coupon_code"] = ctx.admin_call("POST", "/admin/coupons", body)["code"]
    ctx.checkpoint(coupon_code=ctx.run_state["coupon_code"])
    buy(ctx, "cod", coupon_code=ctx.run_state["coupon_code"])


def store_stock(ctx: Attempt, stores, sku, quantity):
    """Return (store, POS item) for the first store that has the SKU in stock, else None."""
    for store in stores:
        query = urlencode({"store_id": store["store_id"], "search": sku})
        items = ctx.admin_call("GET", "/pos/products?" + query)
        item = next((row for row in items if row["sku"] == sku and row["store_stock"] >= quantity), None)
        if item:
            return store, item
    return None


def pos(ctx: Attempt):
    """Sell a random catalog variant at the counter of a random store that has it."""
    quantity = ctx.config.quantity
    stores = []
    for city in ctx.admin_call("GET", "/locations/cities"):
        if city["store_count"]:
            stores += ctx.admin_call("GET", f"/locations/cities/{quote(city['code'], safe='')}/stores")
    ctx.rng.shuffle(stores)
    # Stores do not stock every variant, so try a few random ones before giving up.
    for _ in range(3):
        variant = pick_variant(ctx)
        found = store_stock(ctx, stores, variant["sku"], quantity)
        if found:
            break
    else:
        raise ScenarioFailure("Không cửa hàng nào còn đủ tồn cho các SKU đã thử; dừng trước khi bán.")
    store, item = found
    ctx.checkpoint(sku=variant["sku"])
    body = {
        "store_id": store["store_id"],
        "items": [{"variant_id": item["variant_id"], "quantity": quantity}],
        "payment_method": "cash",
        "amount_received_vnd": item["price_vnd"] * quantity,
    }
    sale = expect(ctx.admin_call("POST", "/pos/transactions", body), "completed", "pos")
    ctx.checkpoint(order_number=sale["order_number"], total_vnd=sale["total_vnd"])


class Scenario(NamedTuple):
    label: str
    run: Callable[[Attempt], None]
    marts: str
    steps: str


# In console order.
SCENARIOS = {
    "purchase": Scenario(
        "Mua hàng", purchase, "đơn, doanh thu", "khách đăng ký → thêm giỏ → checkout (COD hoặc VietQR) → xác minh đơn"
    ),
    "delivered": Scenario(
        "Mua và giao thành công",
        delivered,
        "giao hàng, tồn đọng",
        "khách mua COD → admin giao cho nhân viên → giao tới → khách xác nhận nhận hàng",
    ),
    "failed_delivery": Scenario(
        "Giao thất bại",
        failed_delivery,
        "giao hàng",
        "khách mua COD → admin giao cho nhân viên → giao thất bại 3 lần → đơn failed_delivery",
    ),
    "cancelled": Scenario(
        "Khách huỷ đơn", cancelled, "huỷ đơn", "khách mua VietQR (đã thanh toán) → khách huỷ → API hoàn tiền"
    ),
    "review": Scenario(
        "Mua rồi đánh giá",
        review,
        "đánh giá",
        "khách mua COD → admin giao → giao tới → khách xác nhận → khách đánh giá 3–5 sao",
    ),
    "return": Scenario(
        "Đổi trả, hoàn tiền",
        return_refund,
        "đổi trả",
        "khách mua COD → admin giao → giao tới → khách xác nhận → khách yêu cầu trả → admin duyệt"
        " → nhận hàng → kiểm và hoàn tiền",
    ),
    "coupon": Scenario(
        "Mua có coupon",
        coupon,
        "coupon",
        "admin tạo coupon giảm 10% (một lần mỗi run) → khách mua COD với mã",
    ),
    "pos": Scenario("Bán tại quầy", pos, "POS theo giờ", "admin chọn cửa hàng còn tồn → bán tại quầy, trả tiền mặt"),
}
