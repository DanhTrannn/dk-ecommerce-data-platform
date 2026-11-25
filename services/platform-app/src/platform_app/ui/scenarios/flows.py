"""Console scenarios: each one drives the source through the API so that one Gold mart changes."""

from urllib.parse import quote

from platform_app.ui.scenarios.steps import Attempt, buy, deliver, dispatch, expect


def purchase(ctx: Attempt):
    buy(ctx, ctx.config.payment_method)


def delivered(ctx: Attempt):
    deliver(ctx, buy(ctx, "cod")["order_number"])


def failed_delivery(ctx: Attempt):
    order_number = buy(ctx, "cod")["order_number"]
    dispatch(ctx, order_number)
    path = f"/admin/orders/{quote(order_number, safe='')}/failed-delivery"
    body = {"reason": "Khách không nghe máy (mô phỏng)"}
    expect(ctx.admin_call("POST", path, body, key=ctx.key("fail")), "failed_delivery", "failed-delivery")


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


# Code → (console label, scenario, Gold marts it feeds), in console order.
SCENARIOS = {
    "purchase": ("Mua hàng", purchase, "đơn, doanh thu"),
    "delivered": ("Mua và giao thành công", delivered, "giao hàng, tồn đọng"),
    "failed_delivery": ("Giao thất bại", failed_delivery, "giao hàng"),
    "cancelled": ("Khách huỷ đơn", cancelled, "huỷ đơn"),
    "review": ("Mua rồi đánh giá", review, "đánh giá"),
}
