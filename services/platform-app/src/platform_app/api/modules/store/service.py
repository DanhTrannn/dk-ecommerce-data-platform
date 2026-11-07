from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_app.api.core.errors import not_found
from platform_app.api.models.order import Order
from platform_app.api.modules.orders.schemas import OrderDetailResponse
from platform_app.api.modules.orders.service import get_order_detail


def get_store_order_detail(db: Session, store_id: int, order_number: str) -> OrderDetailResponse:
    order = db.execute(
        select(Order).where(
            Order.order_number == order_number,
            Order.store_id == store_id,
        )
    ).scalar_one_or_none()
    if order is None:
        raise not_found("Không tìm thấy đơn hàng.")
    return get_order_detail(db, order.customer_id, order_number)
