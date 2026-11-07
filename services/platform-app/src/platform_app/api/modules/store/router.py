from fastapi import APIRouter, Depends, Header, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_app.api.core.errors import forbidden, not_found
from platform_app.api.db.deps import get_current_staff, get_db, verify_csrf
from platform_app.api.models.customer import Customer
from platform_app.api.models.multicity import Store
from platform_app.api.modules.orders.schemas import CancelOrderRequest, OrderDetailResponse, OrderTransitionResponse
from platform_app.api.modules.orders.service import cancel_order, confirm_order
from platform_app.api.modules.store.service import get_store_order_detail

admin_store_router = APIRouter(prefix="/admin/store", tags=["admin-store"])


def _resolve_store_id(db: Session, actor: Customer, store_id: int | None) -> int:
    if actor.role == "store_manager":
        if actor.store_id is None:
            raise forbidden("Tài khoản không gắn với cửa hàng nào.")
        if store_id is not None and store_id != actor.store_id:
            raise forbidden("Chỉ được thao tác cửa hàng được phân công.")
        return actor.store_id
    if actor.role == "admin":
        if store_id is not None:
            return store_id
        first_store = db.execute(
            select(Store).where(Store.is_active == True).order_by(Store.store_id).limit(1)
        ).scalar_one_or_none()
        if first_store is None:
            raise not_found("Không có cửa hàng nào.")
        return first_store.store_id
    raise forbidden("Chỉ quản lý cửa hàng hoặc quản trị viên mới có quyền truy cập.")


@admin_store_router.get("/orders/{order_number}", response_model=OrderDetailResponse)
def store_order_detail(
    order_number: str,
    store_id: int | None = Query(default=None, gt=0),
    actor: Customer = Depends(get_current_staff),
    db: Session = Depends(get_db),
) -> OrderDetailResponse:
    resolved_store_id = _resolve_store_id(db, actor, store_id)
    return get_store_order_detail(db, resolved_store_id, order_number)


@admin_store_router.post("/orders/{order_number}/confirm", response_model=OrderTransitionResponse)
def store_confirm_order(
    order_number: str,
    csrf: None = Depends(verify_csrf),
    store_id: int | None = Query(default=None, gt=0),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    actor: Customer = Depends(get_current_staff),
    db: Session = Depends(get_db),
) -> OrderTransitionResponse:
    resolved_store_id = _resolve_store_id(db, actor, store_id)
    get_store_order_detail(db, resolved_store_id, order_number)
    if not idempotency_key:
        raise forbidden("Yêu cầu idempotency key.")
    return confirm_order(order_number, idempotency_key, transition_source="admin")


@admin_store_router.post("/orders/{order_number}/cancel", response_model=OrderTransitionResponse)
def store_cancel_order(
    order_number: str,
    payload: CancelOrderRequest,
    csrf: None = Depends(verify_csrf),
    store_id: int | None = Query(default=None, gt=0),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    actor: Customer = Depends(get_current_staff),
    db: Session = Depends(get_db),
) -> OrderTransitionResponse:
    resolved_store_id = _resolve_store_id(db, actor, store_id)
    get_store_order_detail(db, resolved_store_id, order_number)
    if not idempotency_key:
        raise forbidden("Yêu cầu idempotency key.")
    return cancel_order(
        order_number,
        actor_customer_id=actor.customer_id,
        owner_customer_id=None,
        reason=payload.reason,
        idempotency_key=idempotency_key,
        transition_source="admin",
    )
