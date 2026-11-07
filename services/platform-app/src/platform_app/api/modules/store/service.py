
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from platform_app.api.core.errors import not_found
from platform_app.api.models.catalog import Category, Product, ProductVariant
from platform_app.api.models.customer import Customer, CustomerCredential
from platform_app.api.models.multicity import Store, StoreInventory
from platform_app.api.models.order import Order, OrderItem
from platform_app.api.modules.orders.schemas import OrderDetailResponse
from platform_app.api.modules.orders.service import get_order_detail
from platform_app.api.modules.store.schemas import (
    StoreDashboardResponse,
    StoreInventoryItem,
    StoreOrderResponse,
    StoreStaffMember,
)


def get_store_dashboard(db: Session, store_id: int) -> StoreDashboardResponse:
    from platform_app.api.modules.analytics.service import get_store_overview
    return get_store_overview(store_id)


def list_store_orders(
    db: Session, store_id: int, status: str | None
) -> list[StoreOrderResponse]:
    item_count = (
        select(OrderItem.order_id, func.sum(OrderItem.quantity).label("item_count"))
        .group_by(OrderItem.order_id)
        .subquery()
    )
    stmt = (
        select(
            Order,
            Customer.display_name,
            CustomerCredential.email_normalized,
            func.coalesce(item_count.c.item_count, 0),
        )
        .join(Customer, Customer.customer_id == Order.customer_id)
        .join(CustomerCredential, CustomerCredential.customer_id == Customer.customer_id)
        .outerjoin(item_count, item_count.c.order_id == Order.order_id)
        .where(Order.store_id == store_id)
    )
    if status:
        stmt = stmt.where(Order.status == status)
    rows = db.execute(
        stmt.order_by(Order.created_at.desc(), Order.order_id.desc()).limit(200)
    ).all()
    return [
        StoreOrderResponse(
            order_number=order.order_number,
            customer_name=customer_name,
            customer_email=email,
            channel=order.channel,
            status=order.status,
            total_vnd=order.total_vnd,
            item_count=int(item_count_value),
            created_at=order.created_at,
        )
        for order, customer_name, email, item_count_value in rows
    ]


def get_store_order_detail(
    db: Session, store_id: int, order_number: str
) -> OrderDetailResponse:
    order = db.execute(
        select(Order).where(
            Order.order_number == order_number,
            Order.store_id == store_id,
        )
    ).scalar_one_or_none()
    if order is None:
        raise not_found("Không tìm thấy đơn hàng.")
    return get_order_detail(db, order.customer_id, order_number)


def list_store_inventory(db: Session, store_id: int) -> list[StoreInventoryItem]:
    rows = db.execute(
        select(
            StoreInventory,
            Store.name.label("store_name"),
            ProductVariant.sku.label("variant_sku"),
            Product.name.label("product_name"),
            Category.name.label("category_name"),
            ProductVariant.size_code,
            ProductVariant.color_code,
            ProductVariant.price_vnd,
        )
        .join(Store, Store.store_id == StoreInventory.store_id)
        .join(ProductVariant, ProductVariant.variant_id == StoreInventory.variant_id)
        .join(Product, Product.product_id == ProductVariant.product_id)
        .join(Category, Category.category_id == Product.category_id)
        .where(StoreInventory.store_id == store_id)
        .order_by(StoreInventory.variant_id)
    ).all()
    return [
        StoreInventoryItem(
            store_id=row.StoreInventory.store_id,
            store_name=row.store_name,
            variant_id=row.StoreInventory.variant_id,
            variant_sku=row.variant_sku,
            product_name=row.product_name,
            category_name=row.category_name,
            size_code=row.size_code,
            color_code=row.color_code,
            price_vnd=row.price_vnd,
            on_hand=row.StoreInventory.on_hand,
            opening_on_hand=row.StoreInventory.opening_on_hand,
        )
        for row in rows
    ]


def list_store_staff(db: Session, store_id: int) -> list[StoreStaffMember]:
    rows = db.execute(
        select(Customer, CustomerCredential.email_normalized)
        .join(CustomerCredential, CustomerCredential.customer_id == Customer.customer_id)
        .where(
            Customer.store_id == store_id,
            Customer.role == "store_manager",
        )
        .order_by(Customer.customer_id)
    ).all()
    return [
        StoreStaffMember(
            public_id=str(customer.public_id),
            display_name=customer.display_name,
            email=email,
            role=customer.role,
            status=customer.status,
            created_at=customer.created_at,
        )
        for customer, email in rows
    ]
