from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from platform_app.api.common.money import compute_amounts
from platform_app.api.core.errors import (
    CART_NOT_ACTIVE,
    EMPTY_CART,
    IDEMPOTENCY_CONFLICT,
    OUT_OF_STOCK,
    VALIDATION_ERROR,
    VARIANT_NOT_SELLABLE,
    AppError,
    auth_required,
)
from platform_app.api.core.ids import new_order_number, new_payment_reference, uuid7
from platform_app.api.db.uow import run_in_transaction
from platform_app.api.models.cart import Cart, CartItem
from platform_app.api.models.catalog import Category, Product, ProductVariant
from platform_app.api.models.customer import Customer
from platform_app.api.models.inventory import Inventory
from platform_app.api.models.multicity import City
from platform_app.api.models.order import Order, OrderItem, OrderStatusHistory, Payment
from platform_app.api.models.promotion import CouponRedemption
from platform_app.api.modules.checkout.allocation import deduct_inventory
from platform_app.api.modules.checkout.schemas import CheckoutRequest, CheckoutResultResponse
from platform_app.api.modules.coupons.schemas import CheckoutQuoteRequest, CheckoutQuoteResponse
from platform_app.api.modules.coupons.service import CouponUse, resolve_coupon


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _result_from_order(order: Order, payment: Payment) -> CheckoutResultResponse:
    return CheckoutResultResponse(
        order_number=order.order_number,
        status=order.status,
        payment_status=payment.status,
        failure_code=payment.failure_code,
        coupon_code=order.coupon_code_snapshot,
        subtotal_vnd=order.subtotal_vnd,
        discount_amount_vnd=order.discount_amount_vnd,
        shipping_fee_vnd=order.shipping_fee_vnd,
        total_vnd=order.total_vnd,
        payment_method=order.payment_method,
    )


def quote_checkout(
    db: Session,
    customer_id: int,
    payload: CheckoutQuoteRequest,
) -> CheckoutQuoteResponse:
    cart = db.execute(
        select(Cart).where(Cart.customer_id == customer_id, Cart.status == "active")
    ).scalar_one_or_none()
    if cart is None:
        raise AppError(CART_NOT_ACTIVE, "Không có giỏ hàng đang hoạt động.", status_code=409)
    rows = db.execute(
        select(CartItem, ProductVariant, Product, Category, Inventory)
        .join(ProductVariant, ProductVariant.variant_id == CartItem.variant_id)
        .join(Product, Product.product_id == ProductVariant.product_id)
        .join(Category, Category.category_id == Product.category_id)
        .join(Inventory, Inventory.variant_id == ProductVariant.variant_id)
        .where(CartItem.cart_id == cart.cart_id, CartItem.is_present.is_(True))
        .order_by(CartItem.variant_id)
    ).all()
    if not rows:
        raise AppError(EMPTY_CART, "Giỏ hàng trống.", status_code=409)
    line_totals: list[int] = []
    for item, variant, product, category, inventory in rows:
        if not (variant.is_active and product.is_active and category.is_active):
            raise AppError(VARIANT_NOT_SELLABLE, "Sản phẩm không khả dụng.", status_code=409)
        if inventory.on_hand < item.quantity:
            raise AppError(OUT_OF_STOCK, "Sản phẩm không đủ tồn kho.", status_code=409)
        line_totals.append(variant.price_vnd * item.quantity)

    subtotal = sum(line_totals)
    coupon_use = (
        resolve_coupon(
            db,
            payload.coupon_code,
            customer_id,
            subtotal,
            _utc_now(),
            for_update=False,
        )
        if payload.coupon_code
        else None
    )
    amounts = compute_amounts(
        line_totals,
        coupon_use.discount_amount_vnd if coupon_use else 0,
    )
    return CheckoutQuoteResponse(
        coupon_code=coupon_use.coupon.code_normalized if coupon_use else None,
        discount_type=coupon_use.coupon.discount_type if coupon_use else None,
        discount_value=coupon_use.coupon.discount_value if coupon_use else None,
        subtotal_vnd=amounts.subtotal_vnd,
        discount_amount_vnd=amounts.discount_amount_vnd,
        shipping_fee_vnd=amounts.shipping_fee_vnd,
        total_vnd=amounts.total_vnd,
    )


def checkout(customer_id: int, idempotency_key: str, payload: CheckoutRequest) -> CheckoutResultResponse:
    def _work(db: Session) -> CheckoutResultResponse:
        customer = db.execute(
            select(Customer).where(Customer.customer_id == customer_id).with_for_update()
        ).scalar_one_or_none()
        if customer is None or customer.status != "active":
            raise auth_required()

        if payload.payment_method == "cod" and customer.is_cod_blocked:
            raise AppError(
                VALIDATION_ERROR,
                "Tài khoản của bạn đã bị khóa phương thức COD do boom hàng. Vui lòng chọn VietQR.",
                status_code=403,
            )

        existing = db.execute(
            select(Order).where(Order.checkout_idempotency_key == idempotency_key)
        ).scalar_one_or_none()
        if existing is not None:
            if existing.customer_id != customer_id:
                raise AppError(
                    IDEMPOTENCY_CONFLICT,
                    "Idempotency key đã được dùng cho request khác.",
                    status_code=409,
                )
            payment = db.execute(
                select(Payment).where(Payment.order_id == existing.order_id)
            ).scalar_one()
            return _result_from_order(existing, payment)

        cart = db.execute(
            select(Cart)
            .where(Cart.customer_id == customer_id, Cart.status == "active")
            .with_for_update()
        ).scalar_one_or_none()
        if cart is None:
            raise AppError(CART_NOT_ACTIVE, "Không có giỏ hàng đang hoạt động.", status_code=409)

        items = (
            db.execute(
                select(CartItem)
                .where(CartItem.cart_id == cart.cart_id, CartItem.is_present.is_(True))
                .order_by(CartItem.variant_id)
                .with_for_update()
            )
            .scalars()
            .all()
        )
        if not items:
            raise AppError(EMPTY_CART, "Giỏ hàng trống.", status_code=409)

        variant_ids = sorted({item.variant_id for item in items})
        catalog_rows = db.execute(
            select(ProductVariant, Product, Category)
            .join(Product, Product.product_id == ProductVariant.product_id)
            .join(Category, Category.category_id == Product.category_id)
            .where(ProductVariant.variant_id.in_(variant_ids))
            .order_by(ProductVariant.variant_id)
            .with_for_update()
        ).all()
        catalog = {row[0].variant_id: row for row in catalog_rows}
        for variant_id in variant_ids:
            entry = catalog.get(variant_id)
            if entry is None:
                raise AppError(VARIANT_NOT_SELLABLE, "Sản phẩm không khả dụng.", status_code=409)
            variant, product, category = entry
            if not (variant.is_active and product.is_active and category.is_active):
                raise AppError(VARIANT_NOT_SELLABLE, "Sản phẩm không khả dụng.", status_code=409)

        quantities = {item.variant_id: item.quantity for item in items}
        now = _utc_now()
        order_items: list[OrderItem] = []
        line_totals: list[int] = []
        for variant_id in variant_ids:
            variant, product, category = catalog[variant_id]
            quantity = quantities[variant_id]
            line_total = variant.price_vnd * quantity
            line_totals.append(line_total)
            order_items.append(
                OrderItem(
                    public_id=uuid7(),
                    variant_id=variant_id,
                    product_public_id_snapshot=product.public_id,
                    category_code_snapshot=category.code,
                    category_name_snapshot=category.name,
                    product_name_snapshot=product.name,
                    sku_snapshot=variant.sku,
                    size_code_snapshot=variant.size_code,
                    color_code_snapshot=variant.color_code,
                    unit_price_vnd=variant.price_vnd,
                    cost_price_vnd=variant.cost_price_vnd,
                    quantity=quantity,
                    line_total_vnd=line_total,
                    created_at=now,
                )
            )

        subtotal = sum(line_totals)
        coupon_use: CouponUse | None = (
            resolve_coupon(
                db,
                payload.coupon_code,
                customer_id,
                subtotal,
                now,
                for_update=True,
            )
            if payload.coupon_code
            else None
        )

        amounts = compute_amounts(
            line_totals,
            coupon_use.discount_amount_vnd if coupon_use else 0,
        )
        coupon = coupon_use.coupon if coupon_use else None
        is_cod = payload.payment_method == "cod"
        order_status = "confirmed" if is_cod else "paid"
        paid_at = None if is_cod else now
        confirmed_at = now if is_cod else None
        payment_status = "pending" if is_cod else "succeeded"
        history_to_status = "confirmed" if is_cod else "paid"
        history_idempotency_key = f"{idempotency_key[:54]}:conf" if is_cod else f"{idempotency_key[:54]}:paid"

        shipping_city_id = None
        if payload.shipping_city_code is not None:
            shipping_city_id = db.execute(
                select(City.city_id).where(
                    func.lower(City.code) == payload.shipping_city_code.lower(), City.is_active.is_(True)
                )
            ).scalar_one_or_none()
            if shipping_city_id is None:
                raise AppError(VALIDATION_ERROR, "Không tìm thấy thành phố giao hàng.", status_code=400)

        order = Order(
            order_number=new_order_number(),
            cart_id=cart.cart_id,
            customer_id=customer_id,
            checkout_idempotency_key=idempotency_key,
            coupon_id=coupon.coupon_id if coupon else None,
            status=order_status,
            payment_method=payload.payment_method,
            subtotal_vnd=amounts.subtotal_vnd,
            coupon_code_snapshot=coupon.code_normalized if coupon else None,
            coupon_type_snapshot=coupon.discount_type if coupon else None,
            coupon_value_snapshot=coupon.discount_value if coupon else None,
            discount_amount_vnd=amounts.discount_amount_vnd,
            shipping_fee_vnd=amounts.shipping_fee_vnd,
            total_vnd=amounts.total_vnd,
            receiver_name=payload.receiver_name,
            receiver_phone=payload.receiver_phone,
            shipping_address_text=payload.shipping_address_text,
            shipping_city_id=shipping_city_id,
            data_origin="manual",
            created_at=now,
            paid_at=paid_at,
            confirmed_at=confirmed_at,
        )
        db.add(order)
        db.flush()

        allocation_items = [{"variant_id": vid, "quantity": quantities[vid]} for vid in variant_ids]
        deduct_inventory(db, allocation_items, order_id=order.order_id, reference_code=order.order_number)
        order.channel = "online"
        db.flush()

        for order_item in order_items:
            order_item.order_id = order.order_id
            db.add(order_item)

        payment = Payment(
            payment_reference=new_payment_reference(),
            order_id=order.order_id,
            payment_idempotency_key=f"{idempotency_key}:pay",
            status=payment_status,
            amount_vnd=amounts.total_vnd,
            failure_code=None,
            attempted_at=now,
        )
        db.add(payment)
        db.add(
            OrderStatusHistory(
                order_id=order.order_id,
                from_status=None,
                to_status=history_to_status,
                transition_source="checkout",
                transition_idempotency_key=history_idempotency_key,
                transitioned_at=now,
            )
        )

        if coupon is not None:
            coupon.used_count += 1
            coupon.updated_at = now
            db.add(
                CouponRedemption(
                    coupon_id=coupon.coupon_id,
                    order_id=order.order_id,
                    customer_id=customer_id,
                    status="redeemed",
                    redeemed_at=now,
                )
            )

        cart.status = "checked_out"
        cart.checked_out_at = now
        cart.updated_at = now
        db.flush()
        return _result_from_order(order, payment)

    return run_in_transaction(_work)
