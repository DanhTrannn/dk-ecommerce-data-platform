from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from platform_app.api.core.errors import INVALID_STATE_TRANSITION, VALIDATION_ERROR, AppError, not_found
from platform_app.api.core.ids import uuid7
from platform_app.api.db.uow import run_in_transaction
from platform_app.api.models.catalog import Category, Product, ProductVariant
from platform_app.api.models.customer import Customer, CustomerCredential
from platform_app.api.models.inventory import Inventory
from platform_app.api.models.order import Order, OrderItem
from platform_app.api.modules.admin.schemas import (
    AdminCustomerResponse,
    AdminOrderResponse,
    AdminOverviewResponse,
    AdminProductResponse,
    AdminVariantResponse,
    CreateProductRequest,
    ReinstateCodResponse,
    UpdateProductRequest,
    UpdateVariantRequest,
)
from platform_app.api.modules.orders.schemas import OrderDetailResponse
from platform_app.api.modules.orders.service import get_order_detail


def _parse_public_id(value: str, label: str) -> UUID:
    try:
        return UUID(value)
    except (TypeError, ValueError) as error:
        raise not_found(f"Không tìm thấy {label}.") from error


def get_overview(db: Session) -> AdminOverviewResponse:
    from platform_app.api.modules.analytics.service import get_admin_overview
    return get_admin_overview()


def list_products(db: Session, search: str | None = None) -> list[AdminProductResponse]:
    stmt = select(Product, Category.code).join(
        Category, Category.category_id == Product.category_id
    )
    normalized_search = (search or "").strip()
    if normalized_search:
        variant_match = (
            select(ProductVariant.variant_id)
            .where(
                ProductVariant.product_id == Product.product_id,
                ProductVariant.sku.contains(normalized_search, autoescape=True),
            )
            .exists()
        )
        stmt = stmt.where(
            or_(
                Product.name.contains(normalized_search, autoescape=True),
                Product.slug.contains(normalized_search, autoescape=True),
                variant_match,
            )
        )
    product_rows = db.execute(
        stmt.order_by(Product.created_at.desc(), Product.product_id.desc()).limit(100)
    ).all()
    product_ids = [product.product_id for product, _ in product_rows]
    variants_by_product: dict[int, list[AdminVariantResponse]] = {product_id: [] for product_id in product_ids}
    if product_ids:
        variant_rows = db.execute(
            select(ProductVariant, func.coalesce(Inventory.on_hand, 0).label("on_hand"))
            .outerjoin(Inventory, Inventory.variant_id == ProductVariant.variant_id)
            .where(ProductVariant.product_id.in_(product_ids))
            .order_by(ProductVariant.product_id, ProductVariant.variant_id)
        ).all()
        for variant, on_hand in variant_rows:
            variants_by_product[variant.product_id].append(
                AdminVariantResponse(
                    variant_id=variant.variant_id,
                    public_id=str(variant.public_id),
                    sku=variant.sku,
                    size_code=variant.size_code,
                    color_code=variant.color_code,
                    price_vnd=variant.price_vnd,
                    cost_price_vnd=int(variant.cost_price_vnd or 0),
                    is_active=variant.is_active,
                    on_hand=int(on_hand or 0),
                )
            )
    return [
        AdminProductResponse(
            public_id=str(product.public_id),
            category_code=category_code,
            slug=product.slug,
            name=product.name,
            description=product.description,
            image_url=product.image_url,
            is_active=product.is_active,
            archived_at=product.archived_at,
            archive_reason=product.archive_reason,
            variants=variants_by_product[product.product_id],
        )
        for product, category_code in product_rows
    ]


def create_product(payload: CreateProductRequest) -> AdminProductResponse:
    def _work(db: Session) -> None:
        category = db.execute(
            select(Category).where(Category.code == payload.category_code, Category.is_active.is_(True))
        ).scalar_one_or_none()
        if category is None:
            raise AppError(VALIDATION_ERROR, "Danh mục không hợp lệ.", status_code=422)
        product = Product(
            public_id=uuid7(),
            category_id=category.category_id,
            slug=payload.slug,
            name=payload.name,
            description=payload.description or None,
            image_url=str(payload.image_url) if payload.image_url else None,
            is_active=True,
        )
        db.add(product)
        db.flush()
        for item in payload.variants:
            variant = ProductVariant(
                public_id=uuid7(),
                product_id=product.product_id,
                sku=item.sku.upper(),
                size_code=item.size_code,
                color_code=item.color_code,
                price_vnd=item.price_vnd,
                is_active=True,
            )
            db.add(variant)
            db.flush()
            db.add(
                Inventory(
                    variant_id=variant.variant_id,
                    opening_on_hand=item.opening_on_hand,
                    on_hand=item.opening_on_hand,
                    version=0,
                )
            )
        db.flush()

    try:
        run_in_transaction(_work)
    except IntegrityError as error:
        raise AppError(
            VALIDATION_ERROR,
            "Slug, SKU hoặc tổ hợp size/màu đã tồn tại.",
            status_code=409,
        ) from error
    # Read through a fresh transaction so the response always reflects committed data.
    from platform_app.api.db.session import SessionLocal

    db = SessionLocal()
    try:
        product = db.execute(select(Product).where(Product.slug == payload.slug)).scalar_one()
        return next(item for item in list_products(db) if item.public_id == str(product.public_id))
    finally:
        db.close()


def update_product(public_id: str, payload: UpdateProductRequest) -> None:
    parsed_id = _parse_public_id(public_id, "sản phẩm")

    def _work(db: Session) -> None:
        product = db.execute(
            select(Product).where(Product.public_id == parsed_id).with_for_update()
        ).scalar_one_or_none()
        if product is None:
            raise not_found("Không tìm thấy sản phẩm.")
        fields = payload.model_fields_set
        if "category_code" in fields:
            category = db.execute(
                select(Category).where(Category.code == payload.category_code, Category.is_active.is_(True))
            ).scalar_one_or_none()
            if category is None:
                raise AppError(VALIDATION_ERROR, "Danh mục không hợp lệ.", status_code=422)
            product.category_id = category.category_id
        if "name" in fields:
            product.name = payload.name
        if "description" in fields:
            product.description = payload.description or None
        if "image_url" in fields:
            product.image_url = str(payload.image_url) if payload.image_url else None
        if "is_active" in fields:
            if payload.is_active and product.archived_at is not None:
                raise AppError(
                    INVALID_STATE_TRANSITION,
                    "Sản phẩm đã lưu trữ không thể bật lại.",
                    status_code=409,
                )
            product.is_active = payload.is_active
        product.updated_at = datetime.now(UTC)
        db.flush()

    run_in_transaction(_work)


def archive_product(admin_customer_id: int, public_id: str, reason: str) -> None:
    parsed_id = _parse_public_id(public_id, "sản phẩm")

    def _work(db: Session) -> None:
        product = db.execute(
            select(Product).where(Product.public_id == parsed_id).with_for_update()
        ).scalar_one_or_none()
        if product is None:
            raise not_found("Không tìm thấy sản phẩm.")
        if product.archived_at is not None:
            return
        now = datetime.now(UTC).replace(tzinfo=None)
        product.is_active = False
        product.archived_at = now
        product.archived_by_customer_id = admin_customer_id
        product.archive_reason = reason
        product.updated_at = now
        db.flush()

    run_in_transaction(_work)


def update_variant(public_id: str, payload: UpdateVariantRequest) -> None:
    parsed_id = _parse_public_id(public_id, "biến thể")

    def _work(db: Session) -> None:
        variant = db.execute(
            select(ProductVariant).where(ProductVariant.public_id == parsed_id).with_for_update()
        ).scalar_one_or_none()
        if variant is None:
            raise not_found("Không tìm thấy biến thể.")
        if payload.price_vnd is not None:
            variant.price_vnd = payload.price_vnd
        if payload.is_active is not None:
            variant.is_active = payload.is_active
        variant.updated_at = datetime.now(UTC)
        db.flush()

    run_in_transaction(_work)


def list_orders(db: Session, status: str | None) -> list[AdminOrderResponse]:
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
        .outerjoin(Customer, Customer.customer_id == Order.customer_id)
        .outerjoin(CustomerCredential, CustomerCredential.customer_id == Customer.customer_id)
        .outerjoin(item_count, item_count.c.order_id == Order.order_id)
    )
    if status:
        stmt = stmt.where(Order.status == status)
    rows = db.execute(stmt.order_by(Order.created_at.desc(), Order.order_id.desc()).limit(200)).all()
    return [
        AdminOrderResponse(
            order_number=order.order_number,
            customer_name=customer_name or order.receiver_name or "Khách tại quầy",
            customer_email=email or "",
            status=order.status,
            total_vnd=order.total_vnd,
            item_count=int(item_count_value),
            created_at=order.created_at,
            channel=order.channel or "online",
            payment_method=order.payment_method or "vietqr",
        )
        for order, customer_name, email, item_count_value in rows
    ]



def get_admin_order_detail(db: Session, order_number: str) -> OrderDetailResponse:
    order = db.execute(
        select(Order).where(Order.order_number == order_number)
    ).scalar_one_or_none()
    if order is None:
        raise not_found("Không tìm thấy đơn hàng.")
    return get_order_detail(db, None, order_number)


def list_customers(db: Session) -> list[AdminCustomerResponse]:
    rows = db.execute(
        select(Customer, CustomerCredential.email_normalized)
        .join(CustomerCredential, CustomerCredential.customer_id == Customer.customer_id)
        .order_by(Customer.created_at.desc(), Customer.customer_id.desc())
        .limit(200)
    ).all()
    return [
        AdminCustomerResponse(
            public_id=str(customer.public_id),
            display_name=customer.display_name,
            email=email,
            status=customer.status,
            role=customer.role,
            is_cod_blocked=customer.is_cod_blocked,
            boom_count=customer.boom_count,
            cod_reinstated_at=customer.cod_reinstated_at,
            cod_reinstated_by=customer.cod_reinstated_by,
            cod_reinstatement_reason=customer.cod_reinstatement_reason,
            created_at=customer.created_at,
        )
        for customer, email in rows
    ]


def update_customer_status(admin_customer_id: int, public_id: str, status: str) -> None:
    parsed_id = _parse_public_id(public_id, "khách hàng")

    def _work(db: Session) -> None:
        customer = db.execute(
            select(Customer).where(Customer.public_id == parsed_id).with_for_update()
        ).scalar_one_or_none()
        if customer is None:
            raise not_found("Không tìm thấy khách hàng.")
        if customer.customer_id == admin_customer_id and status != "active":
            raise AppError(VALIDATION_ERROR, "Không thể vô hiệu hóa chính tài khoản đang dùng.", status_code=409)
        if customer.role == "admin" and status != "active":
            raise AppError(VALIDATION_ERROR, "Không thể vô hiệu hóa tài khoản quản trị.", status_code=409)
        customer.status = status
        customer.updated_at = datetime.now(UTC)
        db.flush()

    run_in_transaction(_work)


def reinstate_customer_cod(
    admin_customer_id: int,
    customer_id_or_public_id: int | str,
    reason: str,
    probation_failed_count: int = 2,
) -> ReinstateCodResponse:
    def _work(db: Session) -> ReinstateCodResponse:
        now = datetime.now(UTC)
        query = select(Customer).with_for_update()
        if isinstance(customer_id_or_public_id, int) or (
            isinstance(customer_id_or_public_id, str) and customer_id_or_public_id.isdigit()
        ):
            query = query.where(Customer.customer_id == int(customer_id_or_public_id))
        else:
            try:
                parsed_uuid = UUID(str(customer_id_or_public_id))
                query = query.where(Customer.public_id == parsed_uuid)
            except (ValueError, TypeError):
                raise not_found("Không tìm thấy khách hàng.")

        customer = db.execute(query).scalar_one_or_none()
        if customer is None:
            raise not_found("Không tìm thấy khách hàng.")

        customer.is_cod_blocked = False
        customer.boom_count = min(customer.boom_count, probation_failed_count)
        customer.cod_reinstated_at = now
        customer.cod_reinstated_by = admin_customer_id
        customer.cod_reinstatement_reason = reason.strip()
        customer.updated_at = now
        db.flush()

        return ReinstateCodResponse(
            customer_id=customer.customer_id,
            public_id=str(customer.public_id),
            is_cod_blocked=customer.is_cod_blocked,
            boom_count=customer.boom_count,
            cod_reinstated_at=customer.cod_reinstated_at,
            cod_reinstated_by=customer.cod_reinstated_by,
            cod_reinstatement_reason=customer.cod_reinstatement_reason,
        )

    return run_in_transaction(_work)
