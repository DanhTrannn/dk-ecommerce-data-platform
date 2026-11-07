"""Seed catalogue, opening inventory, bootstrap admin, and welcome coupon.

Idempotent per reference key (never resets existing balances/passwords) and deterministic (public_id
derived via uuid5) so seed/scenario/manifest stay reproducible. Seed only holds
manual catalog + opening balance; historical data is produced by the generator (see database/seeds/README.md).
"""

import uuid
from datetime import datetime

from platform_app.api.core.config import get_settings
from platform_app.api.core.security import hash_password
from platform_app.api.models.catalog import Category, Product, ProductVariant
from platform_app.api.models.customer import Customer, CustomerCredential
from platform_app.api.models.inventory import Inventory
from platform_app.api.models.promotion import Coupon
from sqlalchemy import func, select

_NS = uuid.uuid5(uuid.NAMESPACE_URL, "web:catalog")
PRODUCT_IMAGE_URL = "https://sixdo.vn/modules/uniform/assets/image/aotruoc.webp"


def _pid(kind: str, key: str) -> uuid.UUID:
    return uuid.uuid5(_NS, f"{kind}:{key}")


CATEGORIES = [
    {"code": "ao", "name": "Áo"},
    {"code": "quan", "name": "Quần"},
    {"code": "phu-kien", "name": "Phụ kiện"},
]

# product -> (category_code, name, description, base_price, [(size, color)])
PRODUCTS = [
    (
        "ao-thun-basic",
        "ao",
        "Áo thun basic",
        "Áo thun cotton co giãn.",
        149000,
        [("S", "black"), ("M", "black"), ("L", "white")],
    ),
    (
        "ao-so-mi-linen",
        "ao",
        "Áo sơ mi linen",
        "Áo sơ mi vải linen thoáng mát.",
        329000,
        [("M", "white"), ("L", "blue")],
    ),
    (
        "ao-khoac-gio",
        "ao",
        "Áo khoác gió",
        "Áo khoác chống nước nhẹ.",
        459000,
        [("M", "navy"), ("L", "navy"), ("XL", "black")],
    ),
    (
        "quan-jean-slim",
        "quan",
        "Quần jean slim",
        "Quần jean ống côn.",
        399000,
        [("29", "blue"), ("30", "blue"), ("31", "black")],
    ),
    ("quan-kaki-chino", "quan", "Quần kaki chino", "Quần kaki dáng chino.", 349000, [("30", "beige"), ("32", "beige")]),
    ("quan-short-thun", "quan", "Quần short thun", "Quần short mặc nhà.", 159000, [("M", "gray"), ("L", "gray")]),
    (
        "non-luoi-trai",
        "phu-kien",
        "Nón lưỡi trai",
        "Nón thể thao điều chỉnh size.",
        129000,
        [("F", "black"), ("F", "white")],
    ),
    ("that-lung-da", "phu-kien", "Thắt lưng da", "Thắt lưng da tổng hợp.", 219000, [("F", "brown"), ("F", "black")]),
]

_OPENING_BY_INDEX = [50, 40, 35, 30, 25, 20, 15, 10]


def _seed_admin(session, settings) -> None:
    email = settings.bootstrap_admin_email.strip().lower()
    existing = session.execute(
        select(CustomerCredential).where(func.lower(CustomerCredential.email_normalized) == email)
    ).scalar_one_or_none()
    if existing is not None:
        customer = session.get(Customer, existing.customer_id)
        if customer is None or customer.role != "admin":
            raise RuntimeError(f"Bootstrap admin email {email} is already used by a non-admin account.")
        return

    customer = Customer(
        public_id=_pid("admin", email),
        display_name=settings.bootstrap_admin_display_name.strip(),
        role="admin",
        status="active",
        data_origin="manual",
    )
    session.add(customer)
    session.flush()
    session.add(
        CustomerCredential(
            customer_id=customer.customer_id,
            email_normalized=email,
            password_hash=hash_password(settings.bootstrap_admin_password),
            is_enabled=True,
        )
    )
    print(f"[seed_catalog] bootstrap admin ready: {email}")


def _seed_coupons(session) -> None:
    existing = session.execute(
        select(Coupon).where(func.lower(Coupon.code_normalized) == "welcome10")
    ).scalar_one_or_none()
    if existing is not None:
        return
    session.add(
        Coupon(
            public_id=_pid("coupon", "WELCOME10"),
            code_normalized="WELCOME10",
            discount_type="percentage",
            discount_value=10,
            minimum_subtotal_vnd=100_000,
            starts_at=datetime(2025, 1, 1),
            ends_at=datetime(2035, 1, 1),
            is_active=True,
            total_usage_limit=10_000,
            per_customer_usage_limit=1,
            used_count=0,
        )
    )


def seed(session_factory=None, settings=None) -> None:
    if session_factory is None:
        from platform_app.api.db.session import SessionLocal

        session_factory = SessionLocal
    settings = settings or get_settings()
    with session_factory.begin() as session:
        _seed_admin(session, settings)
        _seed_coupons(session)
        cat_by_code = {}
        for entry in CATEGORIES:
            category = session.scalar(select(Category).where(func.lower(Category.code) == entry["code"].lower()))
            if category is None:
                category = Category(public_id=_pid("category", entry["code"]), **entry, is_active=True)
                session.add(category)
                session.flush()
            cat_by_code[entry["code"]] = category

        for slug, cat_code, name, description, base_price, combos in PRODUCTS:
            product = session.scalar(select(Product).where(func.lower(Product.slug) == slug.lower()))
            if product is None:
                product = Product(
                    public_id=_pid("product", slug),
                    category_id=cat_by_code[cat_code].category_id,
                    slug=slug,
                    name=name,
                    description=description,
                    image_url=PRODUCT_IMAGE_URL,
                    is_active=True,
                )
                session.add(product)
                session.flush()
            for idx, (size, color) in enumerate(combos):
                sku = f"{slug}-{size}-{color}".upper()
                variant = session.scalar(select(ProductVariant).where(func.lower(ProductVariant.sku) == sku.lower()))
                if variant is None:
                    variant = ProductVariant(
                        public_id=_pid("variant", sku),
                        product_id=product.product_id,
                        sku=sku,
                        size_code=size,
                        color_code=color,
                        price_vnd=base_price + idx * 10000,
                        is_active=True,
                    )
                    session.add(variant)
                    session.flush()
                elif variant.product_id != product.product_id:
                    raise RuntimeError(f"Seed SKU belongs to another product: {sku}")
                if session.get(Inventory, variant.variant_id) is None:
                    opening = _OPENING_BY_INDEX[idx % len(_OPENING_BY_INDEX)]
                    session.add(
                        Inventory(variant_id=variant.variant_id, opening_on_hand=opening, on_hand=opening, version=0)
                    )
    print("[seed_catalog] reference records ensured; existing balances and credentials preserved.")


if __name__ == "__main__":
    seed()
