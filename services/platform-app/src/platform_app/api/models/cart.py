from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Computed,
    DateTime,
    FetchedValue,
    ForeignKey,
    Identity,
    Index,
    String,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from platform_app.api.db.base import Base
from platform_app.api.db.types import GUID


class Cart(Base):
    __tablename__ = "carts"

    cart_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("cart_id >= 0", name="cart_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[str] = mapped_column(GUID(), nullable=False)
    customer_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("customer_id >= 0", name="customer_id_non_negative"),
        ForeignKey("customers.customer_id", ondelete="RESTRICT"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default=text("'active'"))
    active_customer_guard: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("active_customer_guard >= 0", name="active_customer_guard_non_negative"),
        Computed("(case when status = 'active' then customer_id else null end)", persisted=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )
    checked_out_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)

    items: Mapped[list["CartItem"]] = relationship(back_populates="cart")

    __table_args__ = (
        CheckConstraint("status in ('active','checked_out')", name="status"),
        Index("uq_carts_public_id", "public_id", unique=True),
        Index("uq_carts_active_customer_guard", "active_customer_guard", unique=True),
        Index("ix_carts_customer_id_created_at_cart_id", "customer_id", "created_at", "cart_id"),
        Index("ix_carts_updated_at_cart_id", "updated_at", "cart_id"),
    )


class CartItem(Base):
    __tablename__ = "cart_items"

    cart_item_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("cart_item_id >= 0", name="cart_item_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    cart_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("cart_id >= 0", name="cart_id_non_negative"),
        ForeignKey("carts.cart_id", ondelete="RESTRICT"),
        nullable=False,
    )
    variant_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("variant_id >= 0", name="variant_id_non_negative"),
        ForeignKey("product_variants.variant_id", ondelete="RESTRICT"),
        nullable=False,
    )
    quantity: Mapped[int] = mapped_column(
        BigInteger, CheckConstraint("quantity <= 4294967295", name="quantity_uint32_max"), nullable=False
    )
    is_present: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    first_added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )

    cart: Mapped["Cart"] = relationship(back_populates="items")

    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint(
            "(is_present IS TRUE and removed_at is null) or (is_present IS FALSE and removed_at is not null)",
            name="presence_removed_consistency",
        ),
        Index("uq_cart_items_cart_id_variant_id", "cart_id", "variant_id", unique=True),
        Index("ix_cart_items_variant_id_cart_item_id", "variant_id", "cart_item_id"),
        Index("ix_cart_items_updated_at_cart_item_id", "updated_at", "cart_item_id"),
    )
