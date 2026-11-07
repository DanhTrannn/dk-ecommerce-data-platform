from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    func,
    BigInteger,
    CheckConstraint,
    DateTime,
    FetchedValue,
    ForeignKey,
    Identity,
    Index,
    JSON,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from platform_app.api.db.base import Base
from platform_app.api.db.types import GUID


class ReturnRequest(Base):
    __tablename__ = "return_requests"

    return_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("return_id >= 0", name="return_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    return_code: Mapped[str] = mapped_column(String(64), nullable=False)
    order_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("order_id >= 0", name="order_id_non_negative"),
        ForeignKey("orders.order_id", ondelete="RESTRICT"),
        nullable=False,
    )
    customer_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("customer_id >= 0", name="customer_id_non_negative"),
        ForeignKey("customers.customer_id", ondelete="RESTRICT"),
        nullable=False,
    )
    action_type: Mapped[str] = mapped_column(String(16), nullable=False)  # 'exchange' | 'refund'
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending_review", server_default=text("'pending_review'")
    )
    customer_reason: Mapped[str] = mapped_column(Text, nullable=False)
    image_urls: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    admin_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )

    items: Mapped[list["ReturnItem"]] = relationship(back_populates="return_request", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint("action_type in ('exchange','refund')", name="ck_return_requests_action_type"),
        CheckConstraint(
            "status in ('pending_review','approved','rejected','goods_received','completed','cancelled')",
            name="ck_return_requests_status",
        ),
        Index("uq_return_requests_public_id", "public_id", unique=True),
        Index("uq_return_requests_code", func.lower(return_code), unique=True),
        Index("ix_return_requests_order_id", "order_id"),
        Index("ix_return_requests_customer_id", "customer_id"),
        Index("ix_return_requests_status", "status"),
        Index("ix_return_requests_updated_at_return_id", "updated_at", "return_id"),
    )


class ReturnItem(Base):
    __tablename__ = "return_items"

    return_item_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("return_item_id >= 0", name="return_item_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    return_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("return_id >= 0", name="return_id_non_negative"),
        ForeignKey("return_requests.return_id", ondelete="CASCADE"),
        nullable=False,
    )
    order_item_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("order_item_id >= 0", name="order_item_id_non_negative"),
        ForeignKey("order_items.order_item_id", ondelete="RESTRICT"),
        nullable=False,
    )
    variant_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("variant_id >= 0", name="variant_id_non_negative"),
        ForeignKey("product_variants.variant_id", ondelete="RESTRICT"),
        nullable=False,
    )
    quantity: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("quantity <= 4294967295", name="quantity_uint32_max"),
        CheckConstraint("quantity >= 0", name="quantity_non_negative"),
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    exchange_variant_id: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("exchange_variant_id >= 0", name="exchange_variant_id_non_negative"),
        ForeignKey("product_variants.variant_id", ondelete="RESTRICT"),
        nullable=True,
    )
    refund_amount_vnd: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("refund_amount_vnd >= 0", name="refund_amount_vnd_non_negative"),
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    inspection_status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="pending", server_default=text("'pending'")
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

    return_request: Mapped["ReturnRequest"] = relationship(back_populates="items")

    __table_args__ = (
        CheckConstraint("inspection_status in ('pending','passed','failed')", name="ck_return_items_inspection_status"),
        Index("uq_return_items_public_id", "public_id", unique=True),
        Index("ix_return_items_return_id", "return_id"),
        Index("ix_return_items_variant_id", "variant_id"),
        Index("ix_return_items_updated_at_item_id", "updated_at", "return_item_id"),
    )
