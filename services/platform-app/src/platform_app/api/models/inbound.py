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
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from platform_app.api.db.base import Base
from platform_app.api.db.types import GUID


class InboundReceipt(Base):
    __tablename__ = "inbound_receipts"

    receipt_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("receipt_id >= 0", name="receipt_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    receipt_code: Mapped[str] = mapped_column(String(64), nullable=False)
    batch_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="completed", server_default=text("'completed'")
    )
    total_items_count: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("total_items_count <= 4294967295", name="total_items_count_uint32_max"),
        nullable=False,
        default=0,
        server_default=text("'0'"),
    )
    total_cost_vnd: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("'0'"))
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by_customer_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("created_by_customer_id >= 0", name="created_by_customer_id_non_negative"),
        ForeignKey("customers.customer_id", ondelete="RESTRICT"),
        nullable=False,
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

    items: Mapped[list["InboundReceiptItem"]] = relationship(back_populates="receipt", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint("status = 'completed'", name="ck_inbound_receipts_status"),
        CheckConstraint("total_items_count >= 0", name="ck_inbound_receipts_items_count"),
        CheckConstraint("total_cost_vnd >= 0", name="ck_inbound_receipts_total_cost"),
        Index("uq_inbound_receipts_public_id", "public_id", unique=True),
        Index("uq_inbound_receipts_code", func.lower(receipt_code), unique=True),
        Index("ix_inbound_receipts_created_at_id", "created_at", "receipt_id"),
    )


class InboundReceiptItem(Base):
    __tablename__ = "inbound_receipt_items"

    item_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("item_id >= 0", name="item_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    receipt_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("receipt_id >= 0", name="receipt_id_non_negative"),
        ForeignKey("inbound_receipts.receipt_id", ondelete="CASCADE"),
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
    unit_cost_vnd: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("'0'"))
    total_cost_vnd: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("'0'"))
    previous_cost_price_vnd: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("previous_cost_price_vnd >= 0", name="previous_cost_price_vnd_non_negative"),
        nullable=False,
        default=0,
        server_default=text("'0'"),
    )
    new_cost_price_vnd: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("new_cost_price_vnd >= 0", name="new_cost_price_vnd_non_negative"),
        nullable=False,
        default=0,
        server_default=text("'0'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )

    receipt: Mapped["InboundReceipt"] = relationship(back_populates="items")

    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_inbound_items_quantity"),
        CheckConstraint("unit_cost_vnd >= 0", name="ck_inbound_items_unit_cost"),
        CheckConstraint("total_cost_vnd >= 0", name="ck_inbound_items_total_cost"),
        Index("uq_inbound_items_public_id", "public_id", unique=True),
        Index("ix_inbound_items_receipt_id", "receipt_id"),
        Index("ix_inbound_items_variant_id", "variant_id"),
    )
