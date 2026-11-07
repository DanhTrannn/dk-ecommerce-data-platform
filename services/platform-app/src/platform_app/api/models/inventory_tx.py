from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Identity, Index, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column

from platform_app.api.db.base import Base
from platform_app.api.db.types import GUID


class InventoryTransaction(Base):
    __tablename__ = "inventory_transactions"

    transaction_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("transaction_id >= 0", name="transaction_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    variant_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("variant_id >= 0", name="variant_id_non_negative"),
        ForeignKey("product_variants.variant_id", ondelete="RESTRICT"),
        nullable=False,
    )
    location_type: Mapped[str] = mapped_column(String(24), nullable=False)  # 'central_warehouse' | 'store'
    store_id: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("store_id >= 0", name="store_id_non_negative"),
        ForeignKey("stores.store_id", ondelete="RESTRICT"),
        nullable=True,
    )
    movement_type: Mapped[str] = mapped_column(String(32), nullable=False)
    quantity_delta: Mapped[int] = mapped_column(Integer, nullable=False)
    reference_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    notes: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )

    __table_args__ = (
        CheckConstraint("location_type in ('central_warehouse','store')", name="ck_inv_tx_location_type"),
        CheckConstraint(
            "(location_type = 'central_warehouse' and store_id is null) or "
            "(location_type = 'store' and store_id is not null)",
            name="ck_inv_tx_location_store_consistency",
        ),
        CheckConstraint("quantity_delta != 0", name="ck_inv_tx_quantity_non_zero"),
        CheckConstraint(
            "(movement_type in ('inbound','transfer_received','return_boom','return_customer','order_cancellation') "
            "and quantity_delta > 0) or "
            "(movement_type in ('outbound_order','outbound_pos','transfer_to_store','exchange_out') "
            "and quantity_delta < 0) or movement_type = 'adjustment'",
            name="ck_inv_tx_movement_direction",
        ),
        CheckConstraint(
            "movement_type in ('inbound','outbound_order','outbound_pos','transfer_to_store','transfer_received','return_boom','return_customer','exchange_out','adjustment','order_cancellation')",
            name="ck_inv_tx_movement_type",
        ),
        Index("uq_inventory_tx_public_id", "public_id", unique=True),
        Index("ix_inv_tx_variant_id", "variant_id"),
        Index("ix_inv_tx_store_id", "store_id"),
        Index("ix_inv_tx_movement_type", "movement_type"),
        Index("ix_inv_tx_created_at_tx_id", "created_at", "transaction_id"),
    )
