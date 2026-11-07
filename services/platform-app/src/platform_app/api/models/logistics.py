from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    func,
    BigInteger,
    Boolean,
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


class DeliveryStaff(Base):
    __tablename__ = "delivery_staff"

    staff_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("staff_id >= 0", name="staff_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    full_name: Mapped[str] = mapped_column(String(100), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)
    vehicle_plate: Mapped[str | None] = mapped_column(String(30), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )

    shipments: Mapped[list["Shipment"]] = relationship(back_populates="delivery_staff")

    __table_args__ = (
        Index("uq_delivery_staff_public_id", "public_id", unique=True),
        Index("uq_delivery_staff_phone", func.lower(phone), unique=True),
        Index("ix_delivery_staff_updated_at_staff_id", "updated_at", "staff_id"),
    )


class Shipment(Base):
    __tablename__ = "shipments"

    shipment_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("shipment_id >= 0", name="shipment_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    shipment_code: Mapped[str] = mapped_column(String(64), nullable=False)
    order_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("order_id >= 0", name="order_id_non_negative"),
        ForeignKey("orders.order_id", ondelete="RESTRICT"),
        nullable=False,
    )
    delivery_staff_id: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("delivery_staff_id >= 0", name="delivery_staff_id_non_negative"),
        ForeignKey("delivery_staff.staff_id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="assigned", server_default=text("'assigned'")
    )
    attempt_count: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("attempt_count <= 4294967295", name="attempt_count_uint32_max"),
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    cod_amount_vnd: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("cod_amount_vnd >= 0", name="cod_amount_vnd_non_negative"),
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    cod_collected_vnd: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("cod_collected_vnd >= 0", name="cod_collected_vnd_non_negative"),
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )

    delivery_staff: Mapped["DeliveryStaff | None"] = relationship(back_populates="shipments")
    attempts: Mapped[list["ShipmentAttempt"]] = relationship(back_populates="shipment", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint(
            "status in ('assigned','picked_up','in_transit','delivered','failed','returned_to_warehouse')",
            name="ck_shipments_status",
        ),
        CheckConstraint("attempt_count between 1 and 3", name="attempt_count"),
        CheckConstraint("cod_collected_vnd <= cod_amount_vnd", name="ck_shipments_cod_collected_limit"),
        Index("uq_shipments_public_id", "public_id", unique=True),
        Index("uq_shipments_code", func.lower(shipment_code), unique=True),
        Index("ix_shipments_order_id", "order_id"),
        Index("ix_shipments_delivery_staff_id", "delivery_staff_id"),
        Index("ix_shipments_status", "status"),
        Index("ix_shipments_updated_at_shipment_id", "updated_at", "shipment_id"),
    )


class ShipmentAttempt(Base):
    __tablename__ = "shipment_attempts"

    attempt_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("attempt_id >= 0", name="attempt_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    shipment_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("shipment_id >= 0", name="shipment_id_non_negative"),
        ForeignKey("shipments.shipment_id", ondelete="RESTRICT"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(
        BigInteger, CheckConstraint("attempt_number <= 4294967295", name="attempt_number_uint32_max"), nullable=False
    )
    delivery_staff_id: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("delivery_staff_id >= 0", name="delivery_staff_id_non_negative"),
        ForeignKey("delivery_staff.staff_id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_buyer_fault: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )

    shipment: Mapped["Shipment"] = relationship(back_populates="attempts")
    delivery_staff: Mapped["DeliveryStaff | None"] = relationship()

    __table_args__ = (
        CheckConstraint("attempt_number between 1 and 3", name="ck_shipment_attempts_attempt_number"),
        CheckConstraint(
            "status in ('attempted_failed', 'attempted_success', 'failed', 'delivered')",
            name="ck_shipment_attempts_status",
        ),
        Index("uq_shipment_attempts_public_id", "public_id", unique=True),
        Index("uq_shipment_attempts_idempotency_key", func.lower(idempotency_key), unique=True),
        Index("uq_shipment_attempts_shipment_attempt_number", "shipment_id", "attempt_number", unique=True),
        Index("ix_shipment_attempts_delivery_staff_id", "delivery_staff_id"),
        Index("ix_shipment_attempts_attempted_at", "attempted_at"),
    )
