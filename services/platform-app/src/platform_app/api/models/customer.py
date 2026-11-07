from datetime import datetime

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
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from platform_app.api.db.base import Base
from platform_app.api.db.types import GUID


class Customer(Base):
    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        CheckConstraint("customer_id >= 0", name="customer_id_non_negative"),
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[str] = mapped_column(GUID(), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="customer", server_default=text("'customer'"))
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default=text("'active'"))
    city_id: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("city_id >= 0", name="city_id_non_negative"),
        ForeignKey("cities.city_id", ondelete="SET NULL"),
        nullable=True,
    )
    store_id: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("store_id >= 0", name="store_id_non_negative"),
        ForeignKey("stores.store_id", ondelete="SET NULL"),
        nullable=True,
    )
    is_cod_blocked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    boom_count: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("boom_count <= 4294967295", name="boom_count_uint32_max"),
        CheckConstraint("boom_count >= 0", name="boom_count_non_negative"),
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    cod_reinstated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    cod_reinstated_by: Mapped[int | None] = mapped_column(
        BigInteger,
        CheckConstraint("cod_reinstated_by >= 0", name="cod_reinstated_by_non_negative"),
        ForeignKey("customers.customer_id", ondelete="SET NULL"),
        nullable=True,
    )
    cod_reinstatement_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_origin: Mapped[str] = mapped_column(
        String(16), nullable=False, default="manual", server_default=text("'manual'")
    )
    generation_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    anonymized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )

    credential: Mapped["CustomerCredential"] = relationship(back_populates="customer", uselist=False)

    __table_args__ = (
        CheckConstraint("status in ('active','inactive')", name="status"),
        CheckConstraint(
            "role in ('customer','admin','store_manager','sales_manager','marketing_manager','inventory_manager','operations_manager','system_admin','city_planner')",
            name="role",
        ),
        CheckConstraint("data_origin in ('manual','synthetic')", name="data_origin"),
        Index("uq_customers_public_id", "public_id", unique=True),
        Index("ix_customers_role_status_id", "role", "status", "customer_id"),
        Index("ix_customers_updated_at_customer_id", "updated_at", "customer_id"),
    )


class CustomerCredential(Base):
    __tablename__ = "customer_credentials"

    customer_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("customer_id >= 0", name="customer_id_non_negative"),
        ForeignKey("customers.customer_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    email_normalized: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    password_changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=text("timezone('UTC', statement_timestamp())")
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

    customer: Mapped["Customer"] = relationship(back_populates="credential")

    __table_args__ = (Index("uq_customer_credentials_email_normalized", func.lower(email_normalized), unique=True),)
