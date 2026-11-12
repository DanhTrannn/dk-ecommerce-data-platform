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
from sqlalchemy.orm import Mapped, mapped_column

from platform_app.api.db.base import Base


class City(Base):
    __tablename__ = "cities"

    city_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        primary_key=True,
        autoincrement=True,
    )
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("statement_timestamp()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
        server_onupdate=FetchedValue(),
    )

    __table_args__ = (
        Index("uq_cities_code", func.lower(code), unique=True),
        Index("ix_cities_updated_at_city_id", "updated_at", "city_id"),
    )


class Store(Base):
    __tablename__ = "stores"

    store_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        primary_key=True,
        autoincrement=True,
    )
    city_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("cities.city_id", ondelete="RESTRICT"),
        nullable=False,
    )
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    address: Mapped[str] = mapped_column(String(500), nullable=False)
    phone: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("statement_timestamp()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
        server_onupdate=FetchedValue(),
    )

    __table_args__ = (
        Index("uq_stores_code", func.lower(code), unique=True),
        Index("ix_stores_city_id_store_id", "city_id", "store_id"),
        Index("ix_stores_updated_at_store_id", "updated_at", "store_id"),
    )


class StoreInventory(Base):
    __tablename__ = "store_inventory"

    store_inventory_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        autoincrement=True,
        unique=True,
    )
    store_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("stores.store_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    variant_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("product_variants.variant_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    on_hand: Mapped[int] = mapped_column(BigInteger, nullable=False)
    opening_on_hand: Mapped[int] = mapped_column(BigInteger, nullable=False)
    version: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("version >= 0", name="version_non_negative"),
        nullable=False,
        default=0,
        server_default=text("'0'"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
        server_onupdate=FetchedValue(),
    )

    __table_args__ = (
        CheckConstraint("opening_on_hand >= 0", name="opening_on_hand_non_negative"),
        CheckConstraint("on_hand >= 0", name="on_hand_non_negative"),
        Index("uq_store_inventory_store_id_variant_id", "store_id", "variant_id", unique=True),
        Index("ix_store_inventory_updated_at_store_id", "updated_at", "store_id"),
    )
