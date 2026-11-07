from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, FetchedValue, ForeignKey, Index, text
from sqlalchemy.orm import Mapped, mapped_column

from platform_app.api.db.base import Base


class Inventory(Base):
    __tablename__ = "inventory"

    variant_id: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("variant_id >= 0", name="variant_id_non_negative"),
        ForeignKey("product_variants.variant_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    opening_on_hand: Mapped[int] = mapped_column(BigInteger, nullable=False)
    on_hand: Mapped[int] = mapped_column(BigInteger, nullable=False)
    version: Mapped[int] = mapped_column(
        BigInteger,
        CheckConstraint("version >= 0", name="version_non_negative"),
        nullable=False,
        default=0,
        server_default=text("'0'"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=text("timezone('UTC', statement_timestamp())"),
        server_onupdate=FetchedValue(),
    )

    __table_args__ = (
        CheckConstraint("opening_on_hand >= 0", name="opening_on_hand_non_negative"),
        CheckConstraint("on_hand >= 0", name="on_hand_non_negative"),
        Index("ix_inventory_updated_at_variant_id", "updated_at", "variant_id"),
    )
