from datetime import datetime

from sqlalchemy import BigInteger, Boolean, CheckConstraint, DateTime, FetchedValue, ForeignKey, Identity, Index, text
from sqlalchemy.orm import Mapped, mapped_column

from platform_app.api.db.base import Base


class WishlistItem(Base):
    __tablename__ = "wishlist_items"

    wishlist_item_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        primary_key=True,
        autoincrement=True,
    )
    customer_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("customers.customer_id", ondelete="RESTRICT"),
        nullable=False,
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("products.product_id", ondelete="RESTRICT"),
        nullable=False,
    )
    is_present: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    first_added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("statement_timestamp()")
    )
    last_added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("statement_timestamp()")
    )
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
        server_onupdate=FetchedValue(),
    )

    __table_args__ = (
        CheckConstraint(
            "(is_present IS TRUE and removed_at is null) or (is_present IS FALSE and removed_at is not null)",
            name="presence_removed_consistency",
        ),
        CheckConstraint("last_added_at >= first_added_at", name="last_added_after_first"),
        CheckConstraint(
            "removed_at is null or removed_at >= last_added_at",
            name="removed_after_last_added",
        ),
        Index(
            "uq_wishlist_items_customer_id_product_id",
            "customer_id",
            "product_id",
            unique=True,
        ),
        Index(
            "ix_wishlist_items_customer_present_last_added_id",
            "customer_id",
            "is_present",
            "last_added_at",
            "wishlist_item_id",
        ),
        Index(
            "ix_wishlist_items_product_id_wishlist_item_id",
            "product_id",
            "wishlist_item_id",
        ),
        Index(
            "ix_wishlist_items_updated_at_wishlist_item_id",
            "updated_at",
            "wishlist_item_id",
        ),
    )
