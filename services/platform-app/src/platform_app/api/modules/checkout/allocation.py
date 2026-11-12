from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_app.api.core.errors import OUT_OF_STOCK, AppError
from platform_app.api.core.ids import uuid7
from platform_app.api.models.inventory import Inventory
from platform_app.api.models.inventory_tx import InventoryTransaction

def deduct_inventory(
    db: Session, items: list[dict], reference_code: str | None = None
) -> None:
    """Deduct central inventory (inventory table) for online orders and record ledger transaction."""
    now = datetime.now(timezone.utc)
    for item in items:
        stmt = (
            select(Inventory)
            .where(Inventory.variant_id == item["variant_id"])
            .with_for_update()
        )
        inv = db.execute(stmt).scalar_one_or_none()
        if inv is None or inv.on_hand < item["quantity"]:
            raise AppError(OUT_OF_STOCK, "Sản phẩm không đủ tồn kho.", status_code=409)
        inv.on_hand -= item["quantity"]
        inv.version += 1
        db.add(
            InventoryTransaction(
                public_id=uuid7(),
                variant_id=item["variant_id"],
                location_type="central_warehouse",
                store_id=None,
                movement_type="outbound_order",
                quantity_delta=-item["quantity"],
                reference_code=reference_code,
                notes="Xuất kho cho đơn hàng online",
                created_at=now,
            )
        )
