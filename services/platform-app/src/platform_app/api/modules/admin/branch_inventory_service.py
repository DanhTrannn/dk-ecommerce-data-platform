from datetime import UTC, datetime

from sqlalchemy import select

from platform_app.api.core.errors import OUT_OF_STOCK, VALIDATION_ERROR, AppError, forbidden, not_found
from platform_app.api.core.ids import uuid7
from platform_app.api.db.uow import run_in_transaction
from platform_app.api.models.customer import Customer
from platform_app.api.models.inventory import Inventory
from platform_app.api.models.inventory_tx import InventoryTransaction
from platform_app.api.models.multicity import Store, StoreInventory
from platform_app.api.modules.admin.branch_inventory_schemas import UpdateStockRequest
from platform_app.api.modules.admin.schemas import RefillInventoryRequest


def check_store_inventory_permission(actor: Customer, target_store_id: int, target_city_id: int) -> None:
    if actor.role == "admin":
        return
    if actor.role == "store_manager":
        if actor.store_id != target_store_id:
            raise forbidden("Store manager can only update own store inventory")
        return
    raise forbidden("No inventory edit permission")


def adjust_branch_stock(actor: Customer, payload: UpdateStockRequest) -> None:
    def work(db):
        store = db.get(Store, payload.store_id)
        if store is None:
            raise not_found("Không tìm thấy cửa hàng.")
        check_store_inventory_permission(actor, payload.store_id, store.city_id)
        inventory = db.scalar(
            select(StoreInventory)
            .where(
                StoreInventory.store_id == payload.store_id,
                StoreInventory.variant_id == payload.variant_id,
            )
            .with_for_update()
        )
        if inventory is None:
            raise not_found("Không tìm thấy tồn kho cho store và variant này.")
        delta = payload.on_hand - inventory.on_hand
        if not -2147483648 <= delta <= 2147483647:
            raise AppError(VALIDATION_ERROR, "Biến động tồn kho vượt miền số ledger.", status_code=400)
        if delta == 0:
            return
        inventory.on_hand = payload.on_hand
        inventory.version += 1
        db.add(
            InventoryTransaction(
                public_id=uuid7(),
                variant_id=payload.variant_id,
                location_type="store",
                store_id=payload.store_id,
                movement_type="adjustment",
                quantity_delta=delta,
                reference_code=f"ADJ-{uuid7().hex}",
                notes=f"Điều chỉnh bởi customer_id={actor.customer_id}",
            )
        )

    run_in_transaction(work)


def transfer_to_store(payload: RefillInventoryRequest) -> None:
    def work(db):
        store = db.get(Store, payload.store_id)
        if store is None:
            raise not_found("Không tìm thấy cửa hàng.")
        inventory = db.scalar(select(Inventory).where(Inventory.variant_id == payload.variant_id).with_for_update())
        if inventory is None or inventory.on_hand < payload.quantity:
            raise AppError(OUT_OF_STOCK, "Kho tổng không đủ tồn kho.", status_code=409)
        branch = db.scalar(
            select(StoreInventory)
            .where(
                StoreInventory.store_id == payload.store_id,
                StoreInventory.variant_id == payload.variant_id,
            )
            .with_for_update()
        )
        now = datetime.now(UTC).replace(tzinfo=None)
        inventory.on_hand -= payload.quantity
        inventory.version += 1
        inventory.updated_at = now
        if branch is None:
            branch = StoreInventory(
                store_id=payload.store_id,
                variant_id=payload.variant_id,
                opening_on_hand=0,
                on_hand=0,
                version=0,
                updated_at=now,
            )
            db.add(branch)
        branch.on_hand += payload.quantity
        branch.version += 1
        branch.updated_at = now
        reference = f"TRANSFER-{uuid7().hex}"
        for location, store_id, movement, delta in (
            ("central_warehouse", None, "transfer_to_store", -payload.quantity),
            ("store", payload.store_id, "transfer_received", payload.quantity),
        ):
            db.add(
                InventoryTransaction(
                    public_id=uuid7(),
                    variant_id=payload.variant_id,
                    location_type=location,
                    store_id=store_id,
                    movement_type=movement,
                    quantity_delta=delta,
                    reference_code=reference,
                    created_at=now,
                )
            )

    run_in_transaction(work)
