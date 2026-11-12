"""Fill the three empty generator tables via real source business services, private fixture only."""

import json

from sqlalchemy import select

from platform_app.api.db.session import SessionLocal
from platform_app.api.models.catalog import ProductVariant
from platform_app.api.models.customer import Customer
from platform_app.api.models.multicity import Store
from platform_app.api.modules.admin.branch_inventory_service import transfer_to_store
from platform_app.api.modules.admin.schemas import RefillInventoryRequest
from platform_app.api.modules.inbound.schemas import CreateInboundReceiptPayload, InboundReceiptItemPayload
from platform_app.api.modules.inbound.service import create_inbound_receipt

with SessionLocal() as db:
    actor = db.scalar(select(Customer).where(Customer.role == "admin").order_by(Customer.customer_id))
    variant = db.scalar(
        select(ProductVariant).where(ProductVariant.is_active == True).order_by(ProductVariant.variant_id)
    )
    store = db.scalar(select(Store).where(Store.is_active == True).order_by(Store.store_id))
    assert actor and variant and store
    variant_id, store_id = variant.variant_id, store.store_id
    receipt = create_inbound_receipt(
        db,
        actor.customer_id,
        CreateInboundReceiptPayload(
            batch_name="Native migration fixture",
            items=[
                InboundReceiptItemPayload(
                    variant_id=variant_id,
                    quantity=5,
                    unit_cost_vnd=int(variant.cost_price_vnd),
                )
            ],
        ),
        idempotency_key="native-batch-fixture-receipt",
    )
transfer_to_store(RefillInventoryRequest(store_id=store_id, variant_id=variant_id, quantity=2))
print(
    "STACK_RESULT " + json.dumps({"inbound_receipt_created": receipt.receipt_id, "store_transfer_created": True}),
    flush=True,
)
