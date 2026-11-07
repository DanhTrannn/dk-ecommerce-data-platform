from fastapi import APIRouter, Depends, Header
from sqlalchemy.orm import Session

from platform_app.api.core.errors import VALIDATION_ERROR, AppError
from platform_app.api.db.deps import get_current_inventory_staff, get_db, verify_csrf
from platform_app.api.models.customer import Customer
from platform_app.api.modules.inbound.schemas import CreateInboundReceiptPayload, InboundReceiptDetailResponse
from platform_app.api.modules.inbound.service import create_inbound_receipt, get_inbound_receipt_detail

router = APIRouter(prefix="/admin/inbound", tags=["admin-inbound"])


def _require_idempotency_key(value: str | None) -> str:
    key = value.strip() if value else ""
    if not key or len(key) > 64:
        raise AppError(
            VALIDATION_ERROR,
            "Idempotency-Key phải có từ 1 đến 64 ký tự.",
            status_code=400,
        )
    return key


@router.post("/receipts", response_model=InboundReceiptDetailResponse, status_code=201)
def create_receipt(
    payload: CreateInboundReceiptPayload,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    actor: Customer = Depends(get_current_inventory_staff),
    _: None = Depends(verify_csrf),
    db: Session = Depends(get_db),
) -> InboundReceiptDetailResponse:
    validated_key = _require_idempotency_key(idempotency_key)
    return create_inbound_receipt(
        db,
        admin_customer_id=actor.customer_id,
        payload=payload,
        idempotency_key=validated_key,
    )




@router.get("/receipts/{receipt_code}", response_model=InboundReceiptDetailResponse)
def get_receipt(
    receipt_code: str,
    _: Customer = Depends(get_current_inventory_staff),
    db: Session = Depends(get_db),
) -> InboundReceiptDetailResponse:
    return get_inbound_receipt_detail(db, receipt_code)
