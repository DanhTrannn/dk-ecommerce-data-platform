from fastapi import APIRouter, Depends, Response
from sqlalchemy.orm import Session

from platform_app.api.db.deps import get_current_admin, get_db, verify_csrf
from platform_app.api.models.customer import Customer
from platform_app.api.modules.logistics.schemas import (
    DeliveryStaffCreateRequest,
    DeliveryStaffResponse,
    DeliveryStaffUpdateRequest,
)
from platform_app.api.modules.logistics.service import create_delivery_staff, list_delivery_staff, update_delivery_staff


def _format_uuid(val) -> str:
    if hasattr(val, "hex"):
        return val.hex() if callable(val.hex) else val.hex
    return str(val)


admin_logistics_router = APIRouter(prefix="/admin", tags=["admin-logistics"])
admin_router = admin_logistics_router


@admin_logistics_router.get("/delivery-staff", response_model=list[DeliveryStaffResponse])
def get_staff_list(_: Customer = Depends(get_current_admin), db: Session = Depends(get_db)):
    rows = list_delivery_staff(db)
    return [
        DeliveryStaffResponse(
            staff_id=r.staff_id,
            public_id=_format_uuid(r.public_id),
            full_name=r.full_name,
            phone=r.phone,
            vehicle_plate=r.vehicle_plate,
            is_active=r.is_active,
            created_at=r.created_at,
        )
        for r in rows
    ]


@admin_logistics_router.post("/delivery-staff", response_model=DeliveryStaffResponse, status_code=201)
def add_staff(
    payload: DeliveryStaffCreateRequest,
    _: Customer = Depends(get_current_admin),
    __: None = Depends(verify_csrf),
    db: Session = Depends(get_db),
):
    staff = create_delivery_staff(db, payload)
    db.commit()
    db.refresh(staff)
    return DeliveryStaffResponse(
        staff_id=staff.staff_id,
        public_id=_format_uuid(staff.public_id),
        full_name=staff.full_name,
        phone=staff.phone,
        vehicle_plate=staff.vehicle_plate,
        is_active=staff.is_active,
        created_at=staff.created_at,
    )


@admin_logistics_router.patch("/delivery-staff/{staff_id}", status_code=204)
def patch_staff(
    staff_id: int,
    payload: DeliveryStaffUpdateRequest,
    _: Customer = Depends(get_current_admin),
    __: None = Depends(verify_csrf),
    db: Session = Depends(get_db),
):
    update_delivery_staff(db, staff_id, payload)
    db.commit()
    return Response(status_code=204)
