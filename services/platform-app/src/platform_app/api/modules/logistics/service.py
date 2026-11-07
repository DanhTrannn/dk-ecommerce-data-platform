from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_app.api.core.errors import not_found
from platform_app.api.core.ids import uuid7
from platform_app.api.models.logistics import DeliveryStaff
from platform_app.api.modules.logistics.schemas import DeliveryStaffCreateRequest, DeliveryStaffUpdateRequest


def list_delivery_staff(db: Session) -> list[DeliveryStaff]:
    return db.execute(select(DeliveryStaff).order_by(DeliveryStaff.staff_id)).scalars().all()


def create_delivery_staff(db: Session, payload: DeliveryStaffCreateRequest) -> DeliveryStaff:
    staff = DeliveryStaff(
        public_id=uuid7(),
        full_name=payload.full_name,
        phone=payload.phone,
        vehicle_plate=payload.vehicle_plate,
        is_active=True,
    )
    db.add(staff)
    db.flush()
    return staff


def update_delivery_staff(db: Session, staff_id: int, payload: DeliveryStaffUpdateRequest) -> None:
    staff = db.execute(select(DeliveryStaff).where(DeliveryStaff.staff_id == staff_id)).scalar_one_or_none()
    if not staff:
        raise not_found("Không tìm thấy nhân viên giao hàng.")
    if payload.full_name is not None:
        staff.full_name = payload.full_name
    if payload.phone is not None:
        staff.phone = payload.phone
    if payload.vehicle_plate is not None:
        staff.vehicle_plate = payload.vehicle_plate
    if payload.is_active is not None:
        staff.is_active = payload.is_active
    db.flush()
