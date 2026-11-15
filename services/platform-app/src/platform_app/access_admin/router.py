from typing import Literal

from fastapi import APIRouter, Depends, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from platform_app.api.core.errors import forbidden
from platform_app.api.db.deps import get_current_customer, verify_csrf, get_db
from platform_app.api.core.config import get_settings
from platform_app.api.models.customer import CustomerCredential
from sqlalchemy import select, func
from platform_app.api.modules.auth.cookies import clear_auth_cookies, set_auth_cookies
from platform_app.api.modules.auth.schemas import LoginRequest
from platform_app.api.modules.auth.service import authenticate
from platform_app.access_admin.schemas import (
    BindingChange,
    ResourceCreate,
    ResourceMutation,
    WorkloadAssignment,
    editable_name,
)
from platform_app.access_admin.service import (
    adapter,
    change_binding,
    configured_audit,
    snapshot,
    create_resource,
    mutate_resource,
)


class AccessRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def validated(request):
            try:
                return await handler(request)
            except RequestValidationError:
                request.state.error_code = "VALIDATION_ERROR"
                return JSONResponse(
                    status_code=422,
                    content={"error": {"code": "VALIDATION_ERROR", "message": "Dữ liệu yêu cầu không hợp lệ."}},
                )

        return validated


router = APIRouter(prefix="/api/v1/access", tags=["access"], route_class=AccessRoute)


def require_admin(customer=Depends(get_current_customer), db=Depends(get_db)):
    owner = db.scalar(
        select(CustomerCredential.customer_id).where(
            func.lower(CustomerCredential.email_normalized) == get_settings().bootstrap_admin_email.strip().lower(),
            CustomerCredential.is_enabled.is_(True),
        )
    )
    if customer.role != "admin" or owner != customer.customer_id:
        raise forbidden("Chỉ admin bootstrap được quản trị quyền hạ tầng.")
    return customer


@router.post("/login")
def login(payload: LoginRequest, response: Response):
    customer, email = authenticate(payload.email, payload.password)
    if customer.role != "admin" or email.strip().lower() != get_settings().bootstrap_admin_email.strip().lower():
        raise forbidden("Chỉ admin bootstrap được quản trị quyền hạ tầng.")
    set_auth_cookies(response, str(customer.public_id))

    return {
        "public_id": str(customer.public_id),
        "role": customer.role,
        "email": email,
        "csrf_cookie_name": get_settings().csrf_cookie_name,
    }


@router.get("/me")
def me(customer=Depends(require_admin)):
    return {"public_id": str(customer.public_id), "role": customer.role}


@router.post("/logout", status_code=204)
def logout(response: Response, csrf=Depends(verify_csrf)):
    clear_auth_cookies(response)
    response.status_code = 204
    return response


@router.get("/audit")
def read_audit(customer=Depends(require_admin)):
    return configured_audit().recent()


@router.get("/{provider}")
def read_access(provider: Literal["polaris", "minio"], customer=Depends(require_admin)):
    with adapter(provider) as backend:
        return snapshot(backend)


@router.post("/{provider}/bindings")
def write_access(
    provider: Literal["polaris", "minio"],
    payload: BindingChange,
    customer=Depends(require_admin),
    csrf=Depends(verify_csrf),
):
    # Provider/kind mismatch is rejected before reading administrative credentials.
    if (provider == "minio") != (payload.kind == "policy"):
        from platform_app.api.core.errors import AppError

        raise AppError("INVALID_BINDING", "Loại liên kết không khớp hệ thống.", 400)
    with adapter(provider) as backend:
        return change_binding(backend, payload, customer, configured_audit())


@router.post("/{provider}/resources", status_code=201)
def create_access_resource(
    provider: Literal["polaris", "minio"],
    payload: ResourceCreate,
    response: Response,
    customer=Depends(require_admin),
    csrf=Depends(verify_csrf),
):
    if (provider == "minio") != (payload.kind in {"user", "bucket", "policy"}):
        from platform_app.api.core.errors import AppError

        raise AppError("INVALID_RESOURCE", "Loại đối tượng không khớp hệ thống.", 400)
    response.headers["Cache-Control"] = "no-store"
    with adapter(provider) as backend:
        return create_resource(backend, payload, customer, configured_audit())


@router.post("/polaris/credentials/{name}")
def read_principal_credentials(
    name: str, response: Response, customer=Depends(require_admin), csrf=Depends(verify_csrf)
):
    if not editable_name(name):
        raise forbidden("Danh tính được bảo vệ.")
    response.headers["Cache-Control"] = "no-store"
    audit = configured_audit()
    with audit.lock():
        audit.append(
            {
                "action": "read_credentials",
                "actor": str(customer.public_id),
                "provider": "polaris",
                "name": name,
                "status": "started",
            }
        )
        with adapter("polaris") as backend:
            credentials = backend.saved_credentials(name)
        audit.append(
            {
                "action": "read_credentials",
                "actor": str(customer.public_id),
                "provider": "polaris",
                "name": name,
                "status": "verified",
            }
        )
    return {"credentials": credentials}


@router.patch("/{provider}/resources")
def mutate_access_resource(
    provider: Literal["polaris", "minio"],
    payload: ResourceMutation,
    response: Response,
    customer=Depends(require_admin),
    csrf=Depends(verify_csrf),
):
    if (provider == "minio") != (payload.kind in {"user", "bucket", "policy"}):
        from platform_app.api.core.errors import AppError

        raise AppError("INVALID_RESOURCE", "Loại đối tượng không khớp hệ thống.", 400)
    response.headers["Cache-Control"] = "no-store"
    with adapter(provider) as backend:
        from platform_app.access_admin.workloads import guard_principal_delete

        preflight = (
            (lambda: guard_principal_delete(payload.name, customer, backend=backend))
            if provider == "polaris" and payload.kind == "principal" and payload.operation == "delete"
            else None
        )
        return mutate_resource(backend, payload, customer, configured_audit(), preflight=preflight)


@router.get("/polaris/workload-credentials")
def read_workload_credentials(response: Response, customer=Depends(require_admin)):
    from platform_app.access_admin.workloads import ExporterClient

    response.headers["Cache-Control"] = "no-store"
    return ExporterClient().snapshot(customer)


@router.post("/polaris/workload-credentials")
def write_workload_credentials(
    payload: WorkloadAssignment, response: Response, customer=Depends(require_admin), csrf=Depends(verify_csrf)
):
    from platform_app.access_admin.workloads import assign_workload

    response.headers["Cache-Control"] = "no-store"
    return assign_workload(payload, customer, configured_audit())
