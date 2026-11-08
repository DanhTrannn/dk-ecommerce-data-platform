"""Internal API for existing access bindings; not registered on the source API."""

from typing import Literal

from fastapi import Depends, FastAPI, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from platform_app.api.core.errors import forbidden
from platform_app.api.core.handlers import register_exception_handlers
from platform_app.api.db.deps import get_current_customer, verify_csrf
from platform_app.api.modules.auth.cookies import clear_auth_cookies, set_auth_cookies
from platform_app.api.modules.auth.schemas import LoginRequest
from platform_app.api.modules.auth.service import authenticate
from platform_app.access_admin.schemas import BindingChange
from platform_app.access_admin.service import adapter, change_binding, configured_audit, snapshot

app = FastAPI(title="Internal Lakehouse Access Administration")
register_exception_handlers(app)
ROLES = {"admin", "system_admin"}


def require_admin(customer=Depends(get_current_customer)):
    if customer.role not in ROLES:
        raise forbidden("Chỉ admin/system_admin được quản trị quyền hạ tầng.")
    return customer


@app.exception_handler(RequestValidationError)
async def invalid_request(request, error):
    return JSONResponse(
        status_code=422, content={"error": {"code": "VALIDATION_ERROR", "message": "Dữ liệu yêu cầu không hợp lệ."}}
    )


@app.get("/health/live")
def health():
    return {"status": "ok"}


@app.post("/api/v1/access/login")
def login(payload: LoginRequest, response: Response):
    customer, email = authenticate(payload.email, payload.password)
    if customer.role not in ROLES:
        raise forbidden("Chỉ admin/system_admin được quản trị quyền hạ tầng.")
    set_auth_cookies(response, str(customer.public_id))
    from platform_app.api.core.config import get_settings

    return {
        "public_id": str(customer.public_id),
        "role": customer.role,
        "email": email,
        "csrf_cookie_name": get_settings().csrf_cookie_name,
    }


@app.get("/api/v1/access/me")
def me(customer=Depends(require_admin)):
    return {"public_id": str(customer.public_id), "role": customer.role}


@app.post("/api/v1/access/logout", status_code=204)
def logout(response: Response, csrf=Depends(verify_csrf)):
    clear_auth_cookies(response)
    response.status_code = 204
    return response


@app.get("/api/v1/access/audit")
def read_audit(customer=Depends(require_admin)):
    return configured_audit().recent()


@app.get("/api/v1/access/{provider}")
def read_access(provider: Literal["polaris", "minio"], customer=Depends(require_admin)):
    with adapter(provider) as backend:
        return snapshot(backend)


@app.post("/api/v1/access/{provider}/bindings")
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
