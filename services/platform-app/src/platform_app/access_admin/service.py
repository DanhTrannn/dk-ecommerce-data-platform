"""Run administrative Polaris writes under one audit lock, recording each before and after."""

import uuid
from contextlib import contextmanager

from pydantic import ValidationError

from platform_app.access_admin.audit import Audit
from platform_app.access_admin.polaris import PolarisAccess
from platform_app.access_admin.settings import AccessSettings
from platform_app.api.core.errors import AppError


@contextmanager
def polaris():
    try:
        backend = PolarisAccess(AccessSettings())
    except Exception:
        raise AppError(
            "ACCESS_UNAVAILABLE", "Không kết nối được quản trị hạ tầng; kiểm tra cấu hình phía server.", 503
        ) from None
    try:
        yield backend
    finally:
        backend.close()


def snapshot(backend):
    try:
        return backend.snapshot()
    except AppError:
        raise
    except Exception:
        raise AppError("ACCESS_UNAVAILABLE", "Không đọc được quyền hiện tại.", 503) from None


def configured_audit():
    try:
        return Audit(AccessSettings().audit_path)
    except (ValidationError, OSError):
        raise AppError("ACCESS_AUDIT_UNAVAILABLE", "Chưa cấu hình nơi lưu audit quản trị.", 503) from None


def audited(change, action, actor, audit, write):
    """Run `write` under the audit lock, recording it as started, then done, rejected or failed.

    The lock serializes the single admin's writes; a write that fails is not retried, since its outcome at Polaris
    is not known.
    """
    operation_id = uuid.uuid4().hex
    record = {
        "operation_id": operation_id,
        "actor": str(actor.public_id),
        "actor_role": actor.role,
        "provider": "polaris",
        "action": action,
        "change": change.model_dump(),
    }
    try:
        with audit.lock():
            audit.append({**record, "status": "started"})
            try:
                result = write() or {}
            except AppError:
                audit.append({**record, "status": "rejected"})
                raise
            except Exception:
                audit.append({**record, "status": "failed"})
                raise AppError(
                    "ACCESS_FAILED",
                    "Thao tác thất bại. Tải lại quyền để xem trạng thái hiện tại; không tự gửi lại.",
                    502,
                    {"operation_id": operation_id},
                ) from None
            audit.append({**record, "status": "done"})
            return {"operation_id": operation_id, "status": "done", **result}
    except OSError:
        raise AppError(
            "ACCESS_AUDIT_UNAVAILABLE",
            "Không ghi được audit. Kiểm tra trạng thái thực tế trước khi thao tác tiếp.",
            503,
            {"operation_id": operation_id},
        ) from None


def change_binding(backend, change, actor, audit):
    def write():
        if backend.assigned(change, snapshot(backend)) != change.granted:
            backend.apply(change)

    return audited(change, "grant" if change.granted else "revoke", actor, audit, write)


def create_resource(backend, change, actor, audit):
    def write():
        state = snapshot(backend)
        backend.validate_create(change, state)
        if backend.exists(change, state):
            raise AppError("ACCESS_EXISTS", "Đối tượng đã tồn tại; không ghi đè hoặc đổi secret.", 409)
        return backend.create(change)

    return audited(change, "create", actor, audit, write)


def mutate_resource(backend, change, actor, audit, *, preflight=None):
    def write():
        payload = backend.validate_mutation(change, snapshot(backend))
        if preflight is not None:
            preflight()
        backend.mutate(change, payload)

    return audited(change, change.operation, actor, audit, write)
