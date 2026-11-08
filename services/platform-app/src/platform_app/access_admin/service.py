"""Serialize checked bindings, audit before writes and verify remote outcomes."""

import hashlib
import json
import uuid
from contextlib import contextmanager

from pydantic import ValidationError

from platform_app.api.core.errors import AppError
from platform_app.access_admin.audit import Audit
from platform_app.access_admin.minio import MinioAccess
from platform_app.access_admin.polaris import PolarisAccess
from platform_app.access_admin.settings import AccessSettings


def revision(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


@contextmanager
def adapter(provider):
    try:
        settings = AccessSettings()
        backend = PolarisAccess(settings) if provider == "polaris" else MinioAccess(settings)
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
        state = backend.snapshot()
        return {**state, "revision": revision(state)}
    except AppError:
        raise
    except Exception:
        raise AppError(
            "ACCESS_UNAVAILABLE", "Không đọc được quyền hiện tại; không hiển thị dữ liệu cũ như dữ liệu mới.", 503
        ) from None


def change_binding(backend, change, actor, audit):
    operation_id = uuid.uuid4().hex
    record = {
        "operation_id": operation_id,
        "actor": str(actor.public_id),
        "actor_role": actor.role,
        "provider": "polaris" if change.kind != "policy" else "minio",
        "change": change.model_dump(exclude={"revision"}),
    }
    try:
        with audit.lock():
            before = snapshot(backend)
            if before["revision"] != change.revision:
                raise AppError("ACCESS_STALE", "Quyền đã thay đổi; tải lại trước khi áp dụng.", 409)
            assigned = backend.assigned(change, before)
            audit.append({**record, "status": "started", "revision": before["revision"]})
            if assigned == change.granted:
                audit.append({**record, "status": "noop"})
                return {"operation_id": operation_id, "status": "noop"}
            try:
                backend.apply(change)
                after = snapshot(backend)
                if backend.assigned(change, after) != change.granted:
                    raise RuntimeError("Outcome mismatch")
            except Exception:
                audit.append({**record, "status": "unknown"})
                raise AppError(
                    "ACCESS_OUTCOME_UNKNOWN",
                    "Chưa xác minh được kết quả. Tải lại quyền; không tự gửi lại thao tác.",
                    502,
                    {"operation_id": operation_id},
                ) from None
            audit.append({**record, "status": "verified", "revision": after["revision"]})
            return {"operation_id": operation_id, "status": "verified"}
    except AppError:
        raise
    except OSError:
        raise AppError(
            "ACCESS_AUDIT_UNAVAILABLE",
            "Không lưu được audit. Kiểm tra trạng thái thực tế trước khi thao tác tiếp.",
            503,
            {"operation_id": operation_id},
        ) from None


def configured_audit():
    try:
        return Audit(AccessSettings().audit_path)
    except (ValidationError, OSError):
        raise AppError("ACCESS_AUDIT_UNAVAILABLE", "Chưa cấu hình nơi lưu audit quản trị.", 503) from None
