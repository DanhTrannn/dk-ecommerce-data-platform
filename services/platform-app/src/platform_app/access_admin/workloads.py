import hashlib
import json
import stat
import uuid
from urllib.parse import quote

import httpx

from platform_app.api.core.errors import AppError
from platform_app.access_admin.settings import AccessSettings
from platform_app.access_admin.service import adapter


def credential_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class ExporterClient:
    def __init__(self, settings=None, transport=None):
        self.settings = settings or AccessSettings()
        self.transport = transport

    def _request(self, method, path, actor, payload=None):
        from platform_app.credential_export.auth import make_token

        try:
            file = self.settings.export_key_file
            metadata = file.lstat()
            if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise ValueError("Invalid signing key permissions")
            key = file.read_bytes()
            if len(key) < 32:
                raise ValueError("Invalid signing key")
            token = make_token(key, str(actor.public_id), method, path, payload)
            with httpx.Client(
                base_url=self.settings.export_url,
                timeout=30,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                response = client.request(method, path, json=payload, headers={"Authorization": "Bearer " + token})
        except (OSError, ValueError, httpx.HTTPError):
            raise AppError(
                "EXPORT_UNAVAILABLE",
                "Chưa kết nối hoặc xác thực được service cấp credential; kiểm tra cấu hình phía server. Không tự gửi lại thao tác ghi.",
                503,
            ) from None
        if response.status_code >= 400:
            messages = {
                400: "Thông tin cấp credential không hợp lệ.",
                401: "Service cấp credential chưa được xác thực; kiểm tra cấu hình phía server.",
                404: "Không có credential đã lưu cho principal được chọn.",
                409: "Trạng thái đã thay đổi hoặc workload đang dùng credential khác. Tải lại và xác nhận thay thế nếu cần.",
                422: "Thông tin cấp credential không hợp lệ.",
            }
            status = response.status_code if response.status_code in messages else 503
            raise AppError(
                "EXPORT_REJECTED" if status != 503 else "EXPORT_OUTCOME_UNKNOWN",
                messages.get(
                    status, "Chưa xác minh được kết quả cấp credential; tải lại trạng thái, không tự gửi lại."
                ),
                status,
            )
        try:
            result = response.json()
            if not isinstance(result, dict):
                raise ValueError("Invalid exporter response")
            return result
        except ValueError:
            raise AppError(
                "EXPORT_UNAVAILABLE",
                "Phản hồi cấp credential không hợp lệ; tải lại trạng thái để kiểm tra kết quả.",
                503,
            ) from None

    def snapshot(self, actor):
        return self._request("GET", "/v1/workloads", actor)

    def apply(self, payload, actor):
        return self._request("POST", "/v1/workloads", actor, payload)


def assign_workload(change, actor, audit, exporter=None):
    exporter = exporter or ExporterClient()
    operation_id = uuid.uuid4().hex
    record = {
        "operation_id": operation_id,
        "actor": str(actor.public_id),
        "actor_role": actor.role,
        "provider": "polaris",
        "action": "workload_" + change.action,
        "change": change.model_dump(exclude={"revision"}),
    }
    try:
        with audit.lock():
            payload = change.model_dump(mode="json")
            if change.action == "assign":
                with adapter("polaris") as backend:
                    credential = backend.saved_credentials(change.principal)
                payload["credential_fingerprint"] = credential_digest(credential)
            audit.append({**record, "status": "started"})
            try:
                result = exporter.apply(payload, actor)
                if result.get("status") not in {"verified", "noop"}:
                    raise AppError(
                        "EXPORT_OUTCOME_UNKNOWN", "Chưa xác minh được kết quả; tải lại trạng thái workload.", 503
                    )
            except AppError as error:
                audit.append(
                    {
                        **record,
                        "status": "rejected" if error.status_code in {400, 401, 403, 404, 409, 422} else "unknown",
                    }
                )
                raise
            audit.append({**record, "status": result["status"]})
            return {**result, "operation_id": operation_id}
    except OSError:
        raise AppError(
            "ACCESS_AUDIT_UNAVAILABLE",
            "Không ghi được audit; kiểm tra trạng thái workload trước khi thao tác tiếp.",
            503,
        ) from None


def guard_principal_delete(name, actor, exporter=None, *, backend=None):
    state = (exporter or ExporterClient()).snapshot(actor)
    present = [row for row in state["workloads"] if row.get("file_present", row.get("configured", False))]
    bound = [row["id"] for row in present if row.get("principal") == name]
    if bound:
        raise AppError(
            "ACCESS_DEPENDENCIES",
            "Principal đang cấp credential cho workload: "
            + ", ".join(bound)
            + ". Gỡ hoặc thay liên kết workload trước khi xóa.",
            409,
        )
    if not present:
        return
    if any(not row.get("client_id") for row in present):
        raise AppError(
            "ACCESS_DEPENDENCIES",
            "Có file credential workload chưa xác định được danh tính; sửa hoặc gỡ file trước khi xóa principal.",
            409,
        )
    if backend is None:
        with adapter("polaris") as polaris:
            return guard_principal_delete(name, actor, exporter, backend=polaris)
    principal = backend.client.call("GET", backend.base + "/principals/" + quote(name, safe=""))
    client_id = principal.get("clientId")
    if not client_id:
        raise AppError("ACCESS_UNAVAILABLE", "Chưa xác minh được client ID của principal; không xóa.", 503)
    if any(row["client_id"] == client_id for row in present):
        raise AppError(
            "ACCESS_DEPENDENCIES",
            "Credential của principal đang được workload sử dụng, kể cả file được cấp ngoài Console. Gỡ hoặc thay liên kết trước khi xóa.",
            409,
        )
