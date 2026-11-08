"""Restricted mc adapter; credentials are environment-only and never CLI arguments."""

import json
import os
import subprocess
import tempfile
from urllib.parse import quote, urlsplit, urlunsplit

from platform_app.api.core.errors import AppError

POLICIES = {"lakehouse-polaris", "lakehouse-spark", "lakehouse-flink", "lakehouse-trino"}


def canonical_policy(value):
    # IAM statements/actions/resources are sets in this readout. MinIO may
    # serialize them in a different order on each GET; ordering is not a change.
    if isinstance(value, dict):
        return {key: canonical_policy(item) for key, item in value.items()}
    if isinstance(value, list):
        return sorted((canonical_policy(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    return value


class MinioAccess:
    def __init__(self, settings, runner=None):
        self.settings, self.runner = settings, runner or subprocess.run

    def close(self):
        pass

    def command(self, *args):
        settings = self.settings
        if not settings.minio_user or not settings.minio_secret.get_secret_value():
            raise AppError("ACCESS_NOT_CONFIGURED", "Chưa cấu hình credential quản trị MinIO.", 503)
        parsed = urlsplit(settings.minio_url)
        credentials = (
            quote(settings.minio_user, safe="") + ":" + quote(settings.minio_secret.get_secret_value(), safe="")
        )
        host = urlunsplit((parsed.scheme, credentials + "@" + parsed.netloc, "", "", ""))
        try:
            with tempfile.TemporaryDirectory(prefix="access-mc-") as temporary:
                result = self.runner(
                    [settings.mc_binary, "--json", "--no-color", *args],
                    env={
                        "PATH": "/usr/local/bin:" + os.defpath,
                        "HOME": temporary,
                        "MC_CONFIG_DIR": temporary,
                        "MC_HOST_accessadmin": host,
                        "MC_DISABLE_PAGER": "1",
                    },
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
            if result.returncode:
                raise AppError(
                    "ACCESS_UPSTREAM_ERROR",
                    "MinIO từ chối hoặc chưa xác định kết quả; tải lại quyền trước khi thao tác tiếp.",
                    502,
                )
            rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            if any(row.get("status") == "error" for row in rows):
                raise ValueError("MinIO error")
            return rows
        except AppError:
            raise
        except Exception:
            raise AppError(
                "ACCESS_UPSTREAM_ERROR", "Không đọc được phản hồi MinIO; không tự thử lại thao tác ghi.", 502
            ) from None

    def snapshot(self):
        listed = self.command("admin", "policy", "list", "accessadmin")
        names = {row.get("policy") or row.get("policyName") for row in listed}
        policies = []
        for name in sorted(POLICIES & names):
            rows = self.command("admin", "policy", "info", "accessadmin", name)
            document = rows[0].get("policyInfo", {}).get("Policy")
            if isinstance(document, str):
                document = json.loads(document)
            if not isinstance(document, dict):
                raise AppError("ACCESS_UPSTREAM_ERROR", "Định dạng policy MinIO không được hỗ trợ.", 502)
            policies.append({"name": name, "document": canonical_policy(document)})
        listed_users = self.command("admin", "user", "list", "accessadmin")
        users = []
        for row in listed_users:
            name = row.get("accessKey")
            if name in self.settings.minio_users and name != self.settings.minio_user:
                info = self.command("admin", "user", "info", "accessadmin", name)[0]
                direct = info.get("policyName") or ""
                users.append(
                    {
                        "name": name,
                        "status": info.get("userStatus", info.get("status")),
                        "policies": sorted(filter(None, direct.split(","))),
                        "groups": sorted(info.get("memberOf") or []),
                    }
                )
        return {
            "target": self.settings.minio_url,
            "users": sorted(users, key=lambda row: row["name"]),
            "policies": policies,
        }

    def assigned(self, change, snapshot):
        user = next((row for row in snapshot["users"] if row["name"] == change.subject), None)
        if (
            change.kind != "policy"
            or user is None
            or change.target not in {row["name"] for row in snapshot["policies"]}
        ):
            raise AppError("INVALID_BINDING", "Chỉ gán/gỡ policy hiện có cho workload MinIO được cấu hình.", 400)
        return change.target in user["policies"]

    def apply(self, change):
        return self.command(
            "admin",
            "policy",
            "attach" if change.granted else "detach",
            "accessadmin",
            change.target,
            "--user",
            change.subject,
        )
