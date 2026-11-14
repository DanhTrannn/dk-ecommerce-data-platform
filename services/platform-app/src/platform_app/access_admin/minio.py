"""Restricted mc adapter; credentials are environment-only and never CLI arguments."""

import json
import os
import fnmatch
import re
import subprocess
import tempfile
from urllib.parse import quote, urlsplit, urlunsplit

from platform_app.api.core.errors import AppError
from platform_app.access_admin.schemas import editable_name
from platform_app.access_admin.policy import data_policy, policy_document

BUILTIN_POLICIES = {"readonly", "readwrite", "writeonly", "consoleadmin", "diagnostics"}

POLICIES = {"lakehouse-polaris", "lakehouse-batch", "lakehouse-flink", "lakehouse-trino"}


def canonical_policy(value):
    # IAM statements/actions/resources are sets in this readout. MinIO may
    # serialize them in a different order on each GET; ordering is not a change.
    if isinstance(value, dict):
        return {key: canonical_policy(item) for key, item in value.items()}
    if isinstance(value, list):
        return sorted((canonical_policy(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    return value


class MinioAccess:
    provider = "minio"

    def __init__(self, settings, runner=None):
        self.settings, self.runner = settings, runner or subprocess.run

    def close(self):
        pass

    def command(self, *args, input=None, allow_no_tags=False):
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
                    input=input,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
            rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            if (
                allow_no_tags
                and len(rows) == 1
                and rows[0].get("status") == "error"
                and rows[0].get("error", {}).get("message", "").startswith("No tags found  for ")
            ):
                return []
            if result.returncode:
                raise AppError(
                    "ACCESS_UPSTREAM_ERROR",
                    "MinIO từ chối hoặc chưa xác định kết quả; tải lại quyền trước khi thao tác tiếp.",
                    502,
                )
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
        all_documents = []
        for name in sorted(n for n in names if n):
            rows = self.command("admin", "policy", "info", "accessadmin", name)
            document = rows[0].get("policyInfo", {}).get("Policy")
            if isinstance(document, str):
                document = json.loads(document)
            if not isinstance(document, dict):
                raise AppError("ACCESS_UPSTREAM_ERROR", "Định dạng policy MinIO không được hỗ trợ.", 502)
            entry = {"name": name, "document": canonical_policy(document)}
            all_documents.append(entry)
            if data_policy(document) and editable_name(name) and name.lower() not in BUILTIN_POLICIES:
                policies.append(entry)
        listed_users = self.command("admin", "user", "list", "accessadmin")
        users = []
        for row in listed_users:
            name = row.get("accessKey")
            if name and editable_name(name) and name != self.settings.minio_user:
                info = self.command("admin", "user", "info", "accessadmin", name)[0]
                direct = info.get("policyName") or ""
                users.append(
                    {
                        "name": name,
                        "status": info.get("userStatus", info.get("status")),
                        "policies": sorted(filter(None, direct.split(","))),
                        "groups": sorted(info.get("memberOf") or []),
                        "service_accounts": self.child_accounts("admin", "user", "svcacct", "ls", "accessadmin", name),
                        "temporary_accounts": self.child_accounts(
                            "admin", "accesskey", "ls", "accessadmin", name, "--temp-only"
                        ),
                    }
                )
        buckets = sorted(row["key"].rstrip("/") for row in self.command("ls", "accessadmin") if row.get("key"))
        details = []
        for bucket in buckets:
            tags = self.command("tag", "list", "accessadmin/" + bucket, allow_no_tags=True)
            properties = tags[0].get("tagset") if tags else {}
            if not isinstance(properties, dict):
                raise AppError("ACCESS_UPSTREAM_ERROR", "Định dạng tag MinIO không được hỗ trợ.", 502)
            details.append({"name": bucket, "properties": properties})
        bindings = []
        for row in self.command("admin", "policy", "entities", "accessadmin"):
            result = row.get("result", {})
            if not isinstance(result, dict) or not isinstance(result.get("policyMappings", []), list):
                raise AppError("ACCESS_UPSTREAM_ERROR", "Định dạng liên kết MinIO không được hỗ trợ.", 502)
            for mapping in result.get("policyMappings") or []:
                bindings.append(
                    {
                        "policy": mapping["policy"],
                        "users": sorted(mapping.get("users") or []),
                        "groups": sorted(mapping.get("groups") or []),
                    }
                )
        return {
            "target": self.settings.minio_url,
            "buckets": buckets,
            "bucket_details": details,
            "all_policy_documents": all_documents,
            "policy_bindings": sorted(bindings, key=lambda row: row["policy"]),
            "policy_names": sorted(n for n in names if n),
            "users": sorted(users, key=lambda row: row["name"]),
            "policies": policies,
        }

    def child_accounts(self, *args):
        rows = self.command(*args)
        if any(not isinstance(row.get("accessKey"), str) or not row["accessKey"] for row in rows):
            raise AppError("ACCESS_UPSTREAM_ERROR", "Không đọc được danh sách credential phụ thuộc MinIO.", 502)
        return sorted(row["accessKey"] for row in rows)

    def assigned(self, change, snapshot):
        user = next((row for row in snapshot["users"] if row["name"] == change.subject), None)
        if (
            change.kind != "policy"
            or user is None
            or change.target not in {row["name"] for row in snapshot["policies"]}
        ):
            raise AppError(
                "INVALID_BINDING", "Chỉ gán/gỡ policy dữ liệu hiện có cho account MinIO không được bảo vệ.", 400
            )
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

    def validate_create(self, change, state):
        if (
            change.kind not in {"user", "bucket", "policy"}
            or not editable_name(change.name)
            or change.name == self.settings.minio_user
            or (change.kind == "policy" and change.name.lower() in BUILTIN_POLICIES)
        ):
            raise AppError("INVALID_RESOURCE", "Đối tượng MinIO không hợp lệ hoặc được bảo vệ.", 400)
        if change.kind == "policy" and change.bucket not in state["buckets"]:
            raise AppError("INVALID_RESOURCE", "Tạo bucket trước khi tạo policy.", 400)

    def exists(self, change, state):
        if change.kind == "bucket":
            return change.name in state["buckets"]
        if change.kind == "policy":
            return change.name in state["policy_names"]
        return change.name in {row["name"] for row in state["users"]} or change.name == self.settings.minio_user

    def create(self, change):
        if change.kind == "user":
            self.command(
                "admin",
                "user",
                "add",
                "accessadmin",
                input=change.name + "\n" + change.secret.get_secret_value() + "\n",
            )
        elif change.kind == "bucket":
            self.command("mb", "accessadmin/" + change.name)
        elif change.kind == "policy":
            with tempfile.TemporaryDirectory(prefix="access-policy-") as temporary:
                path = os.path.join(temporary, "policy.json")
                with open(path, "w") as output:
                    json.dump(policy_document(change), output)
                self.command("admin", "policy", "create", "accessadmin", change.name, path)
        else:
            raise AppError("INVALID_RESOURCE", "Loại đối tượng không được hỗ trợ.", 400)
        return {}

    def validate_mutation(self, change, state):
        if (
            change.kind not in {"user", "bucket", "policy"}
            or not editable_name(change.name)
            or change.name == self.settings.minio_user
            or (change.kind == "policy" and change.name.lower() in BUILTIN_POLICIES)
            or change.operation not in {"update", "delete"}
        ):
            raise AppError("INVALID_RESOURCE", "Đối tượng MinIO không hợp lệ hoặc được bảo vệ.", 400)
        if not self.exists(change, state):
            raise AppError("ACCESS_NOT_FOUND", "Đối tượng không còn tồn tại; tải lại trạng thái.", 404)
        if change.kind == "policy" and change.name not in {p["name"] for p in state["policies"]}:
            raise AppError("INVALID_RESOURCE", "Chỉ sửa hoặc xóa policy dữ liệu được hỗ trợ.", 400)
        if change.operation == "update":
            if change.kind == "user" and not isinstance(change.enabled, bool):
                raise AppError("INVALID_RESOURCE", "Chọn trạng thái account MinIO.", 400)
            if change.kind == "policy" and change.bucket not in state["buckets"]:
                raise AppError("INVALID_RESOURCE", "Bucket của policy không còn tồn tại.", 400)
            if change.kind == "bucket":
                self.validate_tags(change.properties)
            return
        blocked = False
        if change.kind == "user":
            user = next(u for u in state["users"] if u["name"] == change.name)
            blocked = bool(
                user["policies"] or user["groups"] or user.get("service_accounts") or user.get("temporary_accounts")
            )
            blocked |= any(change.name in p["users"] for p in state["policy_bindings"])
        elif change.kind == "policy":
            blocked = any(p["policy"] == change.name and (p["users"] or p["groups"]) for p in state["policy_bindings"])
        else:
            for policy in state["all_policy_documents"]:
                if policy["name"].lower() in BUILTIN_POLICIES:
                    continue
                for statement in policy["document"].get("Statement", []):
                    resources = statement.get("Resource", [])
                    resources = [resources] if isinstance(resources, str) else resources
                    if "NotResource" in statement or any(
                        r == "*"
                        or (
                            r.startswith("arn:aws:s3:::")
                            and fnmatch.fnmatchcase(change.name, r.removeprefix("arn:aws:s3:::").split("/", 1)[0])
                        )
                        for r in resources
                    ):
                        blocked = True
            if not blocked:
                target = "accessadmin/" + change.name
                blocked = bool(self.command("ls", "--recursive", "--versions", target))
                if not blocked:
                    blocked = bool(self.command("ls", "--recursive", "--incomplete", target))
        if blocked:
            raise AppError("ACCESS_DEPENDENCY", "Gỡ liên kết hoặc dọn dữ liệu phụ thuộc trước khi xóa.", 409)

    @staticmethod
    def validate_tags(properties):
        if len(properties) > 10 or any(
            not 1 <= len(key) <= 128
            or len(value) > 256
            or not re.fullmatch(r"[A-Za-z0-9 _.:/@\-]+", key)
            or not re.fullmatch(r"[A-Za-z0-9 _.:/@\-]*", value)
            or key.lower().startswith("aws:")
            for key, value in properties.items()
        ):
            raise AppError(
                "INVALID_RESOURCE",
                "Tag: tối đa 10, key 1–128, value 0–256; dùng chữ, số, khoảng trắng hoặc _ . : / @ -.",
                400,
            )

    def mutate(self, change):
        if change.operation == "delete":
            if change.kind == "bucket":
                self.command("rb", "accessadmin/" + change.name)
            else:
                self.command("admin", change.kind, "remove", "accessadmin", change.name)
        elif change.kind == "user":
            self.command("admin", "user", "enable" if change.enabled else "disable", "accessadmin", change.name)
        elif change.kind == "policy":
            self.create(change)
        elif change.kind == "bucket":
            self.validate_tags(change.properties)
            target = "accessadmin/" + change.name
            if change.properties:
                self.command("tag", "set", target, "&".join(f"{k}={v}" for k, v in sorted(change.properties.items())))
            else:
                self.command("tag", "remove", target)

    def mutation_matches(self, change, after):
        if change.operation == "delete":
            return not self.exists(change, after)
        if change.kind == "user":
            user = next((u for u in after["users"] if u["name"] == change.name), None)
            return user is not None and user["status"] == ("enabled" if change.enabled else "disabled")
        if change.kind == "policy":
            return any(
                p["name"] == change.name and p["document"] == canonical_policy(policy_document(change))
                for p in after["policies"]
            )
        return any(b["name"] == change.name and b["properties"] == change.properties for b in after["bucket_details"])
