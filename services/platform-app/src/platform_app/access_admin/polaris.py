"""Read live Polaris assignments and edit existing workload bindings only."""

import os
import re
import tempfile
from copy import deepcopy
from urllib.parse import quote

from platform_app.api.core.errors import AppError
from platform_app.access_admin.schemas import editable_name
from platform_app.provisioning.state import save_json, read_json, private_directory
from platform_app.provisioning.client import PolarisClient

PROTECTED_ROLES = {"service_admin", "catalog_admin"}
PRIVILEGES = {
    "CATALOG_MANAGE_CONTENT",
    "CATALOG_READ_PROPERTIES",
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "TABLE_LIST",
    "TABLE_READ_PROPERTIES",
    "TABLE_READ_DATA",
    "VIEW_LIST",
    "VIEW_READ_PROPERTIES",
    "NAMESPACE_CREATE",
    "TABLE_CREATE",
    "TABLE_WRITE_DATA",
    "TABLE_DROP",
    "TABLE_WRITE_PROPERTIES",
    "NAMESPACE_WRITE_PROPERTIES",
    "NAMESPACE_DROP",
}
NAMESPACE_PRIVILEGES = PRIVILEGES - {"CATALOG_READ_PROPERTIES"}
TABLE_PRIVILEGES = {
    "TABLE_READ_PROPERTIES",
    "TABLE_WRITE_PROPERTIES",
    "TABLE_READ_DATA",
    "TABLE_WRITE_DATA",
    "TABLE_DROP",
    "TABLE_LIST",
}


class PolarisAccess:
    provider = "polaris"

    def __init__(self, settings, client=None):
        self.settings = settings
        self.client = client or PolarisClient(settings.polaris_url, settings.polaris_realm)
        if client is None:
            if not settings.polaris_secret.get_secret_value():
                self.client.close()
                raise AppError("ACCESS_NOT_CONFIGURED", "Chưa cấu hình credential quản trị Polaris.", 503)
            try:
                self.client.token = self.client.credentials_token(
                    settings.polaris_client_id, settings.polaris_secret.get_secret_value()
                )
            except Exception:
                self.client.close()
                raise
        self.base = "/api/management/v1"
        self.catalog = self.base + "/catalogs/" + quote(settings.polaris_catalog, safe="")

    def close(self):
        self.client.close()

    def snapshot(self):
        call = self.client.call
        principals = call("GET", self.base + "/principals")["principals"]
        principal_roles = call("GET", self.base + "/principal-roles")["roles"]
        catalog_info = call("GET", self.catalog, missing=True)
        catalog_roles = call("GET", self.catalog + "/catalog-roles")["roles"] if catalog_info else []

        def editable(name):
            return editable_name(name) and name not in PROTECTED_ROLES

        result = {
            "catalog": self.settings.polaris_catalog,
            "catalog_exists": catalog_info is not None,
            "catalog_details": catalog_info or {},
            "namespace_privileges": sorted(NAMESPACE_PRIVILEGES),
            "table_privileges": sorted(TABLE_PRIVILEGES),
            "target": {"origin": self.settings.polaris_url, "realm": self.settings.polaris_realm},
            "principals": [],
            "principal_roles": [],
            "catalog_roles": [],
            "privileges": sorted(PRIVILEGES),
        }
        for item in sorted(principals, key=lambda row: row["name"]):
            name = item["name"]
            if editable(name) and name != self.settings.polaris_client_id:
                roles = call("GET", self.base + "/principals/" + quote(name, safe="") + "/principal-roles")["roles"]
                result["principals"].append({**self._metadata(item), "roles": sorted(role["name"] for role in roles)})
        for item in sorted(principal_roles, key=lambda row: row["name"]):
            name = item["name"]
            if editable(name):
                roles = (
                    call(
                        "GET",
                        self.base
                        + "/principal-roles/"
                        + quote(name, safe="")
                        + "/catalog-roles/"
                        + quote(self.settings.polaris_catalog, safe=""),
                    )["roles"]
                    if catalog_info
                    else []
                )
                result["principal_roles"].append(
                    {**self._metadata(item), "catalog_roles": sorted(role["name"] for role in roles)}
                )
        for item in sorted(catalog_roles, key=lambda row: row["name"]):
            name = item["name"]
            if editable(name):
                grants = call("GET", self.catalog + "/catalog-roles/" + quote(name, safe="") + "/grants")["grants"]
                result["catalog_roles"].append(
                    {**self._metadata(item), "grants": sorted(grants, key=lambda row: str(sorted(row.items())))}
                )
        return result

    def assigned(self, change, snapshot):
        collections = {
            "principal_role": ("principals", "roles", "principal_roles"),
            "catalog_role": ("principal_roles", "catalog_roles", "catalog_roles"),
            "catalog_privilege": ("catalog_roles", "grants", None),
        }
        if change.kind not in collections:
            raise AppError("INVALID_BINDING", "Loại liên kết Polaris không hợp lệ.", 400)
        source, field, targets = collections[change.kind]
        item = next((row for row in snapshot[source] if row["name"] == change.subject), None)
        valid_targets = (
            {row["name"] for row in snapshot[targets]}
            if targets
            else {"catalog": PRIVILEGES, "namespace": NAMESPACE_PRIVILEGES, "table": TABLE_PRIVILEGES}[change.scope]
        )
        if item is None or change.target not in valid_targets:
            raise AppError("INVALID_BINDING", "Chỉ chỉnh liên kết giữa workload/role/quyền hiện có được hỗ trợ.", 400)
        if change.kind == "catalog_privilege":
            if change.scope != "catalog":
                resource = (
                    "/api/catalog/v1/"
                    + quote(self.settings.polaris_catalog, safe="")
                    + "/namespaces/"
                    + quote("\x1f".join(change.namespace), safe="")
                )
                if change.scope == "table":
                    resource += "/tables/" + quote(change.table, safe="")
                if self.client.call("GET", resource, missing=True) is None:
                    raise AppError(
                        "ACCESS_RESOURCE_MISSING",
                        "Namespace hoặc bảng chưa tồn tại; tạo dữ liệu trước khi cấp quyền theo phạm vi này.",
                        400,
                    )
            return any(
                all(grant.get(key) == value for key, value in self.grant(change).items()) for grant in item[field]
            )
        return change.target in item[field]

    def apply(self, change):
        subject, target = quote(change.subject, safe=""), quote(change.target, safe="")
        if change.kind == "principal_role":
            path = self.base + f"/principals/{subject}/principal-roles"
            payload = {"principalRole": {"name": change.target}}
        elif change.kind == "catalog_role":
            path = (
                self.base + f"/principal-roles/{subject}/catalog-roles/" + quote(self.settings.polaris_catalog, safe="")
            )
            payload = {"catalogRole": {"name": change.target}}
        else:
            path = self.catalog + f"/catalog-roles/{subject}/grants"
            payload = {"grant": self.grant(change)}
            return self.client.call("PUT" if change.granted else "POST", path, payload)
        return (
            self.client.call("PUT", path, payload)
            if change.granted
            else self.client.call("DELETE", path + "/" + target)
        )

    @staticmethod
    def grant(change):
        value = {"type": change.scope, "privilege": change.target}
        if change.scope != "catalog":
            value["namespace"] = change.namespace
        if change.scope == "table":
            value["tableName"] = change.table
        return value

    def validate_create(self, change, state):
        if change.kind not in {"principal", "principal_role", "catalog_role", "catalog"}:
            raise AppError("INVALID_RESOURCE", "Loại đối tượng Polaris không hợp lệ.", 400)
        if change.name == self.settings.polaris_client_id:
            raise AppError("INVALID_RESOURCE", "Không thay đổi danh tính quản trị Polaris.", 400)
        if change.kind == "catalog" and change.name != self.settings.polaris_catalog:
            raise AppError("INVALID_RESOURCE", "Tên catalog phải khớp catalog cấu hình của nền tảng.", 400)
        if change.kind == "catalog_role" and not state["catalog_exists"]:
            raise AppError("INVALID_RESOURCE", "Tạo catalog trước khi tạo catalog role.", 400)

    def exists(self, change, state):
        if change.kind == "catalog":
            return state["catalog_exists"]
        collection = {"principal": "principals", "principal_role": "principal_roles", "catalog_role": "catalog_roles"}[
            change.kind
        ]
        return change.name in {row["name"] for row in state[collection]}

    def create(self, change):
        if change.kind == "principal":
            directory = private_directory(self.settings.credentials_dir)
            directory_fd = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            with tempfile.TemporaryFile(dir=directory) as probe:
                probe.write(b"credential-storage-check")
                probe.flush()
                os.fsync(probe.fileno())
            response = self.client.call(
                "POST",
                self.base + "/principals",
                {
                    "principal": {"name": change.name, "properties": {}},
                    "credentialRotationRequired": False,
                },
            )
            credential = response["credentials"]
            if not credential.get("clientId") or not credential.get("clientSecret"):
                raise ValueError("Missing principal credentials")
            save_json(self.settings.credentials_dir / (change.name + ".json"), credential)
            return {"credentials": credential}
        if change.kind in {"principal_role", "catalog_role"}:
            path, key = (
                (self.base + "/principal-roles", "principalRole")
                if change.kind == "principal_role"
                else (self.catalog + "/catalog-roles", "catalogRole")
            )
            self.client.call("POST", path, {key: {"name": change.name, "properties": {}}})
        else:
            self.client.call(
                "POST",
                self.base + "/catalogs",
                {
                    "catalog": {
                        "name": change.name,
                        "type": "INTERNAL",
                        "properties": {"default-base-location": f"s3://{change.bucket}/warehouse"},
                        "storageConfigInfo": {
                            "storageType": "S3",
                            "allowedLocations": [f"s3://{change.bucket}/"],
                            "endpoint": self.settings.minio_url,
                            "endpointInternal": self.settings.minio_url,
                            "region": "us-east-1",
                            "pathStyleAccess": True,
                            "stsUnavailable": True,
                            "kmsUnavailable": True,
                        },
                    }
                },
            )
        return {}

    @staticmethod
    def _metadata(item):
        return {
            "name": item["name"],
            "properties": dict(item.get("properties") or {}),
            "entityVersion": item.get("entityVersion"),
        }

    @staticmethod
    def _reserved_property(key):
        key = re.sub(r"[^a-z0-9]+", ".", key.lower()).strip(".")
        reserved = {
            "name",
            "type",
            "id",
            "entityversion",
            "currententityversion",
            "clientid",
            "default.base.location",
            "warehouse",
            "region",
            "endpoint",
            "enabled",
            "storageconfiginfo",
            "createtimestamp",
            "lastupdatetimestamp",
            "federated",
        }
        return (
            key in reserved
            or key.startswith(
                (
                    "polaris.",
                    "system.",
                    "storage.",
                    "s3.",
                    "aws.",
                    "iceberg.",
                    "oauth.",
                    "client.",
                    "credential.",
                    "path.style.",
                    "sts.",
                    "kms.",
                    "default.",
                )
            )
            or any(word in key for word in ("secret", "password", "token", "credential"))
        )

    def _mutation_item(self, change, state):
        if change.kind == "catalog":
            return state.get("catalog_details") if state["catalog_exists"] else None
        collection = {"principal": "principals", "principal_role": "principal_roles", "catalog_role": "catalog_roles"}[
            change.kind
        ]
        return next((row for row in state[collection] if row["name"] == change.name), None)

    def _mutation_path(self, change):
        name = quote(change.name, safe="")
        return {
            "principal": self.base + "/principals/" + name,
            "principal_role": self.base + "/principal-roles/" + name,
            "catalog_role": self.catalog + "/catalog-roles/" + name,
            "catalog": self.catalog,
        }[change.kind]

    def validate_mutation(self, change, state):
        self._validated_mutation = None
        self._mutation_preserved = None
        if (
            change.kind not in {"principal", "principal_role", "catalog_role", "catalog"}
            or change.operation not in {"update", "delete"}
            or not editable_name(change.name)
            or change.name.lower() in PROTECTED_ROLES
            or change.name == self.settings.polaris_client_id
            or (change.kind == "catalog" and change.name != self.settings.polaris_catalog)
            or change.enabled is not None
            or change.bucket is not None
            or change.prefixes
            or change.actions
        ):
            raise AppError("INVALID_RESOURCE", "Đối tượng hoặc thao tác Polaris không hợp lệ hoặc được bảo vệ.", 400)
        if any(self._reserved_property(key) for key in change.properties):
            raise AppError(
                "INVALID_RESOURCE", "Chỉ cập nhật thuộc tính mô tả; cấu hình và credential được bảo vệ.", 400
            )
        item = self._mutation_item(change, state)
        if item is None:
            raise AppError("ACCESS_RESOURCE_MISSING", "Đối tượng Polaris không còn tồn tại.", 404)
        payload = None
        if change.operation == "update":
            version = item.get("entityVersion")
            if not isinstance(version, int) or isinstance(version, bool):
                raise AppError("ACCESS_STALE", "Thiếu phiên bản đối tượng Polaris; tải lại trạng thái.", 409)
            properties = {
                key: value for key, value in (item.get("properties") or {}).items() if self._reserved_property(key)
            }
            properties.update(change.properties)
            payload = {"currentEntityVersion": version, "properties": properties}
            self._mutation_preserved = {
                "properties": {key: value for key, value in properties.items() if self._reserved_property(key)},
                "catalog": deepcopy({key: item.get(key) for key in ("type", "storageConfigInfo")})
                if change.kind == "catalog"
                else None,
            }
        else:
            if change.properties:
                raise AppError("INVALID_RESOURCE", "Xóa đối tượng không nhận thuộc tính cập nhật.", 400)
            self._validate_delete_dependencies(change)
        self._validated_mutation = (change.kind, change.name, change.operation, dict(change.properties), payload)

    def _validate_delete_dependencies(self, change):
        path = self._mutation_path(change)
        call = self.client.call

        def require_empty(items):
            if items:
                raise AppError(
                    "ACCESS_DEPENDENCIES", "Đối tượng còn dữ liệu, quyền hoặc liên kết; gỡ phụ thuộc trước.", 409
                )

        if change.kind == "principal":
            require_empty(call("GET", path + "/principal-roles")["roles"])
        elif change.kind == "principal_role":
            require_empty(call("GET", path + "/principals")["principals"])
            for catalog in call("GET", self.base + "/catalogs")["catalogs"]:
                require_empty(call("GET", path + "/catalog-roles/" + quote(catalog["name"], safe=""))["roles"])
        elif change.kind == "catalog_role":
            require_empty(call("GET", path + "/principal-roles")["roles"])
            require_empty(call("GET", path + "/grants")["grants"])
        else:
            namespaces = call("GET", "/api/catalog/v1/" + quote(change.name, safe="") + "/namespaces")
            require_empty(namespaces["namespaces"])
            require_empty(namespaces.get("next-page-token"))
            roles = call("GET", self.catalog + "/catalog-roles")["roles"]
            for role in roles:
                role_path = self.catalog + "/catalog-roles/" + quote(role["name"], safe="")
                bindings = call("GET", role_path + "/principal-roles")["roles"]
                grants = call("GET", role_path + "/grants")["grants"]
                if role["name"] != "catalog_admin":
                    require_empty([role])
                require_empty([binding for binding in bindings if binding["name"] != "service_admin"])
                builtin_grants = [
                    {"type": "catalog", "privilege": privilege}
                    for privilege in ("CATALOG_MANAGE_ACCESS", "CATALOG_MANAGE_METADATA")
                ]
                require_empty([grant for grant in grants if grant not in builtin_grants])

    def mutate(self, change):
        validated = getattr(self, "_validated_mutation", None)
        if validated is None or validated[:4] != (change.kind, change.name, change.operation, change.properties):
            raise AppError("INVALID_RESOURCE", "Phải kiểm tra đối tượng trước khi thay đổi Polaris.", 400)
        self._validated_mutation = None
        path = self._mutation_path(change)
        if change.operation == "delete":
            return self.client.call("DELETE", path)
        return self.client.call("PUT", path, validated[4])

    def mutation_matches(self, change, after):
        item = self._mutation_item(change, after)
        if change.operation == "delete":
            return item is None
        preserved = getattr(self, "_mutation_preserved", None)
        if item is not None and preserved is not None:
            if {
                key: value for key, value in (item.get("properties") or {}).items() if self._reserved_property(key)
            } != preserved["properties"]:
                return False
            if (
                change.kind == "catalog"
                and {key: item.get(key) for key in ("type", "storageConfigInfo")} != preserved["catalog"]
            ):
                return False
        return (
            item is not None
            and {
                key: value for key, value in (item.get("properties") or {}).items() if not self._reserved_property(key)
            }
            == change.properties
        )

    def finalize_mutation(self, change, after):
        if change.kind == "principal" and change.operation == "delete" and self.mutation_matches(change, after):
            (self.settings.credentials_dir / (change.name + ".json")).unlink(missing_ok=True)

    def saved_credentials(self, name):
        if not editable_name(name) or name == self.settings.polaris_client_id:
            raise AppError("INVALID_RESOURCE", "Danh tính được bảo vệ.", 400)
        credential = read_json(self.settings.credentials_dir / (name + ".json"))
        if not credential:
            raise AppError("ACCESS_CREDENTIALS_MISSING", "Không có credential đã lưu; không tự reset hoặc rotate.", 404)
        principal = self.client.call("GET", self.base + "/principals/" + quote(name, safe=""))
        if principal.get("clientId") != credential.get("clientId"):
            raise AppError("ACCESS_CREDENTIALS_STALE", "Credential đã lưu không khớp principal hiện tại.", 409)
        self.client.credentials_token(credential["clientId"], credential["clientSecret"])
        return credential
