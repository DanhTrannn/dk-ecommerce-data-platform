"""Read live Polaris assignments and edit existing workload bindings only."""

from urllib.parse import quote

from platform_app.api.core.errors import AppError
from platform_app.provisioning.client import PolarisClient

PRINCIPALS = {"batch_writer", "flink_writer", "trino_reader"}
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
}


class PolarisAccess:
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
        catalog_roles = call("GET", self.catalog + "/catalog-roles")["roles"]

        def editable(name):
            return name not in PROTECTED_ROLES and not name.lower().startswith("system$")

        result = {
            "catalog": self.settings.polaris_catalog,
            "target": {"origin": self.settings.polaris_url, "realm": self.settings.polaris_realm},
            "principals": [],
            "principal_roles": [],
            "catalog_roles": [],
            "privileges": sorted(PRIVILEGES),
        }
        for item in sorted(principals, key=lambda row: row["name"]):
            name = item["name"]
            if name in PRINCIPALS:
                roles = call("GET", self.base + "/principals/" + quote(name, safe="") + "/principal-roles")["roles"]
                result["principals"].append({"name": name, "roles": sorted(role["name"] for role in roles)})
        for item in sorted(principal_roles, key=lambda row: row["name"]):
            name = item["name"]
            if editable(name):
                roles = call(
                    "GET",
                    self.base
                    + "/principal-roles/"
                    + quote(name, safe="")
                    + "/catalog-roles/"
                    + quote(self.settings.polaris_catalog, safe=""),
                )["roles"]
                result["principal_roles"].append(
                    {"name": name, "catalog_roles": sorted(role["name"] for role in roles)}
                )
        for item in sorted(catalog_roles, key=lambda row: row["name"]):
            name = item["name"]
            if editable(name):
                grants = call("GET", self.catalog + "/catalog-roles/" + quote(name, safe="") + "/grants")["grants"]
                result["catalog_roles"].append(
                    {"name": name, "grants": sorted(grants, key=lambda row: str(sorted(row.items())))}
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
        valid_targets = {row["name"] for row in snapshot[targets]} if targets else PRIVILEGES
        if item is None or change.target not in valid_targets:
            raise AppError("INVALID_BINDING", "Chỉ chỉnh liên kết giữa workload/role/quyền hiện có được hỗ trợ.", 400)
        if change.kind == "catalog_privilege":
            return any(
                grant.get("type") == "catalog" and grant.get("privilege") == change.target for grant in item[field]
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
            payload = {"grant": {"type": "catalog", "privilege": change.target}}
            return self.client.call("PUT" if change.granted else "POST", path, payload)
        return (
            self.client.call("PUT", path, payload)
            if change.granted
            else self.client.call("DELETE", path + "/" + target)
        )
