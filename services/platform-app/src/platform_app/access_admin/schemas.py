import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

NAME = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
BUCKET = r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
PROTECTED_NAMES = {"root", "service_admin", "catalog_admin", "consoleadmin", "diagnostics"}


def editable_name(name):
    return bool(re.fullmatch(NAME, name)) and name.lower() not in PROTECTED_NAMES


class BindingChange(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["principal_role", "catalog_role", "catalog_privilege", "policy"]
    subject: str = Field(pattern=NAME)
    target: str = Field(pattern=NAME)
    granted: bool
    revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    scope: Literal["catalog", "namespace", "table"] = "catalog"
    namespace: list[str] = Field(default_factory=list, max_length=8)
    table: str | None = Field(default=None, pattern=NAME)

    @model_validator(mode="after")
    def valid_scope(self):
        if not editable_name(self.subject) or not editable_name(self.target):
            raise ValueError("Protected identity or role")
        if any(not re.fullmatch(NAME, part) for part in self.namespace):
            raise ValueError("Invalid namespace")
        if self.kind != "catalog_privilege" and (self.scope != "catalog" or self.namespace or self.table):
            raise ValueError("Only privileges accept resource scopes")
        if (
            self.scope == "catalog"
            and (self.namespace or self.table)
            or self.scope != "catalog"
            and not self.namespace
            or self.scope == "namespace"
            and self.table
            or self.scope == "table"
            and not self.table
        ):
            raise ValueError("Invalid privilege scope")
        return self


class ResourceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["user", "bucket", "policy", "principal", "principal_role", "catalog_role", "catalog"]
    name: str = Field(pattern=NAME)
    revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    secret: SecretStr | None = Field(default=None, repr=False)
    bucket: str | None = Field(default=None, pattern=BUCKET)
    prefixes: list[str] = Field(default_factory=list, max_length=20)
    actions: list[Literal["read", "write", "delete"]] = Field(default_factory=list, max_length=3)

    @field_validator("name")
    @classmethod
    def unprotected(cls, value):
        if not editable_name(value):
            raise ValueError("Protected identity or role")
        return value

    @model_validator(mode="after")
    def fields_for_kind(self):
        if self.kind == "user":
            value = self.secret.get_secret_value() if self.secret else ""
            if not 8 <= len(value) <= 128 or not value.isascii() or any(ord(c) < 33 or ord(c) > 126 for c in value):
                raise ValueError("Secret must have 8–128 printable ASCII characters without whitespace")
            if len(self.name) < 3:
                raise ValueError("Access key must have at least 3 characters")
        elif self.secret is not None:
            raise ValueError("Only MinIO users accept a secret")
        if self.kind in {"policy", "catalog"}:
            if not self.bucket:
                raise ValueError("Bucket is required")
        elif self.bucket is not None:
            raise ValueError("Bucket is not applicable")
        if self.kind == "bucket" and not re.fullmatch(BUCKET, self.name):
            raise ValueError("Invalid bucket name")
        if self.kind == "policy":
            if not self.actions or not self.prefixes:
                raise ValueError("Select actions and prefixes")
            for prefix in self.prefixes:
                if (
                    not re.fullmatch(r"[A-Za-z0-9_/-]+/", prefix)
                    or prefix.startswith("/")
                    or "//" in prefix
                    or any(p in {".", ".."} for p in prefix.split("/"))
                ):
                    raise ValueError("Use relative directory prefixes ending in /")
        elif self.actions or self.prefixes:
            raise ValueError("Only policies accept actions and prefixes")
        return self


class ResourceMutation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["user", "bucket", "policy", "principal", "principal_role", "catalog_role", "catalog"]
    name: str = Field(pattern=NAME)
    revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    operation: Literal["update", "delete"]
    confirm_name: str | None = None
    acknowledge_impact: bool = False
    enabled: bool | None = None
    properties: dict[str, str] = Field(default_factory=dict, max_length=20)
    bucket: str | None = Field(default=None, pattern=BUCKET)
    prefixes: list[str] = Field(default_factory=list, max_length=20)
    actions: list[Literal["read", "write", "delete"]] = Field(default_factory=list, max_length=3)

    @model_validator(mode="after")
    def mutation_fields(self):
        if not editable_name(self.name):
            raise ValueError("Protected identity")
        if self.operation == "delete":
            if self.confirm_name != self.name:
                raise ValueError("Type the exact name to confirm deletion")
            if self.enabled is not None or self.properties or self.bucket or self.prefixes or self.actions:
                raise ValueError("Deletion does not accept update fields")
            return self
        if self.kind == "policy":
            ResourceCreate(
                kind="policy",
                name=self.name,
                revision=self.revision,
                bucket=self.bucket,
                prefixes=self.prefixes,
                actions=self.actions,
            )
            if not self.acknowledge_impact or self.properties or self.enabled is not None:
                raise ValueError("Acknowledge shared-policy impact")
        else:
            if self.bucket or self.prefixes or self.actions:
                raise ValueError("Only policies accept storage scope")
            if self.kind == "user":
                if self.enabled is None or self.properties:
                    raise ValueError("Select account status")
            elif self.enabled is not None:
                raise ValueError("Only MinIO accounts accept enabled")
        for key, value in self.properties.items():
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", key) or len(value) > 512:
                raise ValueError("Invalid property")
            if any(word in key.lower() for word in ("secret", "password", "credential", "token")):
                raise ValueError("Do not store credentials in descriptive properties")
        return self


class WorkloadAssignment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workload: Literal["batch-airflow", "flink", "trino-reader", "trino-writer"]
    principal: str | None = Field(default=None, pattern=NAME)
    revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    action: Literal["assign", "remove"] = "assign"
    confirm_replace: bool = False
    confirm_remove: str | None = None

    @model_validator(mode="after")
    def assignment_fields(self):
        if self.action == "assign":
            if not self.principal or not editable_name(self.principal) or self.confirm_remove is not None:
                raise ValueError("Select an unprotected principal")
        elif self.principal is not None or self.confirm_replace or self.confirm_remove != self.workload:
            raise ValueError("Confirm the exact workload to remove its credential")
        return self
