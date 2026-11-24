import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

NAME = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
BUCKET = r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
PROTECTED_NAMES = {"root", "service_admin", "catalog_admin", "consoleadmin", "diagnostics"}
Workload = Literal["batch-airflow", "trino-reader", "trino-writer"]


def editable_name(name):
    return bool(re.fullmatch(NAME, name)) and name.lower() not in PROTECTED_NAMES


class BindingChange(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["principal_role", "catalog_role", "catalog_privilege"]
    subject: str = Field(pattern=NAME)
    target: str = Field(pattern=NAME)
    granted: bool
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
    kind: Literal["principal", "principal_role", "catalog_role", "catalog"]
    name: str = Field(pattern=NAME)
    bucket: str | None = Field(default=None, pattern=BUCKET)

    @field_validator("name")
    @classmethod
    def unprotected(cls, value):
        if not editable_name(value):
            raise ValueError("Protected identity or role")
        return value

    @model_validator(mode="after")
    def bucket_for_catalog(self):
        if (self.kind == "catalog") != (self.bucket is not None):
            raise ValueError("Only a catalog takes a bucket, and it needs one")
        return self


class ResourceMutation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["principal", "principal_role", "catalog_role", "catalog"]
    name: str = Field(pattern=NAME)
    operation: Literal["update", "delete"]
    confirm_name: str | None = None
    properties: dict[str, str] = Field(default_factory=dict, max_length=20)

    @model_validator(mode="after")
    def mutation_fields(self):
        if not editable_name(self.name):
            raise ValueError("Protected identity")
        if self.operation == "delete":
            if self.confirm_name != self.name:
                raise ValueError("Type the exact name to confirm deletion")
            if self.properties:
                raise ValueError("Deletion does not accept update fields")
            return self
        for key, value in self.properties.items():
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", key) or len(value) > 512:
                raise ValueError("Invalid property")
            if any(word in key.lower() for word in ("secret", "password", "credential", "token")):
                raise ValueError("Do not store credentials in descriptive properties")
        return self


class WorkloadAssignment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workload: Workload
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
