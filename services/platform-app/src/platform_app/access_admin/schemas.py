"""Narrow mutation contracts: existing bindings only, no account or policy edits."""

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class BindingChange(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["principal_role", "catalog_role", "catalog_privilege", "policy"]
    subject: str = Field(min_length=1, max_length=128)
    target: str = Field(min_length=1, max_length=128)
    granted: bool
    revision: str = Field(pattern=r"^[a-f0-9]{64}$")
