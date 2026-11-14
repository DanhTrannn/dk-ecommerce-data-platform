from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from platform_app.access_admin.schemas import NAME, editable_name

Workload = Literal['batch-airflow', 'flink', 'trino-reader', 'trino-writer']


class Assignment(BaseModel):
    model_config = ConfigDict(extra='forbid')
    action: Literal['assign', 'remove'] = 'assign'
    workload: Workload
    principal: str | None = Field(default=None, pattern=NAME)
    revision: str = Field(pattern=r'^[a-f0-9]{64}$')
    confirm_replace: bool = False
    confirm_remove: str | None = None
    credential_fingerprint: str | None = Field(default=None, pattern=r'^[a-f0-9]{64}$')

    @model_validator(mode='after')
    def valid_assignment(self):
        if self.action == 'assign':
            if not self.principal or not editable_name(self.principal) or not self.credential_fingerprint:
                raise ValueError('A saved editable principal is required')
            if self.confirm_remove is not None:
                raise ValueError('Removal confirmation is not applicable')
        elif (self.principal is not None or self.credential_fingerprint is not None or self.confirm_replace
              or self.confirm_remove != self.workload):
            raise ValueError('Confirm the exact workload to remove its credential file')
        return self
