"""Administrative credentials stay in the internal service, outside Streamlit."""

from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AccessSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACCESS_ADMIN_", env_file=".env", extra="ignore", populate_by_name=True
    )
    polaris_url: str = "http://polaris:8181"
    polaris_realm: str = Field(default="POLARIS", validation_alias="POLARIS_REALM")
    polaris_catalog: str = Field(default="lakehouse", validation_alias="POLARIS_CATALOG_NAME")
    polaris_client_id: str = Field(default="root", validation_alias="POLARIS_ROOT_CLIENT_ID")
    polaris_secret: SecretStr = Field(default="", validation_alias="POLARIS_ROOT_CLIENT_SECRET")
    minio_url: str = "http://minio:9000"
    minio_user: str = Field(default="", validation_alias="MINIO_ROOT_USER")
    minio_secret: SecretStr = Field(default="", validation_alias="MINIO_ROOT_PASSWORD")
    minio_polaris: str = Field(default="lakehouse-polaris", validation_alias="MINIO_POLARIS_ACCESS_KEY")
    minio_batch: str = Field(default="lakehouse-batch", validation_alias="MINIO_BATCH_ACCESS_KEY")
    minio_flink: str = Field(default="lakehouse-flink", validation_alias="MINIO_FLINK_ACCESS_KEY")
    minio_trino: str = Field(default="lakehouse-trino", validation_alias="MINIO_TRINO_ACCESS_KEY")
    mc_binary: str = "mc"
    audit_path: Path = Path("data/access-admin/audit.jsonl")

    @field_validator("polaris_url", "minio_url")
    @classmethod
    def origin(cls, value):
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Only HTTP(S) origins without credentials are allowed")
        return value.rstrip("/")

    @property
    def minio_users(self):
        return (self.minio_polaris, self.minio_batch, self.minio_flink, self.minio_trino)
