"""Validated scenario inputs and server-controlled HTTP configuration."""

from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ScenarioSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCENARIO_", env_file=".env", extra="ignore")
    api_base_url: str = "http://127.0.0.1:8000"
    http_timeout_seconds: float = Field(default=15, ge=1, le=120, allow_inf_nan=False)

    @field_validator("api_base_url")
    @classmethod
    def validate_origin(cls, value):
        parts = urlsplit(value)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.path not in {"", "/"}
            or parts.query
            or parts.fragment
        ):
            raise ValueError("API base URL must be an HTTP(S) origin without credentials")
        _ = parts.port
        return value.rstrip("/")


class PurchaseConfig(BaseModel):
    model_config = {"extra": "forbid"}
    purchases: int = Field(default=5, ge=1, le=1000)
    interval_seconds: float = Field(default=2, ge=0, le=3600, allow_inf_nan=False)
    quantity: int = Field(default=1, ge=1, le=20)
    payment_method: str = Field(default="cod", pattern="^(cod|vietqr)$")
    receiver_name: str = Field(default="Scenario customer", min_length=1, max_length=160)
    receiver_phone: str = Field(default="0000000000", min_length=1, max_length=32)
    shipping_address_text: str = Field(default="Scenario, Quận 1, TP. Hồ Chí Minh", min_length=1, max_length=1000)
    shipping_city_code: str | None = Field(default=None, min_length=1, max_length=32)
