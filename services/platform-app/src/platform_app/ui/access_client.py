"""Per-Streamlit-session HTTP cookies; no root credentials or global auth cache."""

from urllib.parse import urlsplit

import httpx
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AccessUISettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="CONSOLE_", extra="ignore")
    access_admin_url: str = "http://127.0.0.1:8010"

    @field_validator("access_admin_url")
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
            raise ValueError("Administrative origin is invalid")
        return value.rstrip("/")


class AccessUIError(RuntimeError):
    def __init__(self, message, status=503):
        super().__init__(message)
        self.status = status


class AccessSession:
    def __init__(self, url, transport=None):
        self.http = httpx.Client(base_url=url, timeout=60, trust_env=False, follow_redirects=False, transport=transport)
        self.csrf_cookie = "web_csrf"

    def close(self):
        self.http.close()

    def request(self, method, path, payload=None):
        headers = {}
        if method != "GET" and (csrf := self.http.cookies.get(self.csrf_cookie)):
            headers["X-CSRF-Token"] = csrf
        try:
            response = self.http.request(method, "/api/v1/access/" + path, json=payload, headers=headers)
        except httpx.HTTPError:
            raise AccessUIError(
                "Không kết nối được API quản trị. Kiểm tra service lakehouse-access; không tự gửi lại thao tác ghi."
            ) from None
        if not 200 <= response.status_code < 300:
            # Backend returns sanitized messages; no arbitrary upstream body is rendered.
            message = "Yêu cầu quản trị thất bại; tải lại quyền trước khi thao tác tiếp."
            try:
                error = response.json().get("error", {})
                if response.status_code in {400, 401, 403, 409, 422, 502, 503}:
                    message = error.get("message") or message
            except (ValueError, AttributeError):
                pass
            raise AccessUIError(message, response.status_code)
        return response.json() if response.content else None

    def login(self, email, password):
        identity = self.request("POST", "login", {"email": email, "password": password})
        self.csrf_cookie = identity["csrf_cookie_name"]
        return identity
