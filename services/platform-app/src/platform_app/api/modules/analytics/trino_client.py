import base64
import ssl
import json
import logging
import os
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None

from platform_app.api.core.errors import INTERNAL_ERROR, AppError

logger = logging.getLogger(__name__)


class TrinoClient:
    """HTTP REST client to query Trino Coordinator."""

    def __init__(
        self,
        base_url: str | None = None,
        catalog: str = "lakehouse",
        schema: str = "gold",
        user: str = "admin",
    ) -> None:
        self._explicit_url = base_url is not None
        self.base_url = (
            base_url or os.getenv("API_TRINO_URL") or os.getenv("TRINO_URL") or "https://trino:8443"
        ).rstrip("/")
        self.catalog = catalog
        self.schema = schema
        self.user = user

    def _get_headers(self) -> dict[str, str]:
        headers = {
            "X-Trino-User": os.environ.get("TRINO_READER_USER", "analytics_reader")
            if os.environ.get("TRINO_READER_PASSWORD")
            else self.user,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
            "Content-Type": "text/plain",
        }
        if os.environ.get("TRINO_READER_PASSWORD"):
            token = (headers["X-Trino-User"] + ":" + os.environ["TRINO_READER_PASSWORD"]).encode()
            headers["Authorization"] = "Basic " + base64.b64encode(token).decode()
        return headers

    def _get_active_url(self) -> str:
        if self._explicit_url:
            return self.base_url

        candidates = [self.base_url]
        if "trino:8443" in self.base_url:
            candidates.append("https://localhost:8084")
        elif "localhost:8084" in self.base_url:
            candidates.append("https://trino:8443")

        for u in candidates:
            try:
                if httpx is not None:
                    with httpx.Client(timeout=0.5, verify=os.environ.get("TRINO_CA_FILE", True)) as client:
                        resp = client.get(f"{u}/v1/info")
                        if resp.status_code == 200:
                            return u
                else:
                    req = urllib.request.Request(f"{u}/v1/info")
                    with urllib.request.urlopen(
                        req, context=ssl.create_default_context(cafile=os.environ.get("TRINO_CA_FILE")), timeout=0.5
                    ) as resp:
                        if resp.status == 200:
                            return u
            except Exception:
                continue
        return self.base_url

    @staticmethod
    def _coordinator_uri(uri: str, active_url: str) -> str:
        target, origin = urlsplit(uri), urlsplit(active_url)
        return urlunsplit((origin.scheme, origin.netloc, target.path, target.query, ""))

    @staticmethod
    def _raise_query_error(body) -> None:
        if "error" in body:
            raise AppError(
                INTERNAL_ERROR, "Trino query failed: " + body["error"].get("message", "unknown error"), status_code=502
            )

    def execute_query(self, sql: str, timeout: float = 30.0) -> list[dict[str, Any]]:
        """Execute SQL query against Trino and return list of dictionaries."""
        clean_sql = sql.strip().rstrip(";")
        active_url = self._get_active_url()
        url = f"{active_url}/v1/statement"
        headers = self._get_headers()

        if httpx is not None:
            try:
                with httpx.Client(
                    timeout=timeout, headers=headers, verify=os.environ.get("TRINO_CA_FILE", True)
                ) as client:
                    post_resp = client.post(url, headers=headers, content=clean_sql.encode("utf-8"))
                    if post_resp.status_code != 200:
                        raise AppError(
                            INTERNAL_ERROR,
                            f"Lỗi khởi tạo truy vấn Trino ({post_resp.status_code}): {post_resp.text}",
                            status_code=500,
                        )

                    body = post_resp.json()
                    self._raise_query_error(body)
                    next_uri = body.get("nextUri")
                    columns: list[str] = [c["name"] for c in body.get("columns", [])]
                    all_rows: list[list[Any]] = body.get("data", [])

                    start_time = time.time()
                    while next_uri:
                        if time.time() - start_time > timeout:
                            raise AppError(
                                INTERNAL_ERROR,
                                f"Trino query timed out after {timeout} seconds",
                                status_code=504,
                            )

                        poll_resp = client.get(self._coordinator_uri(next_uri, active_url))
                        if poll_resp.status_code != 200:
                            raise AppError(
                                INTERNAL_ERROR,
                                f"Lỗi polling kết quả Trino: {poll_resp.status_code}",
                                status_code=500,
                            )

                        poll_body = poll_resp.json()

                        if "error" in poll_body:
                            err = poll_body["error"]
                            err_msg = err.get("message", "Unknown Trino execution error")
                            logger.error("Trino SQL execution failed for %s: %s", clean_sql, err_msg)
                            raise AppError(
                                INTERNAL_ERROR,
                                f"Lỗi thực thi Trino DWH: {err_msg}",
                                status_code=500,
                            )

                        if not columns and "columns" in poll_body:
                            columns = [c["name"] for c in poll_body["columns"]]

                        if "data" in poll_body:
                            all_rows.extend(poll_body["data"])

                        next_uri = poll_body.get("nextUri")

                    if not columns and not all_rows:
                        return []

                    return [dict(zip(columns, row, strict=False)) for row in all_rows]

            except AppError:
                raise
            except Exception as exc:
                logger.error("Failed to connect or execute Trino statement: %s", exc)
                raise AppError(
                    INTERNAL_ERROR,
                    f"Không thể kết nối đến Trino DWH Coordinator ({self.base_url}): {exc}",
                    status_code=500,
                ) from exc
        else:
            try:
                body_bytes = clean_sql.encode("utf-8")
                req = urllib.request.Request(url, data=body_bytes, headers=headers, method="POST")
                with urllib.request.urlopen(
                    req, context=ssl.create_default_context(cafile=os.environ.get("TRINO_CA_FILE")), timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        raise AppError(
                            INTERNAL_ERROR,
                            f"Lỗi khởi tạo truy vấn Trino ({resp.status})",
                            status_code=500,
                        )
                    body = json.loads(resp.read().decode("utf-8"))
                    self._raise_query_error(body)

                next_uri = body.get("nextUri")
                columns: list[str] = [c["name"] for c in body.get("columns", [])]
                all_rows: list[list[Any]] = body.get("data", [])

                start_time = time.time()
                while next_uri:
                    if time.time() - start_time > timeout:
                        raise AppError(
                            INTERNAL_ERROR,
                            f"Trino query timed out after {timeout} seconds",
                            status_code=504,
                        )

                    poll_req = urllib.request.Request(self._coordinator_uri(next_uri, active_url), headers=headers)
                    with urllib.request.urlopen(
                        poll_req,
                        context=ssl.create_default_context(cafile=os.environ.get("TRINO_CA_FILE")),
                        timeout=timeout,
                    ) as poll_resp:
                        if poll_resp.status != 200:
                            raise AppError(
                                INTERNAL_ERROR,
                                f"Lỗi polling kết quả Trino ({poll_resp.status})",
                                status_code=500,
                            )
                        poll_body = json.loads(poll_resp.read().decode("utf-8"))

                    if "error" in poll_body:
                        err = poll_body["error"]
                        err_msg = err.get("message", "Unknown Trino execution error")
                        logger.error("Trino SQL execution failed for %s: %s", clean_sql, err_msg)
                        raise AppError(
                            INTERNAL_ERROR,
                            f"Lỗi thực thi Trino DWH: {err_msg}",
                            status_code=500,
                        )

                    if not columns and "columns" in poll_body:
                        columns = [c["name"] for c in poll_body["columns"]]

                    if "data" in poll_body:
                        all_rows.extend(poll_body["data"])

                    next_uri = poll_body.get("nextUri")

                if not columns and not all_rows:
                    return []

                return [dict(zip(columns, row, strict=False)) for row in all_rows]

            except AppError:
                raise
            except Exception as exc:
                logger.error("Failed to connect or execute Trino statement: %s", exc)
                raise AppError(
                    INTERNAL_ERROR,
                    f"Không thể kết nối đến Trino DWH Coordinator ({self.base_url}): {exc}",
                    status_code=500,
                ) from exc


# Default singleton instance
default_trino_client = TrinoClient()
