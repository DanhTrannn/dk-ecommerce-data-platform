"""Read the log publication's Iceberg snapshots, with a bounded last-good cache.

OLTP Gold has no publication: it is rebuilt only after the batch checks pass and is read directly.
"""

import json
import logging
import os
import re
import time
from collections import OrderedDict
from datetime import UTC, datetime
from threading import Lock

from platform_app.api.core.errors import INTERNAL_ERROR, AppError
from platform_app.api.modules.analytics.trino_client import default_trino_client

_cache = OrderedDict()
_lock = Lock()


class PublishedReader:
    def __init__(self, client=None):
        self.client = client or default_trino_client
        rows = self.client.execute_query("""SELECT run_id, published_at, source_event_at, snapshots_json, schema_version, source_mode, observed_at
            FROM lakehouse.system.logs_analytics_publications ORDER BY published_at DESC LIMIT 1""")
        if not rows:
            raise AppError(INTERNAL_ERROR, "Lakehouse chưa có lần công bố hoàn tất.", status_code=503)
        if rows[0].get("schema_version") != 1 or rows[0].get("source_mode") != "logs_rollup":
            raise AppError(INTERNAL_ERROR, "Dữ liệu phân tích cần được xây dựng lại theo schema mới.", status_code=503)
        self.publication = rows[0]
        self.snapshots = json.loads(rows[0]["snapshots_json"])
        if not isinstance(self.snapshots, dict):
            raise AppError(INTERNAL_ERROR, "Publication snapshot map không hợp lệ.", status_code=503)

    def query(self, sql):
        def pin(match):
            table = match.group(0)
            if table not in self.snapshots:
                raise AppError(INTERNAL_ERROR, f"Bảng chưa được công bố: {table}", status_code=503)
            snapshot = self.snapshots[table]
            if snapshot is not None and (type(snapshot) is not int or snapshot <= 0):
                raise AppError(INTERNAL_ERROR, f"Snapshot không hợp lệ: {table}", status_code=503)
            return (
                f"{table} FOR VERSION AS OF {int(snapshot)}"
                if snapshot is not None
                else f"(SELECT * FROM {table} WHERE false)"
            )

        return self.client.execute_query(re.sub(r"lakehouse\.(?:silver|gold)\.[a-z_]+", pin, sql))

    def one(self, sql):
        rows = self.query(sql)
        return rows[0] if rows else {}


def serve(key, build, client=None):
    """Never run business aggregates on PostgreSQL when the analytical reader fails."""
    try:
        reader = PublishedReader(client)
        result = build(reader).model_copy(
            update={
                "data_source": "lakehouse",
                "source_run_id": reader.publication["run_id"],
                "data_updated_at": datetime.fromisoformat(str(reader.publication["published_at"]).replace(" UTC", "+00:00")).replace(tzinfo=UTC).isoformat(),
                "served_from_cache": False,
                "cache_age_seconds": 0,
            }
        )
        with _lock:
            _cache[key] = (time.monotonic(), result.model_copy(deep=True))
            _cache.move_to_end(key)
            while len(_cache) > 128:
                _cache.popitem(last=False)
        return result
    except Exception as error:
        logging.getLogger(__name__).warning("OLAP read failed for %s", key, exc_info=True)
        if isinstance(error, AppError) and error.code != INTERNAL_ERROR:
            raise
        maximum_age = max(0, int(os.environ.get("API_ANALYTICS_CACHE_MAX_AGE_SECONDS", "3600")))
        with _lock:
            cached = _cache.get(key)
        if cached and time.monotonic() - cached[0] <= maximum_age:
            return cached[1].model_copy(
                deep=True,
                update={
                    "is_degraded": True,
                    "served_from_cache": True,
                    "cache_age_seconds": int(time.monotonic() - cached[0]),
                    "degraded_reason": "Lakehouse chưa đọc được; giữ kết quả OLAP đã tải thành công trước đó. Không có truy vấn tổng hợp PostgreSQL thay thế.",
                },
            )
        raise AppError(
            INTERNAL_ERROR,
            "Chưa đọc được dữ liệu OLAP đã công bố. Hãy kiểm tra Trino và pipeline công bố dữ liệu.",
            status_code=503,
        ) from error
