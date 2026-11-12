"""FastAPI router for analytics endpoints with RBAC enforcement."""

from fastapi import APIRouter, Depends, Query, Response

from platform_app.api.core.errors import FORBIDDEN, AppError
from platform_app.api.db.deps import get_current_staff
from platform_app.api.models.customer import Customer
from platform_app.api.modules.analytics.http_service import get_http_metrics
from platform_app.api.modules.analytics.realtime_service import get_realtime_http
from platform_app.api.modules.analytics.schemas import (
    HttpMetricsResponse,
    RealtimeHttpResponse,
)


router = APIRouter(prefix="/admin/analytics", tags=["http-observability"])


def _require_system_access(actor: Customer) -> None:
    if actor.role not in {"admin", "system_admin"}:
        raise AppError(FORBIDDEN, "Chỉ quản trị viên hoặc quản trị hệ thống được xem dữ liệu HTTP.", status_code=403)


@router.get("/http-realtime", response_model=RealtimeHttpResponse)
def get_analytics_http_realtime(
    response: Response,
    minutes: int = Query(15, ge=5, le=60, description="Rolling event-time window in minutes, UTC [start, observed_at)"),
    actor: Customer = Depends(get_current_staff),
) -> RealtimeHttpResponse:
    """Admin/System Admin: provisional Landing aggregates, one snapshot per read.

    Replays are deduplicated by event ID; conflicting payloads are excluded.
    No raw events, publication requirement, PostgreSQL aggregate or server cache.
    Unavailable Landing returns 503; an empty readable window has null totals.
    """
    _require_system_access(actor)
    response.headers["Cache-Control"] = "no-store"
    return get_realtime_http(minutes=minutes)


@router.get("/http-metrics", response_model=HttpMetricsResponse)
def get_analytics_http_metrics(
    days: int = Query(7, ge=1, le=30, description="UTC calendar dates ending at the log observation date"),
    actor: Customer = Depends(get_current_staff),
) -> HttpMetricsResponse:
    """Admin/System Admin: hourly HTTP reporting pinned to the log publication.

    Empty windows retain null totals; unavailable publications return bounded
    last-good data marked degraded, or 503 when no valid cache exists.
    """
    _require_system_access(actor)
    return get_http_metrics(days=days)
