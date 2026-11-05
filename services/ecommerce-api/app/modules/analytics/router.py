"""FastAPI router for analytics endpoints with RBAC enforcement."""

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

from app.core.errors import FORBIDDEN, AppError
from app.db.deps import get_current_admin, get_current_staff, get_db
from app.models.customer import Customer
from app.modules.admin.schemas import AdminOverviewResponse
from app.modules.admin.service import get_overview
from app.modules.analytics.http_service import get_http_metrics
from app.modules.analytics.realtime_service import get_realtime_http
from app.modules.analytics.schemas import (
    HttpMetricsResponse,
    RealtimeHttpResponse,
    RoleMetricsResponse,
    SalesTrendResponse,
    StoreItemResponse,
)
from app.modules.analytics.service import (
    get_active_stores_list,
    get_role_metrics_data,
    get_sales_trend,
    resolve_effective_store_id,
    validate_role_access,
)

router = APIRouter(prefix="/admin/analytics", tags=["admin-analytics"])


@router.get("/http-realtime", response_model=RealtimeHttpResponse)
def get_analytics_http_realtime(
    response: Response,
    minutes: int = Query(15, ge=5, le=60, description="Rolling event-time window in minutes, UTC [start, observed_at)"),
    actor: Customer = Depends(get_current_staff),
) -> RealtimeHttpResponse:
    """Admin/System Admin: provisional Landing aggregates, one snapshot per read.

    Replays are deduplicated by event ID; conflicting payloads are excluded.
    No raw events, publication requirement, MySQL aggregate or server cache.
    Unavailable Landing returns 503; an empty readable window has null totals.
    """
    validate_role_access(actor=actor, target_role="system")
    response.headers["Cache-Control"] = "no-store"
    return get_realtime_http(minutes=minutes)


@router.get("/stores", response_model=list[StoreItemResponse])
def get_analytics_stores(
    actor: Customer = Depends(get_current_staff),
    db: Session = Depends(get_db),
) -> list[StoreItemResponse]:
    """List available stores for analytics store selection."""
    return get_active_stores_list(actor=actor, db=db)


@router.get("/overview", response_model=AdminOverviewResponse)
def get_analytics_overview(
    db: Session = Depends(get_db),
    admin: Customer = Depends(get_current_admin),
) -> AdminOverviewResponse:
    """Financial & Operational System-wide Overview (Admin only)."""
    return get_overview(db)


@router.get("/role-metrics", response_model=RoleMetricsResponse)
def get_role_metrics(
    target_role: str = Query(..., description="Target business role name or alias"),
    store_id: int | None = Query(None, description="Optional store filter for store metrics"),
    actor: Customer = Depends(get_current_staff),
    db: Session = Depends(get_db),
) -> RoleMetricsResponse:
    """RBAC metrics pinned to completed Iceberg publications, with a last-good OLAP cache."""
    validate_role_access(actor=actor, target_role=target_role, store_id=store_id)
    effective_store_id = resolve_effective_store_id(actor=actor, target_role=target_role, store_id=store_id)
    return get_role_metrics_data(target_role=target_role, store_id=effective_store_id, db=db)


@router.get("/sales-trend", response_model=SalesTrendResponse)
def get_analytics_sales_trend(
    days: int = Query(30, ge=1, le=365, description="Number of days for sales trend"),
    actor: Customer = Depends(get_current_staff),
    db: Session = Depends(get_db),
) -> SalesTrendResponse:
    """Daily revenue, COGS, and profit trend (Admin or Sales Manager only)."""
    if actor.role not in ("admin", "sales_manager"):
        raise AppError(
            FORBIDDEN,
            "Chỉ quản trị viên hoặc trưởng phòng kinh doanh mới có quyền truy cập biểu đồ xu hướng.",
            status_code=403,
        )
    return get_sales_trend(days=days, db=db)


@router.get("/http-metrics", response_model=HttpMetricsResponse)
def get_analytics_http_metrics(
    days: int = Query(7, ge=1, le=30, description="UTC calendar dates ending at the log observation date"),
    actor: Customer = Depends(get_current_staff),
) -> HttpMetricsResponse:
    """Admin/System Admin: hourly HTTP reporting pinned to the log publication.

    Empty windows retain null totals; unavailable publications return bounded
    last-good data marked degraded, or 503 when no valid cache exists.
    """
    validate_role_access(actor=actor, target_role="system")
    return get_http_metrics(days=days)
