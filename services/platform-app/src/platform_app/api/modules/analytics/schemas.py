"""HTTP publication and provisional Landing observation contracts."""

from typing import Literal

from pydantic import BaseModel, Field


class AnalyticsDataStatus(BaseModel):
    """Expose the validated Lakehouse publication and bounded cached-result age."""

    data_source: str = "lakehouse"
    is_degraded: bool = False
    degraded_reason: str | None = None
    source_run_id: str | None = None
    data_updated_at: str | None = None
    served_from_cache: bool = False
    cache_age_seconds: int = 0


class HttpMetricTotals(BaseModel):
    total_requests: int | None = None
    success_count: int | None = None
    client_error_count: int | None = None
    server_error_count: int | None = None
    error_rate_percent: float | None = None
    avg_duration_ms: float | None = None


class HttpHourMetric(HttpMetricTotals):
    metric_date: str
    metric_hour: int


class HttpRouteMetric(HttpMetricTotals):
    http_route: str | None
    http_request_method: str | None


class HttpLatencyGroup(BaseModel):
    metric_date: str
    metric_hour: int
    http_route: str | None
    http_request_method: str | None
    total_requests: int
    p50_duration_ms: float | None
    p95_duration_ms: float | None
    p99_duration_ms: float | None


class HttpMetricsResponse(AnalyticsDataStatus):
    days: int
    window_start: str
    observed_at: str
    totals: HttpMetricTotals = Field(default_factory=HttpMetricTotals)
    hours: list[HttpHourMetric] = Field(default_factory=list)
    routes: list[HttpRouteMetric] = Field(default_factory=list)
    latency_groups: list[HttpLatencyGroup] = Field(default_factory=list)


class RealtimeHttpTotals(HttpMetricTotals):
    p95_duration_ms: float | None = None


class RealtimeHttpMinute(RealtimeHttpTotals):
    minute: str


class RealtimeHttpRoute(RealtimeHttpTotals):
    http_route: str
    http_request_method: str


class RealtimeHttpQuality(BaseModel):
    scanned_rows: int = 0
    invalid_rows: int = 0
    replayed_rows: int = 0
    conflicting_event_ids: int = 0
    late_event_ids: int = 0
    future_event_ids: int = 0


class RealtimeHttpResponse(BaseModel):
    data_source: Literal["landing"] = "landing"
    minutes: int
    window_start: str
    observed_at: str
    queried_at: str
    snapshot_id: str | None = None
    snapshot_current_at: str | None = None
    latest_ingest_at: str | None = None
    latest_event_at: str | None = None
    quality: RealtimeHttpQuality = Field(default_factory=RealtimeHttpQuality)
    totals: RealtimeHttpTotals = Field(default_factory=RealtimeHttpTotals)
    series: list[RealtimeHttpMinute] = Field(default_factory=list)
    routes: list[RealtimeHttpRoute] = Field(default_factory=list)
