"""HTTP reporting over the independently published hourly route mart."""

from datetime import UTC, datetime, timedelta

from app.modules.analytics.reader import serve
from app.modules.analytics.schemas import (
    HttpMetricsResponse,
    HttpMetricTotals,
    HttpHourMetric,
    HttpRouteMetric,
    HttpLatencyGroup,
)

TABLE = "lakehouse.gold.mart_hourly_route_metrics"
# Durations in the existing mart are rounded group means. Weight by requests,
# and do not interpret an absent group mean as a measured zero duration.
TOTALS = """SUM(total_requests) total_requests,
    SUM(success_2xx_count) success_count,
    SUM(client_error_4xx_count) client_error_count,
    SUM(server_error_5xx_count) server_error_count,
    100.0*(SUM(client_error_4xx_count)+SUM(server_error_5xx_count))/NULLIF(SUM(total_requests),0) error_rate_percent,
    SUM(avg_duration_ms*total_requests)/NULLIF(SUM(CASE WHEN avg_duration_ms IS NOT NULL THEN total_requests END),0) avg_duration_ms"""


def get_http_metrics(days=7, trino_client=None):
    days = max(1, min(int(days), 30))

    def build(reader):
        observed = datetime.fromisoformat(str(reader.publication["observed_at"]).replace(" UTC", "+00:00"))
        observed = observed.replace(tzinfo=UTC) if observed.tzinfo is None else observed.astimezone(UTC)
        start = (observed.date() - timedelta(days=days - 1)).isoformat()
        end = observed.date().isoformat()
        scope = f"FROM {TABLE} WHERE metric_date BETWEEN DATE '{start}' AND DATE '{end}'"
        summary = reader.one(f"SELECT {TOTALS} {scope}")
        hours = reader.query(f"""SELECT CAST(metric_date AS varchar) metric_date,metric_hour,{TOTALS}
            {scope} GROUP BY 1,2 ORDER BY 1,2""")
        routes = reader.query(f"""SELECT http_route,http_request_method,{TOTALS} {scope} GROUP BY 1,2
            ORDER BY (SUM(client_error_4xx_count)+SUM(server_error_5xx_count)) DESC,
                avg_duration_ms DESC NULLS LAST,total_requests DESC,http_route,http_request_method LIMIT 20""")
        latency = reader.query(f"""SELECT CAST(metric_date AS varchar) metric_date,metric_hour,http_route,
            http_request_method,total_requests,p50_duration_ms,p95_duration_ms,p99_duration_ms {scope}
            AND p95_duration_ms IS NOT NULL ORDER BY p95_duration_ms DESC,total_requests DESC,
                metric_date DESC,metric_hour DESC,http_route,http_request_method LIMIT 20""")
        return HttpMetricsResponse(
            days=days,
            window_start=f"{start}T00:00:00+00:00",
            observed_at=observed.isoformat(),
            totals=HttpMetricTotals(**summary),
            hours=[HttpHourMetric(**r) for r in hours],
            routes=[HttpRouteMetric(**r) for r in routes],
            latency_groups=[HttpLatencyGroup(**r) for r in latency],
        )

    return serve(("http-metrics", days), build, trino_client, source="logs")
