"""Bounded HTTP observation of one Landing snapshot, independent of publications."""

import logging
from datetime import UTC, datetime, timedelta

from platform_app.api.core.errors import INTERNAL_ERROR, AppError
from platform_app.api.modules.analytics.schemas import (
    RealtimeHttpMinute,
    RealtimeHttpQuality,
    RealtimeHttpResponse,
    RealtimeHttpRoute,
    RealtimeHttpTotals,
)
from platform_app.api.modules.analytics.trino_client import default_trino_client

TABLE = "lakehouse.landing.access_logs"
METRICS = """NULLIF(count_if(eligible), 0) total_requests,
    IF(count_if(eligible) > 0, count_if(eligible AND http_status_code BETWEEN 200 AND 399)) success_count,
    IF(count_if(eligible) > 0, count_if(eligible AND http_status_code BETWEEN 400 AND 499)) client_error_count,
    IF(count_if(eligible) > 0, count_if(eligible AND http_status_code BETWEEN 500 AND 599)) server_error_count,
    100.0 * count_if(eligible AND http_status_code >= 400) / NULLIF(count_if(eligible), 0) error_rate_percent,
    avg(duration_ns / 1e6) FILTER (WHERE eligible) avg_duration_ms,
    approx_percentile(duration_ns / 1e6, 0.95) FILTER (WHERE eligible) p95_duration_ms"""


def _stamp(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value).replace(" UTC", "+00:00"))
    return (parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)).isoformat()


def get_realtime_http(minutes=15, trino_client=None, *, as_of=None):
    """No last-good server cache: failure is 503, never a fresh-looking old window."""
    minutes = int(minutes)
    if not 5 <= minutes <= 60:
        raise ValueError("Realtime window must be between 5 and 60 minutes")
    observed = as_of or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("as_of must contain a timezone")
    observed = observed.astimezone(UTC)
    start = observed - timedelta(minutes=minutes)
    lo = start.strftime("%Y-%m-%d %H:%M:%S.%f")
    hi = observed.strftime("%Y-%m-%d %H:%M:%S.%f")
    client = trino_client or default_trino_client
    try:
        # $history identifies the current main snapshot even after a rollback.
        snapshots = client.execute_query("""SELECT snapshot_id, made_current_at
            FROM lakehouse.landing."access_logs$history"
            ORDER BY made_current_at DESC LIMIT 1""", timeout=8)
        snapshot = snapshots[0] if snapshots else None
        source = f"{TABLE} FOR VERSION AS OF {int(snapshot['snapshot_id'])}" if snapshot else TABLE
        empty = "" if snapshot else "AND false"
        rows = client.execute_query(f"""WITH raw AS (
            SELECT *, coalesce(regexp_like(event_id, '^[0-9a-f]{{32}}$')
                AND event_ts IS NOT NULL AND http_status_code BETWEEN 100 AND 599
                AND duration_ns >= 0 AND http_method IS NOT NULL AND http_route IS NOT NULL
                AND raw_payload IS NOT NULL, false) valid
            FROM {source}
            WHERE ingest_ts <= TIMESTAMP '{hi}'
                AND (event_ts >= TIMESTAMP '{lo}' AND event_ts < TIMESTAMP '{hi}'
                     OR ingest_ts >= TIMESTAMP '{lo}') {empty}
        ), identities AS (
            SELECT event_id, valid, count(*) copies,
                count(DISTINCT IF(valid, sha256(to_utf8(raw_payload)))) variants,
                max(ingest_ts) ingest_ts, max(event_ts) event_ts,
                max(http_route) http_route, max(http_method) http_method,
                max(http_status_code) http_status_code, max(duration_ns) duration_ns
            FROM raw GROUP BY event_id, valid
        ), events AS (
            SELECT *, date_trunc('minute', event_ts) minute,
                valid AND variants = 1 AND event_ts >= TIMESTAMP '{lo}' AND event_ts < TIMESTAMP '{hi}' eligible
            FROM identities
        ), metrics AS (
            SELECT CASE WHEN grouping(minute) = 0 THEN 'minute'
                        WHEN grouping(http_route) = 0 THEN 'route' ELSE 'total' END kind,
                CAST(minute AS varchar) minute, http_route, http_method http_request_method,
                CAST(max(event_ts) FILTER (WHERE eligible) AS varchar) latest_event_at,
                CAST(max(ingest_ts) AS varchar) latest_ingest_at,
                coalesce(sum(copies), 0) scanned_rows,
                coalesce(sum(IF(NOT valid, copies, 0)), 0) invalid_rows,
                coalesce(sum(IF(valid, copies - variants, 0)), 0) replayed_rows,
                count_if(valid AND variants > 1) conflicting_event_ids,
                count_if(valid AND variants = 1 AND event_ts < TIMESTAMP '{lo}') late_event_ids,
                count_if(valid AND variants = 1 AND event_ts >= TIMESTAMP '{hi}') future_event_ids,
                {METRICS}
            FROM events GROUP BY GROUPING SETS ((), (minute), (http_route, http_method))
            HAVING count_if(eligible) > 0 OR grouping(minute, http_route, http_method) = 7
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY kind ORDER BY
                (client_error_count + server_error_count) DESC NULLS LAST,
                total_requests DESC, http_route, http_request_method) position
            FROM metrics
        ) SELECT * FROM ranked WHERE kind <> 'route' OR position <= 20
            ORDER BY kind, minute, position""", timeout=8)
        total = next(row for row in rows if row["kind"] == "total")
        return RealtimeHttpResponse(
            minutes=minutes, window_start=start.isoformat(), observed_at=observed.isoformat(),
            queried_at=datetime.now(UTC).isoformat(),
            snapshot_id=str(snapshot["snapshot_id"]) if snapshot else None,
            snapshot_current_at=_stamp(snapshot["made_current_at"]) if snapshot else None,
            latest_ingest_at=_stamp(total["latest_ingest_at"]),
            latest_event_at=_stamp(total["latest_event_at"]),
            quality=RealtimeHttpQuality(**total), totals=RealtimeHttpTotals(**total),
            series=[RealtimeHttpMinute(**{**row, "minute": _stamp(row["minute"])})
                    for row in rows if row["kind"] == "minute"],
            routes=[RealtimeHttpRoute(**row) for row in rows if row["kind"] == "route"],
        )
    except Exception as error:
        logging.getLogger(__name__).warning("Landing HTTP observation unavailable", exc_info=True)
        raise AppError(INTERNAL_ERROR, "Chưa đọc được HTTP realtime từ Landing. Vui lòng thử lại.",
                       status_code=503) from error
