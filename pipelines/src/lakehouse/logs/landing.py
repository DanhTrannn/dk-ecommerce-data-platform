"""Keep every decoded Kafka payload, even when its event contract is invalid."""

from datetime import datetime, timezone

from lakehouse.logs.parser import InvalidAccessEvent, parse_access_event

LANDING_FIELDS = (
    ("event_id", "STRING"),
    ("event_ts", "STRING"),
    ("ingest_ts", "STRING"),
    ("service_name", "STRING"),
    ("http_method", "STRING"),
    ("http_route", "STRING"),
    ("http_status_code", "INT"),
    ("duration_ns", "LONG"),
    ("actor_type", "STRING"),
    ("ecommerce_action", "STRING"),
    ("raw_payload", "STRING"),
)


def landing_envelope(raw):
    """Projected columns support observation; raw_payload remains unchanged."""
    try:
        event = parse_access_event(raw)
        return (
            event.event_id,
            event.event_ts,
            event.ingest_ts,
            event.service_name,
            event.http_method,
            event.http_route,
            event.http_status,
            event.duration_ns,
            event.actor_type,
            event.action,
            raw,
        )
    except InvalidAccessEvent:
        return (
            None,
            None,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            raw,
        )
