"""Async Kafka producer singleton for access log streaming.

Lifecycle is managed via FastAPI lifespan. The producer is started on app
startup and gracefully stopped on shutdown. Delivery runs in tracked background
tasks; unconfirmed events are retained in a local DLQ for explicit recovery.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("ecommerce_api.kafka")

_KAFKA_ENABLED: bool = os.getenv("KAFKA_ENABLED", "true").lower() == "true"
_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
_ACCESS_LOG_TOPIC: str = os.getenv("KAFKA_ACCESS_LOG_TOPIC", "ecommerce.access_logs")
_DLQ_FILE_PATH: str = os.getenv("KAFKA_DLQ_FILE_PATH", "data/events/dlq_access_logs.jsonl")

_producer = None  # AIOKafkaProducer instance, lazily created
_dropped_events_count: int = 0
_pending: set[asyncio.Task] = set()
_reconnect_task: asyncio.Task | None = None
_RECONNECT_SECONDS = 5
_DELIVERY_TIMEOUT_SECONDS = 10
_MAX_PENDING = 256


def get_dropped_events_count() -> int:
    """Return the total number of events dropped or routed to DLQ."""
    return _dropped_events_count


def reset_dropped_events_count() -> None:
    """Reset the dropped events counter (for testing/monitoring)."""
    global _dropped_events_count
    _dropped_events_count = 0


def record_dropped_event(event: dict[str, Any], reason: str) -> None:
    """Increment drop counter and append event to local Dead-Letter Queue file."""
    global _dropped_events_count
    _dropped_events_count += 1

    dlq_path = os.getenv("KAFKA_DLQ_FILE_PATH", _DLQ_FILE_PATH)
    try:
        os.makedirs(os.path.dirname(os.path.abspath(dlq_path)), exist_ok=True)
        payload = {
            **event,
            "_dlq_reason": reason,
            "_dlq_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with open(dlq_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as dlq_err:
        logger.error("kafka_producer.dlq_write_failed: %s", dlq_err)


async def _connect() -> bool:
    global _producer
    from aiokafka import AIOKafkaProducer

    candidate = AIOKafkaProducer(
        bootstrap_servers=_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        linger_ms=5,
        acks="all",
        request_timeout_ms=5_000,
        retry_backoff_ms=100,
    )
    try:
        await asyncio.wait_for(candidate.start(), timeout=_DELIVERY_TIMEOUT_SECONDS)
    except (Exception, asyncio.CancelledError) as exc:
        try:
            await asyncio.wait_for(candidate.stop(), timeout=_DELIVERY_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("kafka_producer.cleanup_failed")
        if isinstance(exc, asyncio.CancelledError):
            raise
        logger.warning("kafka_producer.start_failed; retaining events in local DLQ")
        return False
    _producer = candidate
    logger.info("kafka_producer.started", extra={"topic": _ACCESS_LOG_TOPIC})
    return True


async def _reconnect() -> None:
    while _producer is None:
        await asyncio.sleep(_RECONNECT_SECONDS)
        if await _connect():
            return


async def start_producer() -> None:
    """Start delivery, retrying in the background when Kafka starts later."""
    global _reconnect_task
    if not _KAFKA_ENABLED or _producer is not None or _reconnect_task is not None:
        return
    if not await _connect():
        _reconnect_task = asyncio.create_task(_reconnect())


async def stop_producer() -> None:
    """Drain tracked deliveries before closing Kafka; retain uncertain sends in DLQ."""
    global _producer, _reconnect_task
    if _reconnect_task is not None:
        _reconnect_task.cancel()
        await asyncio.gather(_reconnect_task, return_exceptions=True)
        _reconnect_task = None
    if _pending:
        await asyncio.gather(*list(_pending), return_exceptions=True)
    if _producer is not None:
        try:
            await asyncio.wait_for(_producer.stop(), timeout=_DELIVERY_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("kafka_producer.stop_error")
        finally:
            _producer = None


def send_access_event(event: dict[str, Any]) -> None:
    """Fire-and-forget: enqueue event to Kafka or route to Dead-Letter Queue.

    This is intentionally *not* an async function so it can be called from
    synchronous or async middleware code alike. If the Kafka producer is
    offline or the event loop is unavailable, the event is saved to DLQ.
    """
    if not _KAFKA_ENABLED:
        return
    if _producer is None:
        record_dropped_event(event, reason="producer_offline")
        return
    if len(_pending) >= _MAX_PENDING:
        record_dropped_event(event, reason="pending_limit")
        return
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            task = loop.create_task(_safe_send(event))
            _pending.add(task)
            task.add_done_callback(_pending.discard)
        else:
            record_dropped_event(event, reason="event_loop_not_running")
    except Exception as exc:
        record_dropped_event(event, reason=f"loop_error: {exc}")
        logger.debug("kafka_producer.send_skipped – event routed to DLQ")


async def _safe_send(event: dict[str, Any]) -> None:
    """Internal coroutine that performs the actual produce call with DLQ fallback."""
    if _producer is None:
        record_dropped_event(event, reason="producer_offline")
        return
    try:
        # send() alone only enqueues: delivery failures arrive on its returned future.
        await asyncio.wait_for(_producer.send_and_wait(_ACCESS_LOG_TOPIC, event), timeout=_DELIVERY_TIMEOUT_SECONDS)
    except Exception as exc:
        record_dropped_event(event, reason=f"send_failed: {exc}")
        logger.warning("kafka_producer.send_failed – routed to DLQ", exc_info=True)
