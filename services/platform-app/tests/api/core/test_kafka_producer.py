"""Delivery acknowledgement, local fallback and producer lifecycle without Kafka."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from platform_app.api.core import kafka_producer as kafka


class KafkaProducerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.dlq = Path(self.temporary.name) / "events.jsonl"
        for name, value in {
            "_KAFKA_ENABLED": True,
            "_producer": None,
            "_pending": set(),
            "_reconnect_task": None,
            "_dropped_events_count": 0,
            "_DELIVERY_TIMEOUT_SECONDS": 0.05,
            "_RECONNECT_SECONDS": 0.001,
        }.items():
            binding = patch.object(kafka, name, value)
            binding.start()
            self.addCleanup(binding.stop)
        binding = patch.dict("os.environ", KAFKA_DLQ_FILE_PATH=str(self.dlq))
        binding.start()
        self.addCleanup(binding.stop)
        self.addAsyncCleanup(kafka.stop_producer)

    def rows(self):
        return [json.loads(line) for line in self.dlq.read_text().splitlines()]

    async def test_acknowledged_send_is_drained_before_shutdown(self):
        producer = AsyncMock()
        kafka._producer = producer
        kafka.send_access_event({"request": {"id": "one"}})
        await kafka.stop_producer()
        producer.send_and_wait.assert_awaited_once_with(kafka._ACCESS_LOG_TOPIC, {"request": {"id": "one"}})
        producer.stop.assert_awaited_once()
        self.assertFalse(self.dlq.exists())
        self.assertFalse(kafka._pending)

    async def test_delivery_error_after_enqueue_is_retained(self):
        producer = AsyncMock()
        producer.send_and_wait.side_effect = RuntimeError("broker acknowledgement failed")
        kafka._producer = producer
        kafka.send_access_event({"request": {"id": "uncertain"}})
        await kafka.stop_producer()
        self.assertEqual(self.rows()[0]["request"]["id"], "uncertain")
        self.assertIn("send_failed", self.rows()[0]["_dlq_reason"])
        self.assertEqual(kafka.get_dropped_events_count(), 1)

    async def test_delivery_timeout_is_bounded_and_retained(self):
        producer = AsyncMock()

        async def blocked(*args):
            await asyncio.Event().wait()

        producer.send_and_wait.side_effect = blocked
        kafka._producer = producer
        kafka.send_access_event({"request": {"id": "timeout"}})
        await asyncio.wait_for(kafka.stop_producer(), timeout=1)
        self.assertEqual(len(self.rows()), 1)

    async def test_offline_and_pending_limit_keep_event_identity(self):
        kafka.send_access_event({"request": {"id": "offline"}})
        kafka._producer = AsyncMock()
        with patch.object(kafka, "_MAX_PENDING", 0):
            kafka.send_access_event({"request": {"id": "overload"}})
        self.assertEqual([row["_dlq_reason"] for row in self.rows()], ["producer_offline", "pending_limit"])

    async def test_startup_failure_cleans_candidate_then_recovers(self):
        failed, recovered = AsyncMock(), AsyncMock()
        failed.start.side_effect = RuntimeError("offline")
        with patch("aiokafka.AIOKafkaProducer", side_effect=[failed, recovered]):
            await kafka.start_producer()
            failed.stop.assert_awaited_once()
            kafka.send_access_event({"request": {"id": "before-recovery"}})
            await asyncio.wait_for(kafka._reconnect_task, timeout=1)
            self.assertIs(kafka._producer, recovered)
            kafka.send_access_event({"request": {"id": "after-recovery"}})
            await kafka.stop_producer()
        self.assertEqual(len(self.rows()), 1)
        recovered.send_and_wait.assert_awaited_once()

    async def test_shutdown_cancels_connection_retry_and_closes_candidate(self):
        failed, waiting = AsyncMock(), AsyncMock()
        failed.start.side_effect = RuntimeError("offline")
        entered = asyncio.Event()

        async def connecting():
            entered.set()
            await asyncio.Event().wait()

        waiting.start.side_effect = connecting
        with patch("aiokafka.AIOKafkaProducer", side_effect=[failed, waiting]):
            await kafka.start_producer()
            await asyncio.wait_for(entered.wait(), timeout=1)
            await asyncio.wait_for(kafka.stop_producer(), timeout=1)
        waiting.stop.assert_awaited_once()
        self.assertIsNone(kafka._reconnect_task)
        self.assertIsNone(kafka._producer)

    async def test_disabled_producer_does_not_connect_or_write(self):
        with patch.object(kafka, "_KAFKA_ENABLED", False), patch("aiokafka.AIOKafkaProducer") as factory:
            await kafka.start_producer()
            kafka.send_access_event({"request": {"id": "disabled"}})
            factory.assert_not_called()
        self.assertFalse(self.dlq.exists())
