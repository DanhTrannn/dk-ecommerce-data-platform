"""Raw retention and identity contracts, independent of Spark/Flink/Kafka."""

import json
import unittest

from lakehouse.logs.landing import LANDING_FIELDS, landing_envelope
from lakehouse.logs.parser import InvalidAccessEvent, parse_access_event


class AccessLogContractTests(unittest.TestCase):
    def event(self):
        return {
            "request": {"id": "0123456789abcdef0123456789abcdef"},
            "timestamp": "2026-11-09T07:00:00+07:00",
            "http": {"request_method": "GET", "route": "/api/v1/products/{slug}", "status_code": 404},
            "event": {"duration_ns": 2500000},
            "data_origin": "observed",
        }

    def test_real_envelope_keeps_identity_duration_status_and_utc(self):
        raw = json.dumps(self.event())
        event = parse_access_event(raw)
        self.assertEqual(event.event_ts, "2026-11-09 00:00:00.000000")
        self.assertEqual(event.event_id, self.event()["request"]["id"])
        self.assertTrue(event.is_client_error)
        self.assertFalse(event.is_success)
        self.assertEqual(event.duration_ms, 2.5)
        projected = dict(zip((name for name, _ in LANDING_FIELDS), landing_envelope(raw)))
        self.assertEqual(projected["raw_payload"], raw)
        self.assertEqual(projected["http_status_code"], 404)

    def test_malformed_and_invalid_contracts_remain_in_landing(self):
        event = self.event()
        event["request"]["id"] = "caller-controlled-id"
        for raw in ("invalid-json", "[]", json.dumps(event)):
            with self.subTest(raw=raw):
                with self.assertRaises(InvalidAccessEvent):
                    parse_access_event(raw)
                row = landing_envelope(raw)
                self.assertIsNone(row[0])
                self.assertIsNotNone(row[2])
                self.assertEqual(row[-1], raw)
