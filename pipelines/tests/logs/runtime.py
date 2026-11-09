"""Network calls from the isolated platform container; only IDs/counts leave it."""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx


def traffic():
    from sqlalchemy import text
    from platform_app.api.db.session import engine

    with engine.connect() as db:
        product = (
            db.execute(text("SELECT product_id,slug FROM products WHERE is_active=true ORDER BY product_id LIMIT 1"))
            .mappings()
            .one()
        )
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=30, trust_env=False) as client:
        responses = [
            client.get("/api/v1/products"),
            client.get("/api/v1/products/" + product["slug"]),
            client.get("/api/v1/products/no-such-product-acceptance"),
        ]
    assert [r.status_code for r in responses] == [200, 200, 404], [r.status_code for r in responses]
    return dict(
        ids=[r.headers["x-request-id"] for r in responses],
        statuses=[r.status_code for r in responses],
        product_id=product["product_id"],
    )


async def inject(ids):
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    topic = os.environ["KAFKA_ACCESS_LOG_TOPIC"]
    consumer = AIOKafkaConsumer(topic, bootstrap_servers="kafka:9092", auto_offset_reset="earliest", group_id=None)
    await consumer.start()
    raw = None
    try:
        async with asyncio.timeout(20):
            async for message in consumer:
                event = json.loads(message.value)
                if event["request"]["id"] == ids[0]:
                    raw = message.value
                    break
    finally:
        await consumer.stop()
    producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
    await producer.start()
    try:
        await producer.send_and_wait(topic, raw)
        await producer.send_and_wait(topic, b"not-json-acceptance")
    finally:
        await producer.stop()
    return dict(identical_replay_sent=True, invalid_payload_sent=True)


def reader():
    from platform_app.api.modules.analytics.reader import PublishedReader
    from platform_app.api.modules.analytics.trino_client import TrinoClient

    r = PublishedReader(TrinoClient(base_url="http://trino:8080", user="logs_acceptance"), source="logs")
    return dict(
        run_id=r.publication["run_id"],
        snapshots=r.snapshots,
        routes=r.query("SELECT * FROM lakehouse.gold.mart_hourly_route_metrics"),
        demand=r.query("SELECT * FROM lakehouse.gold.mart_daily_product_demand"),
        funnel=r.query("SELECT * FROM lakehouse.gold.mart_marketing_funnel_daily"),
    )


def http_report():
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=60, trust_env=False) as client:
        denied = client.get("/api/v1/admin/analytics/http-metrics")
        assert denied.status_code == 401
        login = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@example.com", "password": os.environ["API_BOOTSTRAP_ADMIN_PASSWORD"]},
        )
        assert login.status_code == 200, login.status_code
        report = client.get("/api/v1/admin/analytics/http-metrics")
        assert report.status_code == 200, report.status_code
        body = report.json()
        return dict(
            status=200,
            anonymous_status=401,
            data_source=body["data_source"],
            source_run_id=body["source_run_id"],
            totals=body["totals"],
            served_from_cache=body["served_from_cache"],
        )


mode = sys.argv[1]
if mode == "traffic":
    result = traffic()
elif mode == "inject":
    result = asyncio.run(inject(json.loads(sys.argv[2])))
elif mode == "reader":
    result = reader()
elif mode == "http-report":
    result = http_report()
elif mode == "dlq":
    path = Path(os.environ["KAFKA_DLQ_FILE_PATH"])
    rows = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    result = dict(ids=[row["request"]["id"] for row in rows], count=len(rows))
else:
    with httpx.Client(base_url="http://flink-jobmanager:8081", timeout=30, trust_env=False) as client:
        response = client.get(sys.argv[2])
        response.raise_for_status()
        result = response.json()
print("STACK_RESULT " + json.dumps(result, default=str), flush=True)
