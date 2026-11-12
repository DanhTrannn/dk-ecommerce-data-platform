"""Issue actual API requests and transport-fault probes in the private test network."""

import json
import sys
import asyncio
import os
import httpx
from sqlalchemy import text
from platform_app.api.db.session import engine


def traffic():
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
            client.get("/api/v1/products/native-no-such-product"),
        ]
    return dict(
        ids=[response.headers["x-request-id"] for response in responses],
        statuses=[response.status_code for response in responses],
        product_id=product["product_id"],
    )


async def inject():
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    topic = os.environ["KAFKA_ACCESS_LOG_TOPIC"]
    consumer = AIOKafkaConsumer(topic, bootstrap_servers="kafka:9092", auto_offset_reset="earliest", group_id=None)
    await consumer.start()
    try:
        async with asyncio.timeout(20):
            async for message in consumer:
                raw = message.value
                break
    finally:
        await consumer.stop()
    producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
    await producer.start()
    try:
        await producer.send_and_wait(topic, raw)
        await producer.send_and_wait(topic, b"invalid-native-log-event")
    finally:
        await producer.stop()
    return {"raw_replay_sent": True, "invalid_sent": True}


mode = sys.argv[1]
result = traffic() if mode == "traffic" else asyncio.run(inject())
print("STACK_RESULT " + json.dumps(result), flush=True)
