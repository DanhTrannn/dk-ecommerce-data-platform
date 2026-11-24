import unittest
from unittest.mock import patch

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from platform_app.api.core.config import Settings
from platform_app.api.core.middleware import RequestContextMiddleware


class RequestContextMiddlewareTests(unittest.IsolatedAsyncioTestCase):
    async def test_logs_request_and_matches_response_request_id(self):
        app = FastAPI()
        app.add_middleware(RequestContextMiddleware)

        @app.get("/api/v1/products/{slug}")
        async def product(slug: str):
            return {"slug": slug}

        settings = Settings(_env_file=None, database_url="sqlite://", secret_key="x" * 32)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            with (
                patch("platform_app.api.core.access_logging.get_settings", return_value=settings),
                self.assertLogs("ecommerce_api.access", level="INFO") as logs,
            ):
                response = await client.get("/api/v1/products/test-shirt")

        self.assertEqual(response.status_code, 200)
        event = logs.records[0].access_event
        self.assertEqual(event["request"]["id"], response.headers["X-Request-ID"])
        self.assertEqual(event["http"]["route"], "/api/v1/products/{slug}")
        self.assertEqual(event["ecommerce"]["action"], "product_detail")
