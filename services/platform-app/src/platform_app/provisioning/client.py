"""Bounded Polaris requests with generic failures and no write retries."""

import httpx


class ProvisionError(RuntimeError):
    pass


class PolarisClient:
    def __init__(self, origin, realm, *, transport=None):
        self.http = httpx.Client(
            base_url=origin,
            headers={"Polaris-Realm": realm},
            timeout=30,
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        )
        self.token = None

    def close(self):
        self.http.close()

    def credentials_token(self, client_id, secret):
        try:
            response = self.http.post(
                "/api/catalog/v1/oauth/tokens",
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": secret,
                    "scope": "PRINCIPAL_ROLE:ALL",
                },
            )
            if response.status_code != 200:
                raise ProvisionError("Polaris authentication failed; credentials were not reset")
            return response.json()["access_token"]
        except (httpx.HTTPError, ValueError, KeyError):
            raise ProvisionError("Polaris authentication unavailable or response invalid") from None

    def call(self, method, path, payload=None, *, missing=False):
        try:
            response = self.http.request(method, path, json=payload, headers={"Authorization": f"Bearer {self.token}"})
        except httpx.HTTPError:
            raise ProvisionError(
                "Polaris request failed; no automatic write retry. Rerun to inspect persisted state."
            ) from None
        if missing and response.status_code == 404:
            return None
        if not 200 <= response.status_code < 300:
            raise ProvisionError(f"Polaris {method} failed (HTTP {response.status_code}); response body omitted")
        try:
            return response.json() if response.content else {}
        except ValueError:
            raise ProvisionError("Polaris response is not valid JSON") from None
