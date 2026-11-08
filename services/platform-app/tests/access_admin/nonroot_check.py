"""Exercise authenticated reads/writes as the deployed unprivileged container UID."""

import os
from pathlib import Path

from fastapi.testclient import TestClient
from platform_app.access_admin.app import app
from platform_app.api.core.config import get_settings


def main():
    assert os.geteuid() == 1000
    settings = get_settings()
    with TestClient(app) as session:
        response = session.post(
            "/api/v1/access/login",
            json={
                "email": settings.bootstrap_admin_email,
                "password": settings.bootstrap_admin_password,
            },
        )
        assert response.status_code == 200, response.text
        headers = {"X-CSRF-Token": session.cookies.get(settings.csrf_cookie_name)}
        for provider, kind, subject, target in (
            ("minio", "policy", os.environ["MINIO_TRINO_ACCESS_KEY"], "lakehouse-trino"),
            ("polaris", "principal_role", "trino_reader", "spark_writer_role"),
        ):
            for granted in (True, False):
                state = session.get("/api/v1/access/" + provider)
                assert state.status_code == 200, state.text
                result = session.post(
                    "/api/v1/access/" + provider + "/bindings",
                    headers=headers,
                    json={
                        "kind": kind,
                        "subject": subject,
                        "target": target,
                        "granted": granted,
                        "revision": state.json()["revision"],
                    },
                )
                assert result.status_code == 200 and result.json()["status"] == "verified", result.text
        path = Path(os.environ["ACCESS_ADMIN_AUDIT_PATH"])
        assert path.stat().st_uid == 1000 and path.stat().st_mode & 0o777 == 0o600
        assert len(session.get("/api/v1/access/audit").json()) == 8
        print("ACCESS_ADMIN_NONROOT_OK uid=1000 verified_changes=4")


if __name__ == "__main__":
    main()
