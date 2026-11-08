"""Real auth routes and real MinIO/Polaris access mutations in a private stack."""

import json
import os
import sys
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, "/app")

from database.seeds.seed_catalog import seed
from platform_app.api.core.config import get_settings
from platform_app.access_admin.app import app
from platform_app.access_admin.settings import AccessSettings
from platform_app.provisioning.client import PolarisClient
from platform_app.provisioning.polaris import load_spec, provision


def main():
    settings = get_settings()
    engine = create_engine(settings.database_url)
    try:
        seed(sessionmaker(engine), settings)
        session = TestClient(app)
        with session:
            assert session.get("/api/v1/access/minio").status_code == 401
            response = session.post(
                "/api/v1/access/login",
                json={
                    "email": settings.bootstrap_admin_email,
                    "password": settings.bootstrap_admin_password,
                },
            )
            assert response.status_code == 200, response.text
            headers = {"X-CSRF-Token": session.cookies.get(settings.csrf_cookie_name)}

            def state(provider):
                response = session.get("/api/v1/access/" + provider)
                assert response.status_code == 200, response.text
                data = response.json()
                for name in ("MINIO_ROOT_PASSWORD", "POLARIS_ROOT_CLIENT_SECRET", "API_SECRET_KEY"):
                    assert os.environ[name] not in response.text
                return data

            def mutate(provider, kind, subject, target, grant):
                before = state(provider)
                payload = dict(kind=kind, subject=subject, target=target, granted=grant, revision=before["revision"])
                assert session.post("/api/v1/access/" + provider + "/bindings", json=payload).status_code == 403
                response = session.post("/api/v1/access/" + provider + "/bindings", json=payload, headers=headers)
                assert response.status_code == 200, response.text
                assert response.json()["status"] == "verified", response.text
                assert (
                    session.post("/api/v1/access/" + provider + "/bindings", json=payload, headers=headers).status_code
                    == 409
                )

            # Direct policy attachment changes actual MinIO user assignments.
            mutate("minio", "policy", os.environ["MINIO_TRINO_ACCESS_KEY"], "lakehouse-spark", True)
            assert (
                "lakehouse-spark"
                in next(row for row in state("minio")["users"] if row["name"] == os.environ["MINIO_TRINO_ACCESS_KEY"])[
                    "policies"
                ]
            )
            mutate("minio", "policy", os.environ["MINIO_TRINO_ACCESS_KEY"], "lakehouse-spark", False)
            # Three Polaris association types are reversible without creating identities/roles.
            mutate("polaris", "principal_role", "trino_reader", "spark_writer_role", True)
            assert (
                "spark_writer_role"
                in next(row for row in state("polaris")["principals"] if row["name"] == "trino_reader")["roles"]
            )
            mutate("polaris", "principal_role", "trino_reader", "spark_writer_role", False)
            mutate("polaris", "catalog_role", "trino_reader_role", "spark_writer_catalog_role", True)
            mutate("polaris", "catalog_role", "trino_reader_role", "spark_writer_catalog_role", False)
            mutate("polaris", "catalog_privilege", "trino_reader_catalog_role", "TABLE_READ_DATA", True)
            mutate("polaris", "catalog_privilege", "trino_reader_catalog_role", "TABLE_READ_DATA", False)
            # Existing bootstrap must retain this direct policy removal and grant revocation.
            mutate("minio", "policy", os.environ["MINIO_TRINO_ACCESS_KEY"], "lakehouse-trino", False)
            policy_state = state("minio")
            assert not next(
                row for row in policy_state["users"] if row["name"] == os.environ["MINIO_TRINO_ACCESS_KEY"]
            )["policies"]
            access_settings = AccessSettings()
            root = PolarisClient(access_settings.polaris_url, access_settings.polaris_realm)
            try:
                root.token = root.credentials_token(
                    access_settings.polaris_client_id, access_settings.polaris_secret.get_secret_value()
                )
                spec = load_spec("/app/infrastructure/polaris/rbac.yml")
                provision(
                    root,
                    spec,
                    Path("/run/polaris-state"),
                    dict(origin="http://polaris:8181", realm="POLARIS", catalog="lakehouse"),
                )
            finally:
                root.close()
            assert not any(
                grant.get("privilege") == "TABLE_READ_DATA"
                for role in state("polaris")["catalog_roles"]
                if role["name"] == "trino_reader_catalog_role"
                for grant in role["grants"]
            )
            rows = session.get("/api/v1/access/audit").json()
            assert len([row for row in rows if row["status"] == "verified"]) == 9
            print(
                "ACCESS_ADMIN_LIVE_OK "
                + json.dumps({"verified_changes": 9, "audit_records": len(rows), "providers": ["minio", "polaris"]})
            )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
