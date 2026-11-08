"""Inside-platform helpers for the real private deployment acceptance run."""

import argparse
import json
import os
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx


def source():
    import yaml
    from sqlalchemy import text
    from database.seeds.seed_catalog import seed
    from generator.config import config_from_dict
    from generator.sql_export import export_sql
    from platform_app.api.core.config import get_settings
    from platform_app.api.db.session import SessionLocal, engine
    from platform_app.api.db.import_dataset import import_dataset

    seed(SessionLocal, get_settings())
    raw = yaml.safe_load(Path("/app/generator/configs/small.yml").read_text())
    raw["scale"] = dict(customers=10, products=2, variants=8, orders=30)
    path = Path("/data/stack-fixture.sql")
    export_sql(config_from_dict(raw), path)
    receipt = import_dataset(engine, path)
    with engine.connect() as connection:
        orders = connection.scalar(text("SELECT count(*) FROM orders"))
        total = connection.scalar(text("SELECT coalesce(sum(total_vnd),0) FROM orders"))
    return dict(orders=int(orders), order_total_vnd=int(total), receipt=receipt)


def purchases():
    from platform_app.ui.scenarios.config import PurchaseConfig, ScenarioSettings
    from platform_app.ui.scenarios.store import ScenarioStore, ACTIVE

    before = datetime.now(UTC) - timedelta(seconds=1)
    with tempfile.TemporaryDirectory(prefix="stack-scenarios-") as temporary:
        store = ScenarioStore(Path(temporary))
        run = store.start(
            PurchaseConfig(purchases=2, interval_seconds=0, payment_method="cod"),
            ScenarioSettings(api_base_url="http://127.0.0.1:8000", _env_file=None),
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            current = store.read(run["run_id"])
            if current["status"] not in ACTIVE:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("Runner did not finish")
        assert current["status"] == "succeeded", current["status"]
        return dict(
            run_id=run["run_id"],
            interval_start=before.isoformat(),
            interval_end=(datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
            orders=[
                {"order_number": item["order_number"], "total_vnd": item["total_vnd"]} for item in current["attempts"]
            ],
        )


def trino():
    from platform_app.api.modules.analytics.reader import PublishedReader
    from platform_app.api.modules.analytics.trino_client import TrinoClient

    client = TrinoClient(base_url="http://trino:8080", user="stack_acceptance")
    deadline = time.monotonic() + 90
    while not client.is_healthy():
        if time.monotonic() > deadline:
            raise RuntimeError("Trino is not ready")
        time.sleep(0.5)
    reader = PublishedReader(client)
    orders = reader.one(
        "SELECT count(*) AS orders, coalesce(sum(order_total_vnd),0) AS order_total_vnd FROM lakehouse.gold.fact_order"
    )
    mart = reader.one(
        "SELECT coalesce(sum(total_orders),0) AS orders, coalesce(sum(order_total_vnd),0) AS order_total_vnd, coalesce(sum(net_revenue_vnd),0) AS net_revenue_vnd FROM lakehouse.gold.mart_order_daily"
    )
    return dict(run_id=reader.publication["run_id"], snapshots=reader.snapshots, fact=orders, mart=mart)


def query(sql):
    from platform_app.api.modules.analytics.trino_client import TrinoClient

    return TrinoClient(base_url="http://trino:8080", user="stack_acceptance").execute_query(sql)


def query_denied(sql):
    from platform_app.api.core.errors import AppError

    try:
        query(sql)
    except AppError as error:
        message = str(error).lower()
        assert any(part in message for part in ("denied", "forbidden", "403", "not authorized")), message
        return {"denied": True, "statement": sql.split()[0]}
    raise AssertionError("Trino reader unexpectedly accepted a mutation")


def airflow(action, dag=None, run_id=None, lower=None, upper=None):
    origin = "http://airflow-api-server:8080"
    with httpx.Client(base_url=origin, timeout=30, trust_env=False) as client:
        token = client.post(
            "/auth/token",
            json={
                "username": os.environ["STACK_AIRFLOW_USER"],
                "password": os.environ["STACK_AIRFLOW_PASSWORD"],
            },
        )
        assert token.status_code == 201 or token.status_code == 200, f"Airflow login HTTP {token.status_code}"
        client.headers["Authorization"] = "Bearer " + token.json()["access_token"]
        if action == "wait":
            deadline = time.monotonic() + 150
            while time.monotonic() < deadline:
                response = client.get("/api/v2/dags/" + dag)
                if response.status_code == 200:
                    return response.json()
                time.sleep(1)
            raise RuntimeError("DAG was not imported: " + dag)
        if action == "unpause":
            response = client.patch("/api/v2/dags/" + dag, json={"is_paused": False})
        elif action == "pause":
            response = client.patch("/api/v2/dags/" + dag, json={"is_paused": True})
        else:
            body = {"dag_run_id": run_id, "logical_date": None}
            if lower:
                body.update(logical_date=lower, data_interval_start=lower, data_interval_end=upper)
            response = client.post("/api/v2/dags/" + dag + "/dagRuns", json=body)
        assert response.status_code in (200, 201), (
            f"Airflow {action} HTTP {response.status_code}: {response.text[:500]}"
        )
        return response.json()


def permission(action):
    from platform_app.provisioning.client import PolarisClient

    client = PolarisClient("http://polaris:8181", "POLARIS")
    try:
        client.token = client.credentials_token(
            os.environ["POLARIS_ROOT_CLIENT_ID"], os.environ["POLARIS_ROOT_CLIENT_SECRET"]
        )
        path = "/api/management/v1/principals/spark_writer/principal-roles"
        if action == "revoke":
            client.call("DELETE", path + "/spark_writer_role")
        else:
            client.call("PUT", path, {"principalRole": {"name": "spark_writer_role"}})
        return {"spark_role": action}
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode", choices=["source", "purchases", "trino", "query", "query-denied", "airflow", "permission"]
    )
    parser.add_argument("--sql")
    parser.add_argument("--action")
    parser.add_argument("--dag")
    parser.add_argument("--run-id")
    parser.add_argument("--lower")
    parser.add_argument("--upper")
    args = parser.parse_args()
    if args.mode == "airflow":
        result = airflow(args.action, args.dag, args.run_id, args.lower, args.upper)
    elif args.mode == "query-denied":
        result = query_denied(args.sql)
    elif args.mode == "permission":
        result = permission(args.action)
    elif args.mode == "query":
        result = query(args.sql)
    else:
        result = globals()[args.mode]()
    print("STACK_RESULT " + json.dumps(result, default=str), flush=True)


if __name__ == "__main__":
    # Script lives on a test mount; include the image's documented database package.
    import sys

    sys.path.insert(0, "/app")
    main()
