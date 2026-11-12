"""Private source/catalog/Trino/Airflow fixture with no Spark service or runtime."""

import json
import os
import time

import yaml

from stack.fixture import ROOT, StackFixture


class NativeStack(StackFixture):
    def __init__(self):
        super().__init__()
        self.environment["KAFKA_ENABLED"] = "true"
        self.environment["KAFKA_ACCESS_LOG_TOPIC"] = "acceptance.access_logs"
        extra = self.work / "native.yml"
        services = {
            "batch-client": {
                "volumes": [
                    f"{ROOT}/pipelines/tests/native:/native-checks:ro",
                    f"{ROOT}/pipelines/tests:/pipeline-tests:ro",
                ],
                "environment": {
                    "PYTHONPATH": "/opt/project/pipelines/src:/pipeline-tests",
                    "DBT_PROJECT_DIR": "/opt/project/pipelines/dbt",
                },
            },
            "platform-app": {"volumes": [f"{ROOT}/pipelines/tests/native:/native-checks:ro"]},
            "airflow-scheduler": {"volumes": [f"{ROOT}/pipelines/tests/native:/native-checks:ro"]},
        }
        for name in ("flink-jobmanager", "flink-taskmanager", "flink-submit", "flink-cdc-submit"):
            services[name] = {"image": os.environ.get("LOGS_FLINK_IMAGE", "lakehouse-flink:1.19-iceberg-1.10.1")}
        extra.write_text(yaml.safe_dump({"services": services}))
        self.base.extend(["--profile", "streaming", "-f", str(extra)])
        config = json.loads(self.command(["config", "--format", "json"], private=True).stdout)
        assert "spark-client" not in config["services"]
        assert all(not service.get("ports") for service in config["services"].values())

    def up(self, *services):
        self.command(
            ["up", "-d", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "240", *services], timeout=360
        )

    def http(self, origin, method, path):
        return self.result(self.command([
            "exec", "-T", "platform-app", "python", "/native-checks/service_http.py", origin, method, path
        ]).stdout)

    def traffic(self, mode):
        return self.result(self.command([
            "exec", "-T", "platform-app", "python", "/native-checks/traffic.py", mode
        ]).stdout)

    def start_streaming(self):
        self.command(["exec", "-T", "postgres", "sh", "/opt/project/configure-source-cdc.sh"])
        self.up("kafka", "flink-jobmanager", "flink-taskmanager", "debezium-connect")
        for service in ("kafka-init", "debezium-init", "flink-submit", "flink-cdc-submit"):
            self.command(["run", "--rm", "--no-deps", "-T", service], timeout=360)
        self.wait("both Flink jobs running", lambda: len([
            job for job in self.http("flink", "GET", "/jobs/overview")["jobs"] if job["state"] == "RUNNING"
        ]) == 2)

    def native(self, *args, check=True, timeout=1800):
        response = self.command(["run", "--rm", "--no-deps", "-T", "batch-client", *args], check=check, timeout=timeout)
        return self.result(response.stdout) if response.returncode == 0 else response

    def query(self, sql):
        return self.runtime("query", "--sql", sql)

    def wait(self, label, check, timeout=300):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = check()
            if result:
                print("NATIVE_PROGRESS " + label, flush=True)
                return result
            time.sleep(3)
        raise AssertionError("Native stack timeout: " + label)
