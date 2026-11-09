"""Private deployment for real API access-log acceptance; no host ports or shared volumes."""

import json
import os
import time

import yaml

from stack.fixture import ROOT, StackFixture


class LogsStack(StackFixture):
    def __init__(self):
        super().__init__()
        self.environment["KAFKA_ENABLED"] = "true"
        self.environment["KAFKA_ACCESS_LOG_TOPIC"] = "acceptance.access_logs"
        image = os.environ.get("LOGS_FLINK_IMAGE", "lakehouse-flink:1.19-iceberg-1.10.1")
        services = {
            "platform-app": {"volumes": [f"{ROOT}/pipelines/tests/logs:/logs-checks:ro"]},
            "spark-client": {"volumes": [f"{ROOT}/pipelines/tests/logs:/logs-checks:ro"]},
            "airflow-scheduler": {"volumes": [f"{ROOT}/pipelines/tests/logs:/logs-checks:ro"]},
            "kafka": {"environment": {"KAFKA_HEAP_OPTS": "-Xms128m -Xmx512m"}, "mem_limit": "1500m"},
        }
        for name in ("flink-jobmanager", "flink-taskmanager", "flink-submit"):
            services[name] = {"image": image, "mem_limit": "3g" if name == "flink-taskmanager" else "2g"}
        extra = self.work / "logs.yml"
        extra.write_text(yaml.safe_dump({"services": services}))
        self.base.extend(["--profile", "streaming", "-f", str(extra)])
        config = json.loads(self.command(["config", "--format", "json"], private=True).stdout)
        assert all(not service.get("ports") for service in config["services"].values())
        assert all(v["name"].startswith(self.project + "_") for v in config["volumes"].values())

    def up(self, *services):
        self.command(
            ["up", "-d", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "240", *services], timeout=360
        )

    def helper(self, mode, *args):
        return self.result(
            self.command(["exec", "-T", "platform-app", "python", "/logs-checks/runtime.py", mode, *args]).stdout
        )

    def query(self, sql):
        return self.runtime("query", "--sql", sql)

    def wait(self, label, check, timeout=300):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = check()
            if value:
                print("LOGS_PROGRESS " + label, flush=True)
                return value
            time.sleep(3)
        raise AssertionError("Access-log timeout: " + label)

    def spark_logs(self, mode, run_id="logs-replay"):
        result = self.command(
            ["exec", "-T", "airflow-scheduler", "spark-submit", "/logs-checks/spark_checks.py", mode, run_id],
            timeout=1200,
        )
        return self.result(result.stdout)

    def close(self):
        if self.safe:
            self.command(
                [
                    "logs",
                    "--no-color",
                    "--tail",
                    "100",
                    "platform-app",
                    "kafka",
                    "flink-jobmanager",
                    "flink-taskmanager",
                ],
                check=False,
            )
        super().close()
