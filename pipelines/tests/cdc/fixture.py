"""Reuse private Compose isolation and provider provisioning for CDC acceptance."""

import json
import os
import time

import yaml

from stack.fixture import ROOT, StackFixture


class CDCStack(StackFixture):
    def __init__(self):
        super().__init__()
        extra = self.work / "cdc.yml"
        extra.write_text(
            yaml.safe_dump(
                {
                    "services": {
                        "platform-app": {"volumes": [f"{ROOT}/pipelines/tests/cdc:/cdc-checks:ro"]},
                        "spark-client": {"volumes": [f"{ROOT}/pipelines/tests/cdc:/cdc-checks:ro"]},
                        "flink-jobmanager": {
                            "image": os.environ.get("CDC_FLINK_IMAGE", "lakehouse-flink:1.19-iceberg-1.10.1"),
                            "mem_limit": "2g",
                        },
                        "flink-taskmanager": {
                            "image": os.environ.get("CDC_FLINK_IMAGE", "lakehouse-flink:1.19-iceberg-1.10.1"),
                            "mem_limit": "3g",
                        },
                        "flink-cdc-submit": {
                            "image": os.environ.get("CDC_FLINK_IMAGE", "lakehouse-flink:1.19-iceberg-1.10.1")
                        },
                        "kafka": {"environment": {"KAFKA_HEAP_OPTS": "-Xms128m -Xmx512m"}, "mem_limit": "1500m"},
                        "debezium-connect": {"mem_limit": "1500m"},
                    }
                }
            )
        )
        self.base.extend(["--profile", "streaming", "-f", str(extra)])
        config = json.loads(self.command(["config", "--format", "json"], private=True).stdout)
        assert all(not s.get("ports") for s in config["services"].values())
        assert all(v["name"].startswith(self.project + "_") for v in config["volumes"].values())

    def http(self, origin, method, path, body=None):
        args = ["exec", "-T", "platform-app", "python", "/cdc-checks/runtime.py", origin, method, path]
        if body is not None:
            args.append(json.dumps(body))
        return self.result(self.command(args).stdout)

    def query(self, sql):
        return self.runtime("query", "--sql", sql)

    def up(self, *services):
        self.command(
            ["up", "-d", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "240", *services], timeout=300
        )

    def wait(self, label, check, timeout=300):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            value = check()
            if value:
                print("CDC_PROGRESS " + label, flush=True)
                return value
            last = value
            time.sleep(3)
        raise AssertionError(f"CDC timed out: {label}; last={last}")

    def job(self):
        jobs = self.http("flink", "GET", "/jobs/overview")["jobs"]
        active = [job for job in jobs if job["state"] == "RUNNING"]
        assert len(active) == 1, jobs
        return active[0]["jid"]

    def checkpoints(self, job):
        return self.http("flink", "GET", f"/jobs/{job}/checkpoints")

    def connector_running(self):
        try:
            status = self.http("connect", "GET", "/connectors/postgres-ecommerce-cdc/status")
        except RuntimeError:
            return False
        tasks = status.get("tasks", [])
        return (
            status.get("connector", {}).get("state") == "RUNNING"
            and bool(tasks)
            and all(task["state"] == "RUNNING" for task in tasks)
        )

    def submit(self, *extra):
        result = self.command(["run", "--rm", "--no-deps", "-T", "flink-cdc-submit", *extra], timeout=300)
        self.evidence["last_submission"] = "submitted"
        return result

    def configure_source(self, check=True):
        return self.command(["exec", "-T", "postgres", "sh", "/opt/project/configure-source-cdc.sh"], check=check)

    def close(self):
        if self.safe:
            self.command(
                [
                    "logs",
                    "--no-color",
                    "--tail",
                    "120",
                    "flink-jobmanager",
                    "flink-taskmanager",
                    "debezium-connect",
                    "kafka",
                ],
                check=False,
            )
            # Captured before down; report IDs/counts only, never connector configuration/passwords.
            try:
                self.evidence["connector_status"] = self.http(
                    "connect", "GET", "/connectors/postgres-ecommerce-cdc/status"
                )
            except Exception:
                pass
        super().close()
