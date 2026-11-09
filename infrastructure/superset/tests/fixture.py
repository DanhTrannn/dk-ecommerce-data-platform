"""Private Superset/Trino acceptance without starting Airflow or Kafka."""

import json
import os
import subprocess

import yaml

from stack.fixture import ROOT, StackFixture


class BIStack(StackFixture):
    def __init__(self):
        super().__init__()
        services = {}
        for name in ["superset", "superset-init", "superset-sync"]:
            services[name] = dict(
                image=os.environ.get("BI_TEST_IMAGE", "lakehouse-superset:6.1.0"),
                environment={"SUPERSET_ADMIN_USER": "admin"},
                mem_limit="2g",
                volumes=[f"{ROOT}/infrastructure/superset/tests:/bi-checks:ro"],
            )
        services["spark-client"] = {"volumes": [f"{ROOT}/infrastructure/superset/tests:/bi-checks:ro"]}
        extra = self.work / "bi.yml"
        extra.write_text(yaml.safe_dump({"services": services}))
        self.base.extend(["--profile", "bi", "-f", str(extra)])
        config = json.loads(self.command(["config", "--format", "json"], private=True).stdout)
        assert all(not s.get("ports") for s in config["services"].values())
        assert all(v["name"].startswith(self.project + "_") for v in config["volumes"].values())

    def up(self, *services):
        self.command(
            ["up", "-d", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "240", *services], timeout=360
        )

    def initialize(self):
        self.command(["exec", "-T", "postgres", "sh", "/opt/project/configure-superset.sh"])
        self.command(["run", "--rm", "--no-deps", "-T", "superset-init"], timeout=360)

    def sync(self, check=True):
        result = self.command(["run", "--rm", "--no-deps", "-T", "superset-sync"], timeout=180, check=check)
        if result.returncode:
            return None
        lines = [
            line.removeprefix("SUPERSET_RESULT ")
            for line in result.stdout.splitlines()
            if line.startswith("SUPERSET_RESULT ")
        ]
        return json.loads(lines[-1])

    def inspect(self, mode, *args):
        payload = (
            json.dumps({"username": "admin", "password": self.variables["SUPERSET_ADMIN_PASSWORD"]})
            if mode == "http"
            else None
        )
        return self.result(
            self.command(
                ["exec", "-T", "superset", "python", "/bi-checks/runtime.py", mode, *args],
                timeout=180,
                input=payload,
            ).stdout
        )

    def batch(self, mode):
        result = self.command(
            [
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "spark-client",
                "/opt/spark/bin/spark-submit",
                "--conf",
                "spark.sql.shuffle.partitions=4",
                "/bi-checks/spark_checks.py",
                mode,
            ],
            timeout=1200,
        )
        return self.result(result.stdout)

    def browser(self, slug, next_url=None):
        executable = os.environ.get("BI_BROWSER_PYTHON")
        if not executable:
            return {"skipped": "Set BI_BROWSER_PYTHON to enable the optional Chromium check"}
        container = self.command(["ps", "-q", "superset"]).stdout.strip()
        network = json.loads(subprocess.check_output(["docker", "inspect", container], text=True))[0][
            "NetworkSettings"
        ]["Networks"]
        address = network[self.project]["IPAddress"]
        config = self.work / "browser.json"
        config.write_text(
            json.dumps(
                dict(
                    url=f"http://{address}:8088/superset/dashboard/{slug}/",
                    next_url=f"http://{address}:8088{next_url}" if next_url else None,
                    username="admin",
                    password=self.variables["SUPERSET_ADMIN_PASSWORD"],
                    chromium=os.environ.get("BI_CHROMIUM_PATH"),
                    screenshot=str(self.report / "sales-dashboard.png"),
                )
            )
        )
        config.chmod(0o600)
        result = subprocess.run(
            [executable, str(ROOT / "infrastructure/superset/tests/browser.py"), str(config)],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode:
            raise RuntimeError(self.redact(result.stdout + result.stderr))
        return self.result(result.stdout)

    def close(self):
        if self.safe:
            self.command(["logs", "--no-color", "--tail", "100", "superset"], check=False)
        super().close()
