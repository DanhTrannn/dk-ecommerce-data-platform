"""Reuse the private source/log deployment; add only a one-off Python pilot service."""

import copy
import json
import os

import yaml

from logs.fixture import LogsStack
from stack.fixture import ROOT


class PolarsStack(LogsStack):
    def __init__(self):
        super().__init__()
        service = copy.deepcopy(yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]["spark-client"])
        for key in ("build", "depends_on", "command"):
            service.pop(key, None)
        service.update(
            image=os.environ.get("POLARS_TEST_IMAGE", "tlcn-polars-pilot:local"),
            entrypoint=["python"],
            mem_limit="1g",
            volumes=[
                "polaris-spark-client-credentials:/run/polaris:ro",
                f"{ROOT}/pipelines/tests/polars_checks:/polars-checks:ro",
            ],
        )
        service["environment"]["PYTHONPATH"] = "/app/pipelines/src"
        extra = self.work / "polars.yml"
        extra.write_text(
            yaml.safe_dump(
                {
                    "services": {
                        "polars-pilot": service,
                        "spark-client": {"volumes": [f"{ROOT}/pipelines/tests/polars_checks:/polars-checks:ro"]},
                    }
                }
            )
        )
        self.base.extend(["-f", str(extra)])
        config = json.loads(self.command(["config", "--format", "json"], private=True).stdout)
        assert all(not value.get("ports") for value in config["services"].values())
        assert all(value["name"].startswith(self.project + "_") for value in config["volumes"].values())

    def pilot(self, mode, *args, check=True):
        result = self.command(
            ["run", "--rm", "--no-deps", "-T", "polars-pilot", "/polars-checks/checks.py", mode, *args],
            timeout=240,
            check=check,
        )
        return self.result(result.stdout) if result.returncode == 0 else result

    def spark_compat(self, mode):
        return self.result(
            self.command(
                [
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "spark-client",
                    "/opt/spark/bin/spark-submit",
                    "--conf",
                    "spark.sql.shuffle.partitions=2",
                    "/polars-checks/spark_compat.py",
                    mode,
                ],
                timeout=300,
            ).stdout
        )
