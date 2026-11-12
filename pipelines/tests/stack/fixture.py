"""Compose-based acceptance fixture: same deployment, unique network/volumes, no host ports."""

import base64
import json
import os
import re
import subprocess
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]


class Replace(list):
    pass


class OverrideDumper(yaml.SafeDumper):
    pass


OverrideDumper.add_representer(Replace, lambda dumper, value: dumper.represent_sequence("!override", value))


class StackFixture:
    def __init__(self):
        self.project = "tlcn-stack-" + uuid.uuid4().hex[:10]
        self.temporary = tempfile.TemporaryDirectory(prefix=self.project + "-")
        self.work = Path(self.temporary.name)
        self.report = ROOT / "data/pipeline-runs" / self.project
        self.report.mkdir(parents=True, exist_ok=True)
        self.log = self.report / "commands.log"
        self.secrets = []
        self.evidence = {"project": self.project, "status": "running", "started_at": datetime.now(UTC).isoformat()}
        self.safe = False
        self.prepare()

    def redact(self, text):
        for value in sorted(set(self.secrets), key=len, reverse=True):
            if value:
                text = text.replace(value, "[redacted]")
        return re.sub(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "[redacted-jwt]", text)

    def prepare(self):
        variables = {}
        for line in (ROOT / ".env.example").read_text().splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                variables[key] = value
        for key in list(variables):
            if any(word in key for word in ("PASSWORD", "SECRET", "FERNET", "SALT")):
                variables[key] = uuid.uuid4().hex
        variables.update(
            AIRFLOW_UID="50000",
            PLATFORM_APP_UID=str(os.getuid()),
            PLATFORM_APP_GID=str(os.getgid()),
            AIRFLOW_FERNET_KEY=base64.urlsafe_b64encode(os.urandom(32)).decode(),
            API_SECRET_KEY=uuid.uuid4().hex,
            POSTGRES_ECOMMERCE_READER_PASSWORD=uuid.uuid4().hex,
            SILVER_PSEUDONYMIZE_SALT=uuid.uuid4().hex,
        )
        self.variables = variables
        self.secrets = [
            value
            for key, value in variables.items()
            if any(word in key for word in ("PASSWORD", "SECRET", "FERNET", "SALT"))
        ]
        self.environment = {**os.environ, **variables, "COMPOSE_PROFILES": ""}
        env = self.work / "environment"
        env.write_text("".join(f"{key}={value}\n" for key, value in variables.items()))
        env.chmod(0o600)
        data = self.work / "data"
        data.mkdir(mode=0o777)
        data.chmod(0o777)
        base = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
        services = {name: {"ports": Replace([])} for name, service in base["services"].items() if service.get("ports")}
        platform = os.environ.get("STACK_PLATFORM_IMAGE", "tlcn-platform-stack-check:local")
        airflow = os.environ.get("STACK_AIRFLOW_IMAGE", "tlcn-airflow-native:local")
        for name in ("airflow-init", "airflow-api-server", "airflow-scheduler", "airflow-dag-processor"):
            services.setdefault(name, {}).update(
                image=airflow,
                environment={
                    "AIRFLOW__CORE__PARALLELISM": "3",
                    "AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE": "false",
                    "AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "5",
                    "AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL": "5",
                    "AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES": "1",
                    "OPENBLAS_NUM_THREADS": "1",
                    "OMP_NUM_THREADS": "1",
                },
                volumes=[f"{ROOT}/pipelines/tests/stack:/opt/stack-checks:ro"],
            )
        services["airflow-scheduler"]["mem_limit"] = "2g"
        services["trino"]["mem_limit"] = "3g"
        services["polaris"]["mem_limit"] = "2g"
        services["lakehouse-provision"] = {
            "image": platform,
            "volumes": [f"{ROOT}/pipelines/tests/stack:/stack-checks:ro"],
        }
        services["trino-auth-init"] = {"image": platform}
        services["batch-client"] = {"image": os.environ.get("NATIVE_BATCH_IMAGE", "tlcn-native-batch:local")}
        services["platform-app"].update(
            image=platform,
            environment={
                "API_BOOTSTRAP_ADMIN_EMAIL": "admin@example.com",
                "API_BOOTSTRAP_ADMIN_PASSWORD": variables["API_SECRET_KEY"],
                "STACK_AIRFLOW_USER": variables["AIRFLOW_ADMIN_USER"],
                "STACK_AIRFLOW_PASSWORD": variables["AIRFLOW_ADMIN_PASSWORD"],
                "STACK_USE_PRESET_SCALE": "1" if os.environ.get("STACK_USE_PRESET_SCALE") == "1" else "0",
            },
            volumes=Replace(
                [f"{data}:/data", f"{ROOT}/pipelines/tests/stack:/stack-checks:ro", "trino-ca:/run/trino-ca:ro"]
            ),
        )
        self.secrets.append(variables["API_SECRET_KEY"])
        override = self.work / "override.yml"
        override.write_text(
            yaml.dump(
                {"services": services, "networks": {"lakehouse-network": {"name": self.project, "internal": True}}},
                Dumper=OverrideDumper,
            )
        )
        self.base = [
            "docker",
            "compose",
            "--project-name",
            self.project,
            "--project-directory",
            str(ROOT),
            "--env-file",
            str(env),
            "-f",
            str(ROOT / "docker-compose.yml"),
            "-f",
            str(override),
            "--profile",
            "core",
            "--profile",
            "batch",
        ]
        config = json.loads(self.command(["config", "--format", "json"], private=True).stdout)
        assert config["networks"]["lakehouse-network"]["name"] == self.project
        assert config["networks"]["lakehouse-network"]["internal"] is True
        assert all(not service.get("ports") for service in config["services"].values())
        assert all(value["name"].startswith(self.project + "_") for value in config["volumes"].values())
        self.safe = True

    def command(self, arguments, *, check=True, private=False, input=None, timeout=240):
        result = subprocess.run(
            self.base + arguments, env=self.environment, input=input, capture_output=True, text=True, timeout=timeout
        )
        output = self.redact(result.stdout + result.stderr)
        if not private:
            with self.log.open("a") as log:
                log.write("\n$ compose " + " ".join(arguments) + "\n" + output)
        if check and result.returncode:
            raise RuntimeError("Stack command failed: " + " ".join(arguments[:5]) + "\n" + output[-6000:])
        return result

    def runtime(self, mode, *arguments):
        result = self.command(["exec", "-T", "platform-app", "python", "/stack-checks/runtime.py", mode, *arguments])
        return self.result(result.stdout)

    @staticmethod
    def result(output):
        lines = [line.removeprefix("STACK_RESULT ") for line in output.splitlines() if line.startswith("STACK_RESULT ")]
        if not lines:
            raise RuntimeError("Acceptance helper did not return a result")
        return json.loads(lines[-1])

    def permission(self, action):
        result = self.command(
            [
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--entrypoint",
                "python",
                "lakehouse-provision",
                "/stack-checks/runtime.py",
                "permission",
                "--action",
                action,
            ]
        )
        return self.result(result.stdout)

    def sql(self, database_variable, statement):
        result = self.command(
            [
                "exec",
                "-T",
                "postgres",
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                self.variables["POSTGRES_USER"],
                "-d",
                self.variables[database_variable],
                "-At",
            ],
            input=statement,
        )
        return result.stdout.strip()

    def start_source(self):
        self.command(
            [
                "up",
                "-d",
                "--no-build",
                "--pull",
                "never",
                "--wait",
                "--wait-timeout",
                "180",
                "postgres",
                "minio",
                "polaris",
            ]
        )
        self.command(["exec", "-T", "--user", "0:0", "minio", "sh", "/opt/project/minio/provision.sh"])
        self.command(["run", "--rm", "--no-deps", "-T", "lakehouse-provision"])
        secrets = self.command(
            [
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--entrypoint",
                "python",
                "lakehouse-provision",
                "-c",
                "import pathlib,json; print(json.dumps([json.loads(p.read_text()).get('clientSecret') for p in pathlib.Path('/run/polaris-state').glob('*.json')]))",
            ],
            private=True,
        )
        self.secrets.extend(value for value in json.loads(secrets.stdout) if value)
        self.command(
            [
                "exec",
                "-T",
                "postgres",
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                self.variables["POSTGRES_ECOMMERCE_USER"],
                "-d",
                self.variables["POSTGRES_ECOMMERCE_DB"],
            ],
            input=(ROOT / "database/schema.sql").read_text(),
        )
        self.command(["up", "-d", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "180", "platform-app"])
        self.evidence["source"] = self.runtime("source")
        self.command(["exec", "-T", "postgres", "sh", "/opt/project/configure-source-reader.sh"])

    def start(self):
        self.start_source()
        self.command(
            [
                "up",
                "-d",
                "--no-build",
                "--pull",
                "never",
                "--wait",
                "--wait-timeout",
                "240",
                "airflow-api-server",
                "airflow-dag-processor",
                "airflow-scheduler",
                "trino",
            ],
            timeout=360,
        )
        self.runtime("airflow", "--action", "wait", "--dag", "init_lakehouse_oltp")
        self.runtime("airflow", "--action", "wait", "--dag", "lakehouse_oltp_pipeline")

    def wait_run(self, dag, run_id, timeout=None, *, expected_tasks=2):
        if timeout is None:
            timeout = 3600 if self.environment.get("STACK_USE_PRESET_SCALE") == "1" else 1500
        started = time.monotonic()
        last = None
        while time.monotonic() - started < timeout:
            query = f"""SELECT json_build_object('state', d.state, 'tasks', (SELECT json_object_agg(state, n) FROM
                (SELECT coalesce(state,'none') AS state,count(*) AS n FROM task_instance WHERE dag_id='{dag}' AND run_id='{run_id}' GROUP BY state) s))
                FROM dag_run d WHERE dag_id='{dag}' AND run_id='{run_id}';"""
            value = self.sql("AIRFLOW_DB_NAME", query)
            state = json.loads(value) if value else {}
            if state != last:
                print(f"STACK_PROGRESS {dag} {run_id} {json.dumps(state)}", flush=True)
                last = state
            if state.get("state") == "success":
                assert (
                    sum(state["tasks"].values()) == expected_tasks and state["tasks"].get("success") == expected_tasks
                ), state
                return dict(state=state, elapsed_seconds=round(time.monotonic() - started, 1))
            if state.get("state") == "failed" or any(
                key in (state.get("tasks") or {}) for key in ("failed", "up_for_retry")
            ):
                raise RuntimeError(f"Airflow run failed or needs retry: {dag} {run_id} {state}")
            time.sleep(3)
        raise RuntimeError(f"Airflow run timed out: {dag} {run_id}")

    def close(self):
        try:
            if self.safe:
                self.command(
                    [
                        "logs",
                        "--no-color",
                        "--tail",
                        "100",
                        "airflow-api-server",
                        "airflow-dag-processor",
                        "airflow-scheduler",
                        "trino",
                    ],
                    check=False,
                )
                tasks = self.command(
                    [
                        "exec",
                        "-T",
                        "airflow-scheduler",
                        "python",
                        "-c",
                        "import pathlib; files=list(pathlib.Path('/opt/airflow/logs').rglob('*.log')); print('\\n'.join(str(p)+'\\n'+p.read_text(errors='replace')[-18000:] for p in files if 'error' in p.read_text(errors='replace').lower()))",
                    ],
                    check=False,
                )
                (self.report / "task-errors.log").write_text(self.redact(tasks.stdout))
                self.command(["down", "--volumes", "--remove-orphans", "--timeout", "10"], check=False)
        finally:
            if self.evidence["status"] != "passed":
                self.evidence["status"] = "failed"
            self.evidence["finished_at"] = datetime.now(UTC).isoformat()
            (self.report / "report.json").write_text(json.dumps(self.evidence, ensure_ascii=False, indent=2))
            self.temporary.cleanup()
