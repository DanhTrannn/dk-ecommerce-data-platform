"""Opt-in provisioning against temporary real PostgreSQL, MinIO and Polaris."""

import os
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]


@unittest.skipUnless(os.environ.get("RUN_PROVISION_TESTS") == "1", "opt-in disposable storage/catalog provisioning")
class LiveProvisionTests(unittest.TestCase):
    def command(self, command, *, check=True, **kwargs):
        result = subprocess.run(command, capture_output=True, text=True, **kwargs)
        if check and result.returncode:
            output = result.stdout + result.stderr
            for value in self.secrets:
                output = output.replace(value, "[redacted]")
            self.fail("Disposable provisioning command failed:\n" + output[-12000:])
        return result

    def test_minio_and_polaris_bootstrap_permissions_and_rerun(self):
        suffix = uuid.uuid4().hex[:10]
        network, pg, minio, server = (
            f"tlcn-provision-{kind}-{suffix}" for kind in ("network", "postgres", "minio", "polaris")
        )
        self.secrets = []
        temporary = tempfile.TemporaryDirectory(prefix="tlcn-provision-")
        self.addCleanup(temporary.cleanup)
        work = Path(temporary.name)
        image = os.environ.get("PROVISION_TEST_IMAGE", "tlcn-provision-check:local")
        self.command(["docker", "network", "create", "--internal", network])
        self.addCleanup(lambda: subprocess.run(["docker", "network", "rm", network], capture_output=True))
        for name in (pg, minio, server):
            self.addCleanup(lambda name=name: subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True))
        for name in ("spark-client", "spark-airflow", "flink", "trino-reader", "state"):
            (work / name).mkdir()
        # Consumer outputs are chowned to real workload UIDs; restore temporary ownership for cleanup.
        self.addCleanup(
            lambda: subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--pull",
                    "never",
                    "--network",
                    "none",
                    "--user",
                    "0:0",
                    "-v",
                    f"{work}:/work",
                    "--entrypoint",
                    "sh",
                    image,
                    "-c",
                    f"chown -R {os.getuid()}:{os.getgid()} /work && chmod -R u+rwX /work",
                ],
                capture_output=True,
            )
        )
        env = {
            "POSTGRES_USER": "postgres",
            "POSTGRES_DB": "polaris",
            "POSTGRES_PASSWORD": uuid.uuid4().hex,
            "MINIO_ROOT_USER": "provision-root",
            "MINIO_ROOT_PASSWORD": uuid.uuid4().hex,
            "MINIO_LAKEHOUSE_BUCKET": "lakehouse",
            "POLARIS_REALM": "POLARIS",
            "POLARIS_ROOT_CLIENT_ID": "root",
            "POLARIS_ROOT_CLIENT_SECRET": uuid.uuid4().hex,
            "POLARIS_REALM_CONTEXT_REALMS": "POLARIS",
            "POLARIS_PERSISTENCE_TYPE": "relational-jdbc",
            "POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE": "postgresql",
            "QUARKUS_DATASOURCE_JDBC_URL": "jdbc:postgresql://source-postgres:5432/polaris",
            "QUARKUS_DATASOURCE_USERNAME": "postgres",
            "QUARKUS_OTEL_SDK_DISABLED": "true",
            "AWS_REGION": "us-east-1",
            "POLARIS_CATALOG_NAME": "lakehouse",
        }
        env["QUARKUS_DATASOURCE_PASSWORD"] = env["POSTGRES_PASSWORD"]
        for kind in ("POLARIS", "SPARK", "FLINK", "TRINO"):
            env[f"MINIO_{kind}_ACCESS_KEY"] = f"test-{kind.lower()}"
            env[f"MINIO_{kind}_SECRET_KEY"] = uuid.uuid4().hex
        env["AWS_ACCESS_KEY_ID"] = env["MINIO_POLARIS_ACCESS_KEY"]
        env["AWS_SECRET_ACCESS_KEY"] = env["MINIO_POLARIS_SECRET_KEY"]
        self.secrets = [value for key, value in env.items() if "SECRET" in key or "PASSWORD" in key]
        env_file = work / "environment"
        env_file.write_text("".join(f"{key}={value}\n" for key, value in env.items()))
        env_file.chmod(0o600)
        self.command(
            [
                "docker",
                "run",
                "-d",
                "--pull",
                "never",
                "--name",
                pg,
                "--network",
                network,
                "--network-alias",
                "source-postgres",
                "--tmpfs",
                "/var/lib/postgresql/data:rw,size=256m",
                "--env-file",
                str(env_file),
                "postgres:16.8-alpine",
            ]
        )
        for _ in range(120):
            result = self.command(
                [
                    "docker",
                    "exec",
                    pg,
                    "sh",
                    "-c",
                    'test "$(cat /proc/1/comm)" = postgres && pg_isready -U postgres -d polaris',
                ],
                check=False,
            )
            if result.returncode == 0:
                break
            time.sleep(0.25)
        else:
            self.fail("Temporary PostgreSQL not ready")
        self.command(
            [
                "docker",
                "exec",
                pg,
                "psql",
                "-U",
                "postgres",
                "-d",
                "polaris",
                "-c",
                "CREATE SCHEMA polaris_schema AUTHORIZATION postgres",
            ]
        )
        self.command(
            [
                "docker",
                "run",
                "-d",
                "--pull",
                "never",
                "--name",
                minio,
                "--network",
                network,
                "--network-alias",
                "minio",
                "--env-file",
                str(env_file),
                "--tmpfs",
                "/data:rw,mode=1777,size=512m",
                "-v",
                f"{ROOT}/infrastructure/minio:/opt/project/minio:ro",
                "cgr.dev/chainguard/minio:latest-dev",
                "server",
                "/data",
            ]
        )
        for _ in range(80):
            if (
                self.command(
                    ["docker", "exec", minio, "wget", "-q", "--spider", "http://127.0.0.1:9000/minio/health/live"],
                    check=False,
                ).returncode
                == 0
            ):
                break
            time.sleep(0.25)
        else:
            self.fail("Temporary MinIO not ready")
        for _ in range(2):
            self.command(["docker", "exec", "--user", "0:0", minio, "sh", "/opt/project/minio/provision.sh"])
        wrong = self.command(
            [
                "docker",
                "exec",
                "--user",
                "0:0",
                "-e",
                "MINIO_SPARK_SECRET_KEY=deliberately-wrong-test-secret",
                minio,
                "sh",
                "/opt/project/minio/provision.sh",
            ],
            check=False,
        )
        self.assertNotEqual(wrong.returncode, 0)
        for secret in self.secrets:
            self.assertNotIn(secret, wrong.stdout + wrong.stderr)
        # A wrong supplied secret must not have changed the existing account password.
        self.command(["docker", "exec", "--user", "0:0", minio, "sh", "/opt/project/minio/provision.sh"])
        # Verify S3 permissions through the actual mc client without displaying credentials.
        self.command(
            [
                "docker",
                "exec",
                "--user",
                "0:0",
                minio,
                "sh",
                "-c",
                """set -eu
umask 077
export MC_CONFIG_DIR=$(mktemp -d)
trap 'rm -rf "$MC_CONFIG_DIR"' EXIT
mc alias set writer http://127.0.0.1:9000 "$MINIO_SPARK_ACCESS_KEY" "$MINIO_SPARK_SECRET_KEY" >/dev/null 2>&1
mc alias set reader http://127.0.0.1:9000 "$MINIO_TRINO_ACCESS_KEY" "$MINIO_TRINO_SECRET_KEY" >/dev/null 2>&1
printf 'test' | mc pipe writer/lakehouse/warehouse/permission-check >/dev/null 2>&1
mc cat reader/lakehouse/warehouse/permission-check >/dev/null 2>&1
if printf 'test' | mc pipe reader/lakehouse/warehouse/forbidden >/dev/null 2>&1; then exit 1; fi
if printf 'test' | mc pipe writer/lakehouse/forbidden/object >/dev/null 2>&1; then exit 1; fi
""",
            ]
        )
        self.command(
            [
                "docker",
                "run",
                "--rm",
                "--pull",
                "never",
                "--network",
                network,
                "--env-file",
                str(env_file),
                "--entrypoint",
                "/bin/sh",
                "apache/polaris-admin-tool:1.8.0",
                "-c",
                'java -jar /deployments/polaris-admin-tool.jar bootstrap --realm="$POLARIS_REALM" --credential="$POLARIS_REALM,$POLARIS_ROOT_CLIENT_ID,$POLARIS_ROOT_CLIENT_SECRET"',
            ],
            timeout=90,
        )
        self.command(
            [
                "docker",
                "run",
                "-d",
                "--pull",
                "never",
                "--name",
                server,
                "--network",
                network,
                "--network-alias",
                "polaris",
                "--env-file",
                str(env_file),
                "apache/polaris:1.8.0",
            ]
        )
        for _ in range(120):
            if (
                self.command(
                    ["docker", "exec", server, "curl", "-sf", "http://localhost:8182/q/health"], check=False
                ).returncode
                == 0
            ):
                break
            time.sleep(0.5)
        else:
            self.fail("Temporary Polaris not ready")
        command = [
            "docker",
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            network,
            "--env-file",
            str(env_file),
            "--user",
            "0:0",
        ]
        for directory in ("spark-client", "spark-airflow", "flink", "trino-reader"):
            command += ["-v", f"{work / directory}:/run/polaris-output/{directory}"]
        command += [
            "-v",
            f"{work / 'state'}:/run/polaris-state",
            "-v",
            f"{ROOT}/services/platform-app/tests/provisioning:/checks:ro",
            "--entrypoint",
            "python",
            image,
            "/checks/live_check.py",
        ]
        initialized = self.command(command[:-1] + ["-m", "platform_app.provisioning.polaris"], timeout=120)
        self.assertIn('"credential_outputs": 4', initialized.stdout)
        result = self.command(command, timeout=120)
        self.assertIn("POLARIS_PROVISION_CHECK_OK", result.stdout)
        print(result.stdout.strip())
        if os.environ.get("RUN_ACCESS_EXERCISE") == "1":
            env.update(
                {
                    "API_DATABASE_URL": "postgresql+psycopg2://postgres:"
                    + env["POSTGRES_PASSWORD"]
                    + "@source-postgres:5432/polaris",
                    "API_SECRET_KEY": uuid.uuid4().hex,
                    "API_BOOTSTRAP_ADMIN_EMAIL": "admin@example.com",
                    "API_BOOTSTRAP_ADMIN_PASSWORD": uuid.uuid4().hex,
                    "ACCESS_ADMIN_AUDIT_PATH": "/tmp/access-audit/audit.jsonl",
                }
            )
            self.secrets.extend([env["API_SECRET_KEY"], env["API_BOOTSTRAP_ADMIN_PASSWORD"]])
            env_file.write_text("".join(f"{key}={value}\n" for key, value in env.items()))
            self.command(
                ["docker", "exec", "-i", pg, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "polaris"],
                input=(ROOT / "database/schema.sql").read_text(),
            )
            access_command = command[:-4] + [
                "-v",
                f"{ROOT}/services/platform-app/tests/access_admin:/access-checks:ro",
                "--entrypoint",
                "python",
                image,
                "/access-checks/live_check.py",
            ]
            live = self.command(access_command, timeout=180)
            self.assertIn("ACCESS_ADMIN_LIVE_OK", live.stdout)
            print(live.stdout.strip())
            nonroot = self.command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--pull",
                    "never",
                    "--network",
                    network,
                    "--env-file",
                    str(env_file),
                    "--user",
                    "1000:1000",
                    "-e",
                    "ACCESS_ADMIN_AUDIT_PATH=/tmp/access-nonroot/audit.jsonl",
                    "-v",
                    f"{ROOT}/services/platform-app/tests/access_admin:/access-checks:ro",
                    "--entrypoint",
                    "python",
                    image,
                    "/access-checks/nonroot_check.py",
                ],
                timeout=180,
            )
            self.assertIn("ACCESS_ADMIN_NONROOT_OK", nonroot.stdout)
            print(nonroot.stdout.strip())
