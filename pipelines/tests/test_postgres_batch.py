"""Opt-in disposable PostgreSQL + MinIO + Spark integration; no user volumes."""

import os
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_BATCH_TESTS") == "1", "opt-in disposable Spark/PostgreSQL batch")
class PostgresBatchTests(unittest.TestCase):
    def command(self, args, **kwargs):
        return subprocess.run(args, check=True, capture_output=True, text=True, **kwargs)

    def test_source_reader_and_complete_batch(self):
        import yaml
        from generator.config import config_from_dict
        from generator.sql_export import export_sql

        temporary = tempfile.TemporaryDirectory(prefix="tlcn-pg-batch-")
        self.addCleanup(temporary.cleanup)
        work = Path(temporary.name)
        suffix = uuid.uuid4().hex[:10]
        network, pg, minio, spark = (
            f"tlcn-batch-{kind}-{suffix}" for kind in ("network", "postgres", "minio", "spark")
        )
        self.command(["docker", "network", "create", "--internal", network])
        self.addCleanup(lambda: subprocess.run(["docker", "network", "rm", network], capture_output=True))
        for container in (pg, minio, spark):
            self.addCleanup(
                lambda name=container: subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
            )
        reader_password, admin_password, minio_secret = (uuid.uuid4().hex for _ in range(3))
        environment = {
            "POSTGRES_PASSWORD": admin_password,
            "POSTGRES_DB": "ecommerce",
            "POSTGRES_USER": "postgres",
            "POSTGRES_ECOMMERCE_DB": "ecommerce",
            "POSTGRES_ECOMMERCE_READER_USER": "batch_reader",
            "POSTGRES_ECOMMERCE_READER_PASSWORD": reader_password,
            "POSTGRES_SOURCE_HOST": "source-postgres",
            "POSTGRES_SOURCE_PORT": "5432",
            "MINIO_ROOT_USER": "batch-root",
            "MINIO_ROOT_PASSWORD": minio_secret,
            "AWS_REGION": "us-east-1",
            "MINIO_ENDPOINT": "http://object-store:9000",
            "MINIO_LAKEHOUSE_BUCKET": "batch-check",
            "MINIO_SPARK_ACCESS_KEY": "batch-root",
            "MINIO_SPARK_SECRET_KEY": minio_secret,
            "SILVER_PSEUDONYMIZE_SALT": uuid.uuid4().hex,
            "PYTHONPATH": "/opt/project/pipelines/src",
        }
        env_file = work / "environment"
        env_file.write_text("".join(f"{key}={value}\n" for key, value in environment.items()))
        env_file.chmod(0o600)
        self.command(
            [
                "docker",
                "run",
                "--pull",
                "never",
                "-d",
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
                "-v",
                f"{ROOT}/infrastructure/postgres/configure-source-reader.sh:/configure-reader.sh:ro",
                os.environ.get("POSTGRES_TEST_IMAGE", "postgres:16.8-alpine"),
            ]
        )
        for _ in range(120):
            ready = subprocess.run(
                [
                    "docker",
                    "exec",
                    pg,
                    "sh",
                    "-c",
                    'test "$(cat /proc/1/comm)" = postgres && pg_isready -U postgres -d ecommerce',
                ],
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            self.fail("Disposable PostgreSQL did not start")

        def sql(statement):
            return self.command(
                [
                    "docker",
                    "exec",
                    "-i",
                    pg,
                    "psql",
                    "-X",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "postgres",
                    "-d",
                    "ecommerce",
                    "-At",
                ],
                input=statement,
            ).stdout.strip()

        missing = subprocess.run(["docker", "exec", pg, "sh", "/configure-reader.sh"], capture_output=True, text=True)
        self.assertNotEqual(missing.returncode, 0)
        self.assertEqual(sql("SELECT count(*) FROM pg_roles WHERE rolname='batch_reader';"), "0")
        sql((ROOT / "database/schema.sql").read_text())
        config = yaml.safe_load((ROOT / "generator/configs/small.yml").read_text())
        config["scale"] = dict(customers=10, products=2, variants=8, orders=30)
        dataset = work / "source.sql"
        export_sql(config_from_dict(config), dataset)
        sql(dataset.read_text())
        for index, instant in enumerate(
            ("2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00.123456+00:00", "2020-01-01T01:00:00+00:00")
        ):
            label = ("lower", "inside", "upper")[index]
            sql(
                f"INSERT INTO categories(category_id, public_id, code, name, created_at, updated_at) VALUES ({10000 + index}, '{uuid.uuid4()}', 'boundary-{label}', 'Boundary', '{instant}', '{instant}');"
            )
        for _ in range(2):
            self.command(["docker", "exec", pg, "sh", "/configure-reader.sh"])
        self.assertEqual(
            sql(
                "SELECT count(*) FROM information_schema.role_table_grants WHERE grantee='batch_reader' AND privilege_type='SELECT';"
            ),
            "27",
        )
        # Even overriding the read-only default cannot confer missing write grants.
        for query in (
            "SELECT * FROM customer_credentials;",
            "UPDATE orders SET status=status;",
            "INSERT INTO categories(code,name) VALUES ('forbidden','Forbidden');",
        ):
            denied = subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    pg,
                    "psql",
                    "-X",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "batch_reader",
                    "-d",
                    "ecommerce",
                ],
                input="SET default_transaction_read_only=off;\n" + query,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(denied.returncode, 0)
            self.assertIn("permission denied", denied.stderr)
        self.command(
            [
                "docker",
                "run",
                "--pull",
                "never",
                "-d",
                "--name",
                minio,
                "--network",
                network,
                "--network-alias",
                "object-store",
                "--tmpfs",
                "/data:rw,size=512m,mode=1777",
                "--env-file",
                str(env_file),
                "cgr.dev/chainguard/minio:latest-dev",
                "server",
                "/data",
            ]
        )
        for _ in range(80):
            ready = subprocess.run(
                ["docker", "exec", minio, "wget", "-q", "--spider", "http://127.0.0.1:9000/minio/health/live"],
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            self.fail("Disposable object store did not start")
        log_path = work / "spark.log"
        with log_path.open("w") as output:
            completed = subprocess.run(
                [
                    "docker",
                    "run",
                    "--pull",
                    "never",
                    "--name",
                    spark,
                    "--network",
                    network,
                    "--memory",
                    "6g",
                    "--user",
                    "root",
                    "--env-file",
                    str(env_file),
                    "-v",
                    f"{ROOT}/pipelines:/opt/project/pipelines:ro",
                    "--entrypoint",
                    "/opt/spark/bin/spark-submit",
                    os.environ.get("SPARK_TEST_IMAGE", "tlcn-batch-postgres-check:local"),
                    "--driver-memory",
                    "2g",
                    "/opt/project/pipelines/tests/postgres_spark_check.py",
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=900,
            )
        log = log_path.read_text()
        # Preserve sanitized failure evidence in the test output, never the env file.
        for secret in (reader_password, admin_password, minio_secret, environment["SILVER_PSEUDONYMIZE_SALT"]):
            log = log.replace(secret, "[redacted]")
        self.assertEqual(completed.returncode, 0, log[-18000:])
        self.assertIn("POSTGRES_BATCH_CHECK_OK", log)
        print(next(line for line in log.splitlines() if line.startswith("POSTGRES_BATCH_CHECK_OK")))
