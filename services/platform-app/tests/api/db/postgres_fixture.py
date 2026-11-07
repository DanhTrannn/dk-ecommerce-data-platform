"""Shared isolated PostgreSQL lifecycle for opt-in integration tests."""

import os
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from platform_app.api.db.schema import render_schema


class PostgresFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory(prefix="tlcn-pg-schema-")
        cls.addClassCleanup(temporary.cleanup)
        socket = Path(temporary.name) / "socket"
        socket.mkdir(mode=0o777)
        socket.chmod(0o777)
        cls.container = "tlcn-pg-schema-" + uuid.uuid4().hex[:12]
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--rm",
                "--pull",
                "never",
                "--name",
                cls.container,
                "--network",
                "none",
                "--tmpfs",
                "/var/lib/postgresql/data:rw,size=256m",
                "--mount",
                f"type=bind,source={socket},target=/var/run/postgresql",
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e",
                "POSTGRES_DB=schema_tests",
                os.environ.get("POSTGRES_TEST_IMAGE", "postgres:16.8-alpine"),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        cls.addClassCleanup(cls.cleanup_container)
        # Docker entrypoint uses a temporary server during init; wait for final PID 1.
        for _ in range(120):
            ready = subprocess.run(
                [
                    "docker",
                    "exec",
                    cls.container,
                    "sh",
                    "-c",
                    'test "$(cat /proc/1/comm)" = postgres && pg_isready -U postgres -d schema_tests',
                ],
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("Isolated PostgreSQL did not become ready")
        cls.engine = create_engine(
            URL.create("postgresql+psycopg2", username="postgres", database="schema_tests", query={"host": str(socket)})
        )
        cls.addClassCleanup(cls.engine.dispose)
        with cls.engine.begin() as connection:
            connection.exec_driver_sql(render_schema())

    @classmethod
    def cleanup_container(cls):
        # Remove the socket directory's sticky bit even if the server crashed.
        subprocess.run(
            ["docker", "exec", "--user", "root", cls.container, "chmod", "0777", "/var/run/postgresql"],
            capture_output=True,
        )
        subprocess.run(["docker", "stop", "--time", "5", cls.container], capture_output=True)
        subprocess.run(["docker", "rm", "-f", "-v", cls.container], capture_output=True)
