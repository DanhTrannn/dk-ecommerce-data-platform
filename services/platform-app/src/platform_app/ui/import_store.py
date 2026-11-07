"""Durable import attempts, separate from immutable generation artifacts."""

import fcntl
import json
import os
import re
import subprocess
import sys
import uuid
from threading import Thread
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from sqlalchemy.engine import URL, make_url

from platform_app.ui.job_store import JobStore, JobStoreError
from platform_app.ui.paths import repo_root

ACTIVE = {"queued", "running"}
BLOCKING = {"succeeded", "already_present", "unknown"}


def target_for(url: URL | str) -> dict[str, str]:
    import hashlib

    url = make_url(url)
    if url.get_backend_name() != "postgresql":
        raise ValueError("PostgreSQL is required")
    host = url.query.get("host") or url.host or "localhost"
    identity = json.dumps([str(host), url.port or 5432, url.database], ensure_ascii=False)
    return {"id": hashlib.sha256(identity.encode()).hexdigest(), "label": f"{host}:{url.port or 5432}/{url.database}"}


def configured_target() -> dict[str, str]:
    from platform_app.api.core.config import get_settings

    return target_for(get_settings().database_url)


def _launch(job_dir: Path, attempt_id: str) -> int:
    process = subprocess.Popen(
        [sys.executable, "-m", "platform_app.ui.import_worker", str(job_dir), attempt_id],
        cwd=str(repo_root()),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    # Keep the handle alive and reap the child without blocking Streamlit.
    Thread(target=process.wait, name=f"import-reaper-{attempt_id[:8]}", daemon=True).start()
    return process.pid


def _alive(pid: int | None, attempt_id: str) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        command = Path(f"/proc/{pid}/cmdline")
        if command.exists():
            value = command.read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
            return "platform_app.ui.import_worker" in value and attempt_id in value
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def stamp() -> str:
    return datetime.now(UTC).isoformat()


class ImportStore:
    def __init__(self, jobs: JobStore, launcher: Callable | None = None, alive: Callable | None = None):
        self.jobs, self.root = jobs, jobs.root
        self.launcher, self.alive = launcher or _launch, alive or _alive

    def job(self, job_id):
        if not job_id or Path(job_id).name != job_id or job_id.startswith("."):
            raise JobStoreError("Invalid job ID")
        path = self.root / job_id
        if path.is_symlink() or path.resolve().parent != self.root.resolve():
            raise JobStoreError("Invalid job directory")
        return self.jobs.get_job(job_id)

    def _path(self, job_id, attempt_id):
        if not re.fullmatch(r"[0-9a-f]{32}", attempt_id):
            raise JobStoreError("Invalid import ID")
        directory = self.job(job_id).directory / "imports"
        if directory.is_symlink():
            raise JobStoreError("Invalid import directory")
        path = directory / f"{attempt_id}.json"
        if path.is_symlink():
            raise JobStoreError("Invalid import record")
        return path

    def read(self, job_id, attempt_id):
        value = json.loads(self._path(job_id, attempt_id).read_text(encoding="utf-8"))
        if value["attempt_id"] != attempt_id or value["job_id"] != job_id:
            raise JobStoreError("Import record identity mismatch")
        return value

    def history(self, job_id):
        directory = self.job(job_id).directory / "imports"
        if directory.is_symlink():
            raise JobStoreError("Invalid import directory")
        return sorted(
            (self.read(job_id, path.stem) for path in directory.glob("*.json")),
            key=lambda row: (row["created_at"], row["attempt_id"]),
            reverse=True,
        )

    def _write(self, record):
        # All mutations hold the import lock; generation state is never rewritten.
        JobStore._write_json(self._path(record["job_id"], record["attempt_id"]), record)

    @contextmanager
    def lock(self, *, execution=False):
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / (".import-execution.lock" if execution else ".import.lock")).open("a+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | (fcntl.LOCK_NB if execution else 0))
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _all(self):
        return [record for job in self.jobs.list_jobs() for record in self.history(job.job_id)]

    def reconcile(self):
        with self.lock():
            for record in self._all():
                if record["status"] in ACTIVE and not self.alive(record["pid"], record["attempt_id"]):
                    record.update(
                        status="unknown",
                        finished_at=stamp(),
                        error_code="worker_lost",
                        message="Worker đã dừng; chưa xác định kết quả giao dịch. Kiểm tra database trước khi nạp lại.",
                    )
                    self._write(record)

    def sql_artifact(self, job_id):
        job = self.job(job_id)
        if job.status not in {"succeeded", "partial"} or job.state["stages"]["sql"]["status"] != "succeeded":
            raise JobStoreError("Chỉ nạp được SQL của job đã hoàn tất và sinh SQL thành công.")
        artifacts = [item for item in job.state.get("artifacts", []) if item.get("kind") == "sql"]
        if len(artifacts) != 1:
            raise JobStoreError("Job phải có đúng một SQL artifact.")
        artifact = artifacts[0]
        relative = Path(artifact["path"])
        path = job.directory / relative
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.parts[:2] != ("outputs", "sql")
            or path.suffix != ".sql"
        ):
            raise JobStoreError("Đường dẫn SQL không hợp lệ.")
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(job.directory.resolve()):
            raise JobStoreError("SQL artifact không còn trong thư mục job.")
        if not re.fullmatch(r"[0-9a-f]{64}", str(artifact.get("sha256", ""))):
            raise JobStoreError("SQL artifact chưa có checksum hợp lệ.")
        return path, artifact

    def start(self, job_id, target):
        self.reconcile()
        with self.lock():
            _, artifact = self.sql_artifact(job_id)
            job = self.job(job_id)
            for previous in self._all():
                if previous["status"] in ACTIVE:
                    raise JobStoreError("Đang có một tác vụ nạp PostgreSQL; chờ hoàn tất.")
                if (
                    previous["target"]["id"] == target["id"]
                    and previous["dataset_identity"] == job.state["dataset_identity"]
                    and previous["status"] in BLOCKING
                ):
                    raise JobStoreError(
                        "Dataset đã nạp, đã có trong database hoặc chưa xác định kết quả; không tự nạp lại."
                    )
            record = dict(
                attempt_id=uuid.uuid4().hex,
                job_id=job_id,
                target=target,
                dataset_identity=job.state["dataset_identity"],
                artifact=artifact["path"],
                sql_sha256=artifact["sha256"],
                status="queued",
                pid=None,
                created_at=stamp(),
                started_at=None,
                finished_at=None,
                receipt=None,
                error_code=None,
                message=None,
            )
            self._write(record)
            try:
                record["pid"] = int(self.launcher(job.directory, record["attempt_id"]))
                if record["pid"] <= 0:
                    raise ValueError("Invalid worker PID")
            except Exception:
                record.update(
                    status="failed",
                    finished_at=stamp(),
                    error_code="launch_failed",
                    message="Không khởi động được worker nạp dữ liệu.",
                )
                self._write(record)
                raise JobStoreError(record["message"]) from None
            self._write(record)
            return record

    def claim(self, job_id, attempt_id):
        with self.lock():
            record = self.read(job_id, attempt_id)
            if record["status"] != "queued" or record["pid"] != os.getpid():
                raise JobStoreError("Import attempt is not assigned to this worker")
            record.update(status="running", started_at=stamp())
            self._write(record)
            return record

    def finish(self, record, status, *, receipt=None, code=None, message=None):
        with self.lock():
            latest = self.read(record["job_id"], record["attempt_id"])
            if latest["status"] != "running" or latest["pid"] != os.getpid():
                raise JobStoreError("Import attempt is no longer owned by this worker")
            latest.update(status=status, finished_at=stamp(), receipt=receipt, error_code=code, message=message)
            self._write(latest)
