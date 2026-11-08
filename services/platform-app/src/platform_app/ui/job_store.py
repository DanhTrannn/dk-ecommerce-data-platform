from __future__ import annotations

import copy
import fcntl
import json
import os
import secrets
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from platform_app.ui.paths import repo_root

SCHEMA_VERSION = 1
STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_INTERRUPTED = "interrupted"
ACTIVE_STATUSES = {STATUS_QUEUED, STATUS_RUNNING}
STAGE_ORDER = ("validate", "sql")
STAGE_PENDING = "pending"
STAGE_RUNNING = "running"
STAGE_SUCCEEDED = "succeeded"
STAGE_FAILED = "failed"
STAGE_SKIPPED = "skipped"
STAGE_INTERRUPTED = "interrupted"


class JobStoreError(Exception):
    """Base error for the persistent job store."""


class JobAlreadyRunning(JobStoreError):
    """Raised when a second job is requested while one is still active."""


class JobNotFound(JobStoreError):
    """Raised when a job directory/state is missing."""


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    directory: Path
    state: dict[str, Any]

    @property
    def status(self) -> str:
        return str(self.state.get("status", STATUS_FAILED))

    @property
    def created_at(self) -> str:
        return str(self.state.get("created_at", ""))

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_STATUSES


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _stage(status: str = STAGE_PENDING) -> dict[str, Any]:
    return {
        "status": status,
        "started_at": None,
        "finished_at": None,
        "exit_code": None,
        "command": None,
        "message": None,
        "summary": None,
    }


def _default_launcher(job_dir: Path) -> int:
    log_directory = job_dir / "logs"
    log_directory.mkdir(parents=True, exist_ok=True)
    log_path = log_directory / "worker.log"
    with log_path.open("ab") as log_stream:
        process = subprocess.Popen(
            [sys.executable, "-m", "platform_app.ui.worker", str(job_dir)],
            cwd=str(repo_root()),
            stdin=subprocess.DEVNULL,
            stdout=log_stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    return process.pid


class JobStore:
    def __init__(
        self,
        root: Path,
        launcher: Callable[[Path], int | None] | None = None,
    ) -> None:
        self.root = Path(root)
        self._launcher = launcher or _default_launcher

    def create_job(
        self,
        document: dict[str, Any],
        request: dict[str, Any],
        *,
        preset_name: str,
        config_yaml: str,
        generator_version: str,
        dataset_identity: str,
    ) -> JobRecord:
        self.root.mkdir(parents=True, exist_ok=True)
        with self._global_lock():
            self._reconcile_locked()
            active = self._read_active()
            active_job = self._read_job(active["job_id"]) if active else None
            if active_job is None:
                active_job = next((job for job in self.list_jobs() if job.is_active), None)
            if active_job is not None and active_job.is_active:
                raise JobAlreadyRunning(f"Job {active_job.job_id} is still active; wait for it to finish.")
            if active is not None:
                self._remove_active_locked()

            job_id = self._new_job_id()
            job_dir = self.root / job_id
            job_dir.mkdir(parents=True, exist_ok=False)
            (job_dir / "logs").mkdir()
            (job_dir / "outputs").mkdir()

            created_at = _now()
            state = {
                "schema_version": SCHEMA_VERSION,
                "job_id": job_id,
                "created_at": created_at,
                "started_at": None,
                "finished_at": None,
                "status": STATUS_QUEUED,
                "pid": None,
                "generator_version": generator_version,
                "dataset_identity": dataset_identity,
                "scenario_id": str(document.get("scenario_id", "")),
                "preset": preset_name,
                "output": "sql",
                "current_stage": "validate",
                "stages": {
                    "validate": _stage(),
                    "sql": _stage(),
                },
                "artifacts": [],
                "error": None,
            }
            request_record = {**request, "job_id": job_id, "created_at": created_at}
            self._write_text(job_dir / "config.yml", config_yaml)
            self._write_json(job_dir / "request.json", request_record)
            self._write_json(job_dir / "state.json", state)
            self._write_json(
                self.root / ".active.json",
                {"job_id": job_id, "pid": None, "created_at": created_at},
            )

            try:
                pid = self._launcher(job_dir)
            except Exception as error:
                state["status"] = STATUS_FAILED
                state["finished_at"] = _now()
                state["error"] = f"Could not start worker: {error}"
                self._write_json(job_dir / "state.json", state)
                self._remove_active_locked()
                return JobRecord(job_id, job_dir, state)

            if pid is not None:
                state["pid"] = int(pid)
                self._write_json(job_dir / "state.json", state)
                self._write_json(
                    self.root / ".active.json",
                    {"job_id": job_id, "pid": int(pid), "created_at": created_at},
                )
            return JobRecord(job_id, job_dir, state)

    def get_job(self, job_id: str) -> JobRecord:
        job = self._read_job(job_id)
        if job is None:
            raise JobNotFound(f"Job not found: {job_id}")
        return job

    def list_jobs(self) -> list[JobRecord]:
        if not self.root.exists():
            return []
        jobs = []
        for directory in self.root.iterdir():
            if not directory.is_dir() or directory.name.startswith("."):
                continue
            job = self._read_job(directory.name)
            if job is not None:
                jobs.append(job)
        return sorted(jobs, key=lambda job: (job.created_at, job.job_id), reverse=True)

    def active_job(self) -> JobRecord | None:
        active = self._read_active()
        if active is not None:
            job = self._read_job(str(active.get("job_id", "")))
            if job is not None and job.is_active:
                return job
        return next((job for job in self.list_jobs() if job.is_active), None)

    def reconcile(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with self._global_lock():
            self._reconcile_locked()

    def update_state(
        self,
        job_id: str,
        mutator: Callable[[dict[str, Any]], None],
    ) -> dict[str, Any]:
        with self._job_lock(job_id):
            state = self._read_state_or_raise(job_id)
            mutator(state)
            self._write_json(self.root / job_id / "state.json", state)
            return state

    def mark_started(self, job_id: str, pid: int) -> None:
        def update(state: dict[str, Any]) -> None:
            state["status"] = STATUS_RUNNING
            state["pid"] = int(pid)
            state["started_at"] = state.get("started_at") or _now()

        self.update_state(job_id, update)

    def mark_stage_running(self, job_id: str, stage: str, command: str) -> None:
        def update(state: dict[str, Any]) -> None:
            state["current_stage"] = stage
            stage_state = state["stages"][stage]
            stage_state["status"] = STAGE_RUNNING
            stage_state["started_at"] = _now()
            stage_state["command"] = command

        self.update_state(job_id, update)

    def complete_stage(
        self,
        job_id: str,
        stage: str,
        status: str,
        *,
        exit_code: int | None = None,
        message: str | None = None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        def update(state: dict[str, Any]) -> None:
            stage_state = state["stages"][stage]
            stage_state["status"] = status
            stage_state["finished_at"] = _now()
            stage_state["exit_code"] = exit_code
            stage_state["message"] = message
            if summary is not None:
                stage_state["summary"] = summary
            state["current_stage"] = stage

        self.update_state(job_id, update)

    def set_artifacts(self, job_id: str, artifacts: list[dict[str, Any]]) -> None:
        self.update_state(job_id, lambda state: state.__setitem__("artifacts", artifacts))

    def finish_job(self, job_id: str, status: str, error: str | None = None) -> None:
        def update(state: dict[str, Any]) -> None:
            state["status"] = status
            state["finished_at"] = _now()
            state["current_stage"] = None
            state["error"] = error

        self.update_state(job_id, update)

    def release_active(self, job_id: str) -> None:
        with self._global_lock():
            active = self._read_active()
            if active is not None and active.get("job_id") == job_id:
                self._remove_active_locked()

    def _reconcile_locked(self) -> None:
        active = self._read_active()
        active_job_id = str(active.get("job_id")) if active else None
        for job in self.list_jobs():
            if not job.is_active:
                continue
            pid = job.state.get("pid") or (active.get("pid") if active_job_id == job.job_id else None)
            if pid is not None and self._pid_matches(int(pid), job.job_id):
                continue
            self._mark_interrupted(job.job_id)
        if active is not None:
            job = self._read_job(active_job_id or "")
            if job is None or not job.is_active:
                self._remove_active_locked()

    def _mark_interrupted(self, job_id: str) -> None:
        def update(state: dict[str, Any]) -> None:
            state["status"] = STATUS_INTERRUPTED
            state["finished_at"] = _now()
            state["error"] = "Worker process is no longer running"
            for stage_state in state.get("stages", {}).values():
                if stage_state.get("status") == STAGE_RUNNING:
                    stage_state["status"] = STAGE_INTERRUPTED
                    stage_state["finished_at"] = _now()
                elif stage_state.get("status") == STAGE_PENDING:
                    stage_state["status"] = STAGE_SKIPPED
            state["current_stage"] = None

        self.update_state(job_id, update)

    @staticmethod
    def _pid_matches(pid: int, job_id: str) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            pass
        except OSError:
            return False
        cmdline_path = Path(f"/proc/{pid}/cmdline")
        if not cmdline_path.exists():
            return True
        try:
            cmdline = cmdline_path.read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            return True
        return "platform_app.ui.worker" in cmdline and job_id in cmdline

    def _new_job_id(self) -> str:
        for _ in range(10):
            candidate = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"
            if not (self.root / candidate).exists():
                return candidate
        raise JobStoreError("Could not allocate a unique job id")

    def _read_job(self, job_id: str) -> JobRecord | None:
        if not job_id:
            return None
        directory = self.root / job_id
        state_path = directory / "state.json"
        if not state_path.is_file():
            return None
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return JobRecord(job_id=job_id, directory=directory, state=state)

    def _read_state_or_raise(self, job_id: str) -> dict[str, Any]:
        job = self._read_job(job_id)
        if job is None:
            raise JobNotFound(f"Job not found: {job_id}")
        return copy.deepcopy(job.state)

    def _read_active(self) -> dict[str, Any] | None:
        path = self.root / ".active.json"
        if not path.is_file():
            return None
        try:
            active = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return active if isinstance(active, dict) and active.get("job_id") else None

    def _remove_active_locked(self) -> None:
        path = self.root / ".active.json"
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    @contextmanager
    def _global_lock(self) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / ".lock").open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def _job_lock(self, job_id: str) -> Iterator[None]:
        directory = self.root / job_id
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / "state.lock").open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, path)

    @staticmethod
    def _write_text(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
