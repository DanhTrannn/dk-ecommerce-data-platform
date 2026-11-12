"""Local durable scenario runs and cooperative stop requests."""

import fcntl
import json
import os
import re
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import Thread

from platform_app.ui.paths import repo_root
from platform_app.ui.scenarios.config import PurchaseConfig, ScenarioSettings

ACTIVE = {"queued", "running"}


class ScenarioStoreError(Exception):
    """Invalid scenario state or a run that cannot be started/updated."""


def stamp() -> str:
    return datetime.now(UTC).isoformat()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _launch(root, run_id):
    process = subprocess.Popen(
        [sys.executable, "-m", "platform_app.ui.scenarios.worker", str(root), run_id],
        cwd=repo_root(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    Thread(target=process.wait, daemon=True, name=f"scenario-{run_id[:8]}").start()
    return process.pid


def _alive(pid, run_id):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        command = Path(f"/proc/{pid}/cmdline")
        return not command.exists() or (
            b"platform_app.ui.scenarios.worker" in command.read_bytes() and run_id.encode() in command.read_bytes()
        )
    except PermissionError:
        return True
    except OSError:
        return False


class ScenarioStore:
    def __init__(self, root: Path, launcher=None, alive=None):
        self.root = Path(root).absolute()
        self.launcher = launcher or _launch
        self.alive = alive or _alive

    def path(self, run_id):
        if not re.fullmatch(r"[a-f0-9]{32}", run_id):
            raise ScenarioStoreError("Invalid scenario run ID")
        path = self.root / f"{run_id}.json"
        if path.is_symlink():
            raise ScenarioStoreError("Invalid scenario record")
        return path

    @contextmanager
    def lock(self, execution=False):
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / (".execution.lock" if execution else ".state.lock")).open("a+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | (fcntl.LOCK_NB if execution else 0))
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def read(self, run_id):
        record = json.loads(self.path(run_id).read_text(encoding="utf-8"))
        if record["run_id"] != run_id:
            raise ScenarioStoreError("Scenario identity mismatch")
        return record

    def write(self, record):
        _write_json(self.path(record["run_id"]), record)

    def history(self):
        return sorted(
            (self.read(path.stem) for path in self.root.glob("*.json")),
            key=lambda row: row["created_at"],
            reverse=True,
        )

    def reconcile(self):
        with self.lock():
            for record in self.history():
                if record["status"] in ACTIVE and not self.alive(record["pid"], record["run_id"]):
                    record.update(
                        status="unknown",
                        finished_at=stamp(),
                        message="Worker đã dừng; kiểm tra các lượt chưa xác minh trước khi chạy lại.",
                    )
                    if record["attempts"] and record["attempts"][-1]["status"] == "running":
                        record["attempts"][-1]["status"] = "unknown"
                    self.write(record)

    def start(self, config: PurchaseConfig, settings: ScenarioSettings):
        self.reconcile()
        with self.lock():
            if any(row["status"] in ACTIVE for row in self.history()):
                raise ScenarioStoreError("Đang có kịch bản chạy; chờ hoàn tất hoặc yêu cầu dừng.")
            record = dict(
                run_id=uuid.uuid4().hex,
                config=config.model_dump(),
                target=settings.api_base_url,
                timeout=settings.http_timeout_seconds,
                status="queued",
                pid=None,
                stop_requested=False,
                attempts=[],
                created_at=stamp(),
                started_at=None,
                finished_at=None,
                message=None,
            )
            self.write(record)
            try:
                record["pid"] = int(self.launcher(self.root, record["run_id"]))
                if record["pid"] <= 0:
                    raise ValueError("Invalid worker PID")
            except Exception:
                record.update(status="failed", finished_at=stamp(), message="Không khởi động được worker.")
                self.write(record)
                raise ScenarioStoreError(record["message"]) from None
            self.write(record)
            return record

    def claim(self, run_id):
        with self.lock():
            record = self.read(run_id)
            if record["status"] != "queued" or record["pid"] != os.getpid():
                raise ScenarioStoreError("Scenario is not assigned to this worker")
            record.update(status="running", started_at=stamp())
            self.write(record)
            return record

    def update(self, run_id, change):
        with self.lock():
            record = self.read(run_id)
            if record["status"] != "running" or record["pid"] != os.getpid():
                raise ScenarioStoreError("Scenario is no longer owned by this worker")
            change(record)
            self.write(record)
            return record

    def stop(self, run_id):
        with self.lock():
            record = self.read(run_id)
            if record["status"] in ACTIVE:
                record["stop_requested"] = True
                self.write(record)
