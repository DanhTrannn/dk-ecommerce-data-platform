from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

from platform_app.ui.artifacts import archive_logs, collect_artifacts, write_checksums
from platform_app.ui.job_store import (
    STAGE_FAILED,
    STAGE_SKIPPED,
    STAGE_SUCCEEDED,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_SUCCEEDED,
    JobStore,
)
from platform_app.ui.paths import generator_cli_command, repo_root

CommandRunner = Callable[[list[str], Path], subprocess.CompletedProcess[str]]


def _run_command(command: list[str], log_path: Path) -> subprocess.CompletedProcess[str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"$ {shlex.join(command)}\n")
        log.flush()
        completed = subprocess.run(
            command,
            cwd=str(repo_root()),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.stdout:
            log.write(completed.stdout)
            if not completed.stdout.endswith("\n"):
                log.write("\n")
        if completed.stderr:
            log.write("\n[stderr]\n")
            log.write(completed.stderr)
            if not completed.stderr.endswith("\n"):
                log.write("\n")
        log.write(f"\n[exit] {completed.returncode}\n")
    return completed


def _parse_json(stdout: str) -> dict[str, Any]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"Generator did not return JSON: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError("Generator JSON summary must be an object")
    return payload


def _failure_message(completed: subprocess.CompletedProcess[str]) -> str:
    output = (completed.stderr or completed.stdout or "").strip()
    if not output:
        return f"Command exited with code {completed.returncode}"
    return output[-2000:]


def _run_stage(
    store: JobStore,
    job_id: str,
    stage: str,
    command: list[str],
    runner: CommandRunner,
) -> tuple[bool, dict[str, Any] | None]:
    store.mark_stage_running(job_id, stage, shlex.join(command))
    completed = runner(command, store.root / job_id / "logs" / f"{stage}.log")
    if completed.returncode != 0:
        store.complete_stage(
            job_id,
            stage,
            STAGE_FAILED,
            exit_code=completed.returncode,
            message=_failure_message(completed),
        )
        return False, None
    try:
        payload = _parse_json(completed.stdout)
    except ValueError as error:
        store.complete_stage(
            job_id,
            stage,
            STAGE_FAILED,
            exit_code=completed.returncode,
            message=str(error),
        )
        return False, None
    store.complete_stage(
        job_id,
        stage,
        STAGE_SUCCEEDED,
        exit_code=completed.returncode,
        summary=payload,
    )
    return True, payload


def _validation_command(config_path: Path, job_dir: Path) -> list[str]:
    return generator_cli_command() + [
        "run",
        "--config",
        str(config_path),
        "--output",
        str(job_dir),
        "--json",
    ]


def _sql_command(config_path: Path, sql_path: Path) -> list[str]:
    return generator_cli_command() + [
        "export-sql",
        "--config",
        str(config_path),
        "--output",
        str(sql_path),
        "--json",
    ]


def _logs_command(config_path: Path, log_root: Path, expected_requests: int) -> list[str]:
    command = generator_cli_command() + [
        "export-logs",
        "--config",
        str(config_path),
        "--output-directory",
        str(log_root),
        "--json",
    ]
    if expected_requests > 0:
        command.extend(["--expected-requests", str(expected_requests)])
    return command


def _overall_status(store: JobStore, job_id: str) -> tuple[str, str | None]:
    state = store.get_job(job_id).state
    selected = [
        stage
        for stage in ("sql", "logs")
        if state["stages"][stage]["status"] != STAGE_SKIPPED
    ]
    succeeded = [stage for stage in selected if state["stages"][stage]["status"] == STAGE_SUCCEEDED]
    failed = [stage for stage in selected if state["stages"][stage]["status"] != STAGE_SUCCEEDED]
    if not selected:
        return STATUS_FAILED, "Validation failed"
    if not failed:
        return STATUS_SUCCEEDED, None
    messages = []
    for stage in failed:
        message = state["stages"][stage].get("message") or state["stages"][stage]["status"]
        messages.append(f"{stage}: {message}")
    error = "; ".join(messages)
    if succeeded:
        return STATUS_PARTIAL, error
    return STATUS_FAILED, error


def run_job(
    job_dir: Path,
    command_runner: CommandRunner | None = None,
) -> int:
    job_dir = Path(job_dir)
    store = JobStore(job_dir.parent)
    job = store.get_job(job_dir.name)
    job_id = job.job_id
    runner = command_runner or _run_command
    config_path = job_dir / "config.yml"
    try:
        store.mark_started(job_id, os.getpid())
        validation_ok, validation = _run_stage(
            store,
            job_id,
            "validate",
            _validation_command(config_path, job_dir),
            runner,
        )
        if validation_ok and validation:
            def update_identity(state: dict[str, Any]) -> None:
                if validation.get("generator_version"):
                    state["generator_version"] = validation["generator_version"]
                if validation.get("logical_identity"):
                    state["dataset_identity"] = validation["logical_identity"]

            store.update_state(job_id, update_identity)
            manifest_path = Path(str(validation.get("manifest_path", "")))
            if not manifest_path.is_file():
                store.complete_stage(
                    job_id,
                    "validate",
                    STAGE_FAILED,
                    exit_code=0,
                    message=f"Manifest was not written: {manifest_path}",
                )
                validation_ok = False

        if validation_ok:
            state = store.get_job(job_id).state
            scenario = str(state["scenario_id"])
            identity = str(state["dataset_identity"])
            sql_path = job_dir / "outputs" / "sql" / f"{scenario}-{identity}.sql"
            if state["stages"]["sql"]["status"] != STAGE_SKIPPED:
                sql_path.parent.mkdir(parents=True, exist_ok=True)
                sql_ok, _ = _run_stage(
                    store,
                    job_id,
                    "sql",
                    _sql_command(config_path, sql_path),
                    runner,
                )
                if sql_ok and not sql_path.is_file():
                    store.complete_stage(
                        job_id,
                        "sql",
                        STAGE_FAILED,
                        exit_code=0,
                        message=f"SQL file was not written: {sql_path}",
                    )
            log_root = job_dir / "outputs" / "access-logs"
            if state["stages"]["logs"]["status"] != STAGE_SKIPPED:
                _run_stage(
                    store,
                    job_id,
                    "logs",
                    _logs_command(config_path, log_root, int(state["expected_requests"])),
                    runner,
                )
        else:
            for stage in ("sql", "logs"):
                if store.get_job(job_id).state["stages"][stage]["status"] != STAGE_SKIPPED:
                    store.complete_stage(
                        job_id,
                        stage,
                        STAGE_SKIPPED,
                        message="Skipped because validation failed",
                    )

        archive_logs(job_dir)
        write_checksums(job_dir)
        store.set_artifacts(job_id, collect_artifacts(job_dir))
        status, error = _overall_status(store, job_id)
        store.finish_job(job_id, status, error)
        return 0 if status == STATUS_SUCCEEDED else 1
    except Exception as error:
        store.finish_job(job_id, STATUS_FAILED, f"Worker error: {error}")
        traceback.print_exc()
        return 1
    finally:
        store.release_active(job_id)


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python -m platform_app.ui.worker <job_dir>", file=sys.stderr)
        raise SystemExit(2)
    try:
        exit_code = run_job(Path(sys.argv[1]))
    except Exception:
        traceback.print_exc()
        exit_code = 1
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
