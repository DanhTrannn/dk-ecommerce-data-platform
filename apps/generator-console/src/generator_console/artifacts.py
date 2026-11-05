from __future__ import annotations

import hashlib
import os
import zipfile
from pathlib import Path
from typing import Any

DEFAULT_MAX_ARCHIVE_BYTES = 200 * 1024 * 1024
LOG_ROOT_RELATIVE = Path("outputs") / "access-logs"
CHUNK_SIZE = 1024 * 1024
EXCLUDED_NAMES = {"state.json", "state.lock"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_files(job_dir: Path) -> list[Path]:
    result = []
    for path in sorted(job_dir.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(job_dir)
        if path.name in EXCLUDED_NAMES or path.name.endswith(".tmp"):
            continue
        result.append(path)
    return result


def write_checksums(job_dir: Path) -> Path:
    lines = []
    for path in artifact_files(job_dir):
        if path.name == "checksums.sha256":
            continue
        relative = path.relative_to(job_dir).as_posix()
        lines.append(f"{sha256_file(path)}  {relative}\n")
    target = job_dir / "checksums.sha256"
    temporary = target.with_suffix(".sha256.tmp")
    temporary.write_text("".join(lines), encoding="utf-8")
    os.replace(temporary, target)
    return target


def max_archive_bytes() -> int:
    return int(os.environ.get("GENERATOR_CONSOLE_MAX_ARCHIVE_BYTES", DEFAULT_MAX_ARCHIVE_BYTES))


def archive_logs(job_dir: Path, limit: int | None = None) -> Path | None:
    log_root = job_dir / LOG_ROOT_RELATIVE
    if not log_root.is_dir():
        return None
    files = sorted(path for path in log_root.rglob("*") if path.is_file())
    total_size = sum(path.stat().st_size for path in files)
    if total_size > (limit if limit is not None else max_archive_bytes()):
        return None
    target = job_dir / "outputs" / "access-logs.zip"
    temporary = target.with_suffix(".zip.tmp")
    base = log_root.parent
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
        for path in files:
            archive.write(path, arcname=path.relative_to(base).as_posix())
    os.replace(temporary, target)
    return target


def collect_artifacts(job_dir: Path) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for path in artifact_files(job_dir):
        relative = path.relative_to(job_dir)
        if relative.parts[:2] == ("outputs", "access-logs"):
            continue
        artifacts.append(
            {
                "path": relative.as_posix(),
                "kind": _kind_for(path),
                "size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    log_summary = _log_directory_summary(job_dir)
    if log_summary is not None:
        artifacts.append(log_summary)
    return sorted(artifacts, key=lambda item: item["path"])


def _log_directory_summary(job_dir: Path) -> dict[str, Any] | None:
    log_root = job_dir / LOG_ROOT_RELATIVE
    if not log_root.is_dir():
        return None
    files = sorted(path for path in log_root.rglob("*") if path.is_file())
    if not files:
        return None
    digest = hashlib.sha256()
    total_size = 0
    for path in files:
        file_digest = sha256_file(path)
        total_size += path.stat().st_size
        digest.update(f"{path.relative_to(log_root).as_posix()}:{file_digest}\n".encode("utf-8"))
    archive = job_dir / "outputs" / "access-logs.zip"
    return {
        "path": LOG_ROOT_RELATIVE.as_posix(),
        "kind": "logs_directory",
        "size": total_size,
        "sha256": digest.hexdigest(),
        "file_count": len(files),
        "archive": archive.relative_to(job_dir).as_posix() if archive.is_file() else None,
    }


def _kind_for(path: Path) -> str:
    relative = path.as_posix()
    if relative.endswith("config.yml"):
        return "config"
    if relative.endswith("request.json"):
        return "request"
    if relative.endswith(".manifest.json"):
        return "manifest"
    if relative.endswith(".sql"):
        return "sql"
    if relative.endswith(".zip"):
        return "logs_archive"
    if relative.endswith("checksums.sha256"):
        return "checksums"
    if relative.startswith("logs/") or "/logs/" in relative:
        return "log"
    return "artifact"
