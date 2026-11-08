from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

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
        if path.relative_to(job_dir).parts[0] == "imports":
            continue
        if not path.is_file() or path.is_symlink():
            continue
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


def collect_artifacts(job_dir: Path) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for path in artifact_files(job_dir):
        relative = path.relative_to(job_dir)
        artifacts.append(
            {
                "path": relative.as_posix(),
                "kind": _kind_for(path),
                "size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return sorted(artifacts, key=lambda item: item["path"])


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
    if relative.endswith("checksums.sha256"):
        return "checksums"
    if relative.startswith("logs/") or "/logs/" in relative:
        return "log"
    return "artifact"
