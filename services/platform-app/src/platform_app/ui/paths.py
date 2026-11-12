from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    override = os.environ.get("GENERATOR_CONSOLE_REPO_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[5]


def jobs_root() -> Path:
    override = os.environ.get("GENERATOR_CONSOLE_DATA_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return repo_root() / "data" / "generator" / "jobs"
