from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    override = os.environ.get("PLATFORM_APP_REPO_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[5]


def scenarios_root() -> Path:
    override = os.environ.get("PLATFORM_APP_SCENARIOS_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return repo_root() / "data" / "scenarios"
