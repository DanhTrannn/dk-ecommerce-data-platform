from __future__ import annotations

import os
import sys
from pathlib import Path


def repo_root() -> Path:
    override = os.environ.get("GENERATOR_CONSOLE_REPO_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[5]


def configs_dir() -> Path:
    override = os.environ.get("GENERATOR_CONFIGS_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return repo_root() / "generator" / "configs"


def profile_path() -> Path:
    override = os.environ.get("GENERATOR_PROFILE_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return repo_root() / "generator" / "profiles" / "uci-online-retail-uk-v1.json"


def jobs_root() -> Path:
    override = os.environ.get("GENERATOR_CONSOLE_DATA_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return repo_root() / "data" / "generator" / "jobs"


def generator_cli_command() -> list[str]:
    return [sys.executable, "-m", "generator.cli"]
