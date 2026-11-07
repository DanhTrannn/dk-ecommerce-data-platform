from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from platform_app.ui.paths import profile_path


def load_profile(path: Path | None = None) -> dict[str, Any]:
    target = path or profile_path()
    payload = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Reference profile must contain a JSON object")
    return payload
