from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from generator.config import GeneratorConfig, config_from_dict

SCENARIO_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
OUTPUT_MODES = ("sql", "logs", "both")


@dataclass(frozen=True)
class FormValues:
    scenario_id: str
    seed: int
    anchor_time: str
    history_months: int
    customers: int
    products: int
    variants: int
    orders: int
    output_mode: str
    expected_requests: int
    distributions_yaml: str
    store_codes: tuple[str, ...] | None = None


def list_presets(configs_directory: Path | None = None) -> list[Path]:
    directory = configs_directory or _configs_dir()
    return sorted(path for path in directory.glob("*.yml") if path.is_file())


def load_preset(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"Preset {path.name} must contain a YAML mapping")
    return document


def build_config_document(preset: dict[str, Any], values: FormValues) -> dict[str, Any]:
    document = copy.deepcopy(preset)
    document["scenario_id"] = values.scenario_id.strip()
    document["seed"] = int(values.seed)
    document["anchor_time"] = values.anchor_time.strip()
    document["history_months"] = int(values.history_months)
    document["scale"] = {
        "customers": int(values.customers),
        "products": int(values.products),
        "variants": int(values.variants),
        "orders": int(values.orders),
    }
    if values.store_codes is not None:
        document["locations"] = {"store_codes": list(values.store_codes)}
    if values.distributions_yaml.strip():
        distributions = yaml.safe_load(values.distributions_yaml)
        if not isinstance(distributions, dict):
            raise ValueError("Distributions YAML must contain a mapping")
        document["distributions"] = distributions
    return document


def validate_config_document(document: dict[str, Any], values: FormValues) -> GeneratorConfig:
    scenario_id = str(document.get("scenario_id", "")).strip()
    if not SCENARIO_ID_PATTERN.fullmatch(scenario_id):
        raise ValueError(
            "scenario_id must start with a letter/digit and contain only letters, digits, '.', '_' or '-'"
        )
    if values.output_mode not in OUTPUT_MODES:
        raise ValueError(f"output_mode must be one of {OUTPUT_MODES}")
    if int(document.get("history_months", 0)) < 1:
        raise ValueError("history_months must be positive")
    config = config_from_dict(document)
    if values.output_mode in ("logs", "both"):
        expected = resolve_expected_requests(document, values)
        if expected < 1:
            raise ValueError("expected_requests must be positive")
    return config


def resolve_expected_requests(document: dict[str, Any], values: FormValues) -> int:
    if int(values.expected_requests) > 0:
        return int(values.expected_requests)
    orders = int(document["scale"]["orders"])
    return max(1_000, orders * 20)


def freeze_config(document: dict[str, Any]) -> str:
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)


def build_request(preset_name: str, values: FormValues, expected_requests: int) -> dict[str, Any]:
    return {
        "preset": preset_name,
        "output": values.output_mode,
        "expected_requests": int(expected_requests),
    }


def _configs_dir() -> Path:
    from platform_app.ui.paths import configs_dir

    return configs_dir()
