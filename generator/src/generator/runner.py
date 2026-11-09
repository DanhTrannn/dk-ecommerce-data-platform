import json
from datetime import UTC, datetime
from pathlib import Path

from generator import __version__
from generator.config import GeneratorConfig
from generator.postgres_sql import SQL_FORMAT

SUPPORTED_MODES = {
    "seed_master",
    "historical_transactions",
    "failure_fixtures",
    "repurchase_history",
}


def run(config: GeneratorConfig, output_directory: Path) -> Path:
    unsupported = set(config.modes) - SUPPORTED_MODES
    if unsupported:
        raise ValueError(f"unsupported generator modes: {sorted(unsupported)}")
    output_directory.mkdir(parents=True, exist_ok=True)
    generation_run_id = config.generation_run_id
    manifest = {
        "generation_run_id": generation_run_id,
        "identifier_strategy": "uuid5-deterministic-v1",
        "scenario_id": config.scenario_id,
        "dataset_size": config.dataset_size,
        "logical_identity": config.logical_identity,
        "seed": config.seed,
        "anchor_time": config.anchor_time.isoformat(),
        "history_months": config.history_months,
        "modes": list(config.modes),
        "scale": config.scale,
        "locations": {"store_codes": list(config.locations.store_codes)},
        "location_counts": config.locations.counts,
        "generator_version": __version__,
        "sql_format": SQL_FORMAT,
        "data_origin": "synthetic",
        "generated_at": datetime.now(UTC).isoformat(),
        "implementation_status": "sql_export_available",
        "empirical_rules": ["hm_2019_product_popularity"],
        "assumptions_document": "docs/data.md",
    }
    path = output_directory / f"{config.scenario_id}-{config.logical_identity}.manifest.json"
    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary_path.replace(path)
    return path
