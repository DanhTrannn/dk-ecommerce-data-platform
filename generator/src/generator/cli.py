"""CLI used directly and by the existing Streamlit job worker."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from generator import __version__
from generator.config import load_config
from generator.sql_export import export_sql


def main() -> None:
    parser = argparse.ArgumentParser(prog="generator")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("run", "Validate a scenario and write its manifest"),
        ("export-sql", "Generate data for the current MySQL schema"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--output", type=Path)
        command.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        config = load_config(args.config)
        if args.command == "run":
            directory = args.output or Path("data/generator")
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{config.scenario_id}-{config.logical_identity}.manifest.json"
            payload = {
                "manifest_path": str(path),
                "generation_run_id": config.generation_run_id,
                "logical_identity": config.logical_identity,
                "generator_version": __version__,
                "scenario_id": config.scenario_id,
                "scale": config.scale,
                "modes": list(config.modes),
                "seed": config.seed,
                "anchor_time": config.anchor_time.isoformat(),
                "history_months": config.history_months,
                "data_origin": "synthetic",
                "generated_at": datetime.now(UTC).isoformat(),
                "implementation_status": "sql_export_available",
                "empirical_rules": ["hm_2019_product_popularity"],
                "assumptions_document": "docs/project/GENERATOR_METRICS.md",
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        else:
            path = args.output or Path("data/generator") / f"{config.scenario_id}-{config.logical_identity}.sql"
            summary = export_sql(config, path)
            payload = {
                "sql_path": str(summary.sql_path),
                "generation_run_id": summary.generation_run_id,
                "logical_identity": config.logical_identity,
                "customers": summary.customers,
                "products": summary.products,
                "variants": summary.variants,
                "orders": summary.orders,
            }
    except (OSError, ValueError, TypeError, yaml.YAMLError) as error:
        parser.error(str(error))
    print(json.dumps(payload, ensure_ascii=False, indent=2) if args.json else str(path))


if __name__ == "__main__":
    main()
