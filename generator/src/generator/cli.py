"""CLI used directly and by the existing Streamlit job worker."""

import argparse
import json
from pathlib import Path

import yaml

from generator import __version__
from generator.config import load_config
from generator.postgres_sql import SQL_FORMAT
from generator.runner import run
from generator.sql_export import export_sql


def main() -> None:
    parser = argparse.ArgumentParser(prog="generator")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("run", "Validate a scenario and write its manifest"),
        ("export-sql", "Generate data for the current PostgreSQL schema"),
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
            path = run(config, directory)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["manifest_path"] = str(path)
        else:
            path = args.output or Path("data/generator") / f"{config.scenario_id}-{config.logical_identity}.sql"
            summary = export_sql(config, path)
            payload = {
                "sql_path": str(summary.sql_path),
                "sql_format": SQL_FORMAT,
                "generator_version": __version__,
                "cities": summary.cities,
                "stores": summary.stores,
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
