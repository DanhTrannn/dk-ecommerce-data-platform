import argparse
import json
from pathlib import Path
from typing import Any

from generator import __version__
from generator.config import load_config
from generator.reference_profile import write_reference_profile
from generator.runner import run
from generator.sql_export import export_sql


def _emit_summary(as_json: bool, payload: dict[str, Any], human_lines: list[str]) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    for line in human_lines:
        print(line)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="generator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    reference_parser = subparsers.add_parser("profile-reference", help="Measure a pinned UCI source without changing the sampler")
    reference_parser.add_argument("--input", type=Path, required=True)
    reference_parser.add_argument("--spec", type=Path, required=True)
    reference_parser.add_argument("--output", type=Path, required=True)

    run_parser = subparsers.add_parser("run", help="Write the scenario manifest")
    run_parser.add_argument("--config", type=Path, required=True)
    run_parser.add_argument("--output", type=Path, default=Path("/data/generator"))
    run_parser.add_argument("--json", action="store_true", help="Print a machine-readable summary")

    sql_parser = subparsers.add_parser("export-sql", help="Generate an importable MySQL dataset")
    sql_parser.add_argument("--config", type=Path, required=True)
    sql_parser.add_argument("--output", type=Path)
    sql_parser.add_argument("--json", action="store_true", help="Print a machine-readable summary")

    return parser


def main() -> None:
    parser = build_parser()
    arguments = parser.parse_args()
    if arguments.command == "profile-reference":
        try:
            profile = write_reference_profile(arguments.input, arguments.spec, arguments.output)
        except (ValueError, OSError) as error:
            parser.error(str(error))
        print(f"Reference profile: {arguments.output}")
        print(f"Profile ID: {profile['profile_id']}")
        print(f"Rows: {profile['audit']['raw_rows']} raw / {profile['audit']['kept_rows']} kept")
        return
    config = load_config(arguments.config)

    if arguments.command == "run":
        output_path = run(config, arguments.output)
        _emit_summary(
            arguments.json,
            {
                "manifest_path": str(output_path),
                "generation_run_id": config.generation_run_id,
                "logical_identity": config.logical_identity,
                "generator_version": __version__,
                "scenario_id": config.scenario_id,
                "scale": config.scale,
                "modes": list(config.modes),
            },
            [str(output_path)],
        )
        return

    output_path = arguments.output or Path("/data/generator") / f"{config.scenario_id}-{config.logical_identity}.sql"
    summary = export_sql(config, output_path)
    _emit_summary(
        arguments.json,
        {
            "sql_path": str(summary.sql_path),
            "generation_run_id": summary.generation_run_id,
            "logical_identity": config.logical_identity,
            "customers": summary.customers,
            "products": summary.products,
            "variants": summary.variants,
            "orders": summary.orders,
        },
        [
            f"SQL file: {summary.sql_path}",
            f"Generation run: {summary.generation_run_id}",
            (
                f"Rows: {summary.customers} customers, {summary.products} products, "
                f"{summary.variants} variants, {summary.orders} orders"
            ),
        ],
    )


if __name__ == "__main__":
    main()
