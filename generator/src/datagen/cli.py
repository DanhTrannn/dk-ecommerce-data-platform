"""Command line: datagen generate|verify --preset <name>, and datagen reset --yes."""

import argparse
import os
import sys
import time

from datagen.oltp.run import RESET_TABLES, AlreadyGenerated, BatchFailed, ResetRefused, generate, reset
from datagen.postgres import SchemaMismatch, connect
from datagen.presets import OltpPreset, load_oltp_preset, preset_path
from datagen.verify import ledger, rules
from datagen.verify.report import render

DATABASE_URL_VARIABLE = "GENERATOR_DATABASE_URL"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datagen", description="Synthetic OLTP history for the D&K database")
    commands = parser.add_subparsers(dest="command", required=True)
    generate_parser = commands.add_parser("generate", help="load 12 months of synthetic history into an empty database")
    verify_parser = commands.add_parser("verify", help="check the generated data against the preset (read only)")
    for command in (generate_parser, verify_parser):
        command.add_argument("--preset", required=True, help="preset name, e.g. 1m, 10m, 50m")
    reset_parser = commands.add_parser("reset", help="empty every table of the D&K database")
    reset_parser.add_argument("--yes", action="store_true", help="confirm that all rows are deleted")
    return parser


def _fail(message: str, code: int) -> int:
    print(message, file=sys.stderr)
    return code


def _generate(preset: OltpPreset, connection) -> int:
    started = time.monotonic()
    try:
        counts = generate(preset, connection)
    except (AlreadyGenerated, BatchFailed, SchemaMismatch) as error:
        return _fail(str(error), 1)
    for table, rows in sorted(counts.items()):
        print(f"{table:<24}{rows:>14,}")
    print(f"Xong sau {time.monotonic() - started:.0f} giây")
    return 0


def _verify(preset: OltpPreset, connection) -> int:
    checks = ledger.checks(connection) + rules.checks(connection, preset)
    print(render(checks))
    return 0 if all(c.passed for c in checks) else 1


def _reset(connection) -> int:
    try:
        reset(connection)
    except ResetRefused as error:
        return _fail(str(error), 1)
    print(f"Đã xóa trắng {len(RESET_TABLES)} bảng")
    return 0


PRESET_COMMANDS = {"generate": _generate, "verify": _verify}


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    is_reset = args.command == "reset"
    if is_reset and not args.yes:
        return _fail(f"Thêm --yes để xác nhận xóa trắng {len(RESET_TABLES)} bảng", 2)
    url = os.environ.get(DATABASE_URL_VARIABLE)
    if not url:
        return _fail(f"Thiếu biến môi trường {DATABASE_URL_VARIABLE}", 2)
    if not is_reset:
        path = preset_path(args.preset)
        if not path.exists():
            return _fail(f"Không có preset {args.preset} ({path})", 2)
        preset = load_oltp_preset(path)
    connection = connect(url)
    try:
        if is_reset:
            return _reset(connection)
        return PRESET_COMMANDS[args.command](preset, connection)
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
