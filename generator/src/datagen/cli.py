"""Command line: datagen generate|verify --preset <name>, datagen logs --preset <name> --out <dir>, datagen reset --yes."""

import argparse
import os
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from datagen.oltp.run import (
    RESET_TABLES,
    AlreadyGenerated,
    LoadFailed,
    ManualOrdersPresent,
    ResetRefused,
    generate,
    reset,
)
from datagen.logs.inputs import EmptyWindow, OltpMismatch, read_inputs
from datagen.logs.run import generate_logs
from datagen.postgres import SchemaMismatch, connect
from datagen.presets import LogPreset, OltpPreset, load_log_preset, load_oltp_preset, log_preset_path, preset_path
from datagen.verify import ledger, rules
from datagen.verify.report import render

DATABASE_URL_VARIABLE = "GENERATOR_DATABASE_URL"
# A rolled-back load leaves no visible row, but its rows still take disk space until the tables are emptied.
RECLAIM_HINT = (
    "Mọi dòng đã được hoàn tác nhưng vẫn chiếm đĩa; trước khi chạy lại: 'datagen reset --yes' rồi 'make seed'"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datagen", description="Synthetic OLTP history for the D&K database")
    commands = parser.add_subparsers(dest="command", required=True)
    generate_parser = commands.add_parser("generate", help="load 12 months of synthetic history into an empty database")
    verify_parser = commands.add_parser("verify", help="check the generated data against the preset (read only)")
    for command in (generate_parser, verify_parser):
        command.add_argument("--preset", required=True, help="preset name, e.g. 1m, 10m, 50m")
    logs_parser = commands.add_parser(
        "logs", help="write the synthetic access log of the preset window as Parquet files"
    )
    logs_parser.add_argument("--preset", required=True, help="preset name, e.g. 1m, 10m, 50m")
    logs_parser.add_argument("--out", required=True, type=Path, help="new or empty directory for the daily files")
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
    except LoadFailed as error:
        return _fail(f"{error}\n{RECLAIM_HINT}", 1)
    except (AlreadyGenerated, ManualOrdersPresent, SchemaMismatch) as error:
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


def _log_preset_problem(log: LogPreset, oltp: OltpPreset, out: Path) -> str | None:
    if log.oltp_scenario_id != oltp.scenario_id:
        return f"Preset log đi cùng {log.oltp_scenario_id}, không phải {oltp.scenario_id}"
    zone = ZoneInfo(oltp.business_timezone)
    first, end = oltp.history_start.astimezone(zone).date(), oltp.anchor_time.astimezone(zone).date()
    if not (first <= log.window.start and log.window.end <= end):
        return f"Cửa sổ log {log.window.start} → {log.window.end} nằm ngoài lịch sử OLTP {first} → {end}"
    if out.exists() and any(out.iterdir()):
        return f"Thư mục {out} đã có file; chọn thư mục mới hoặc rỗng"
    return None


def _logs(log: LogPreset, oltp: OltpPreset, out: Path, connection) -> int:
    started = time.monotonic()
    try:
        inputs = read_inputs(connection, oltp, log.window)
    except (OltpMismatch, EmptyWindow) as error:
        return _fail(str(error), 1)
    days = generate_logs(log, oltp, inputs, out)
    print(f"{'ngày':<12}{'phiên mua':>12}{'phiên không mua':>18}{'request':>14}")
    for day, counts in days.items():
        print(f"{day:<12}{counts['buying_sessions']:>12,}{counts['non_buying_sessions']:>18,}{counts['requests']:>14,}")
    total = sum(counts["requests"] for counts in days.values())
    print(f"{len(days)} file, {total:,} request trong {out}; xong sau {time.monotonic() - started:.0f} giây")
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
    if args.command == "logs":
        log = load_log_preset(log_preset_path(args.preset))
        if problem := _log_preset_problem(log, preset, args.out):
            return _fail(problem, 1)
    connection = connect(url)
    try:
        if is_reset:
            return _reset(connection)
        if args.command == "logs":
            return _logs(log, preset, args.out, connection)
        return PRESET_COMMANDS[args.command](preset, connection)
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
