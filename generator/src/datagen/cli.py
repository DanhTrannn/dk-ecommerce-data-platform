"""Command line: datagen generate --preset <name>."""

import argparse
import os
import sys
import time

from datagen.oltp.run import AlreadyGenerated, BatchFailed, generate
from datagen.postgres import SchemaMismatch, connect
from datagen.presets import load_oltp_preset, preset_path

DATABASE_URL_VARIABLE = "GENERATOR_DATABASE_URL"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datagen", description="Synthetic OLTP history for the D&K database")
    commands = parser.add_subparsers(dest="command", required=True)
    generate_parser = commands.add_parser("generate", help="load 12 months of synthetic history into an empty database")
    generate_parser.add_argument("--preset", required=True, help="preset name, e.g. 1m, 10m, 50m")
    return parser


def _fail(message: str, code: int) -> int:
    print(message, file=sys.stderr)
    return code


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    url = os.environ.get(DATABASE_URL_VARIABLE)
    if not url:
        return _fail(f"Thiếu biến môi trường {DATABASE_URL_VARIABLE}", 2)
    path = preset_path(args.preset)
    if not path.exists():
        return _fail(f"Không có preset {args.preset} ({path})", 2)
    preset = load_oltp_preset(path)
    connection = connect(url)
    try:
        started = time.monotonic()
        counts = generate(preset, connection)
    except (AlreadyGenerated, BatchFailed, SchemaMismatch) as error:
        return _fail(str(error), 1)
    finally:
        connection.close()
    for table, rows in sorted(counts.items()):
        print(f"{table:<24}{rows:>14,}")
    print(f"Xong sau {time.monotonic() - started:.0f} giây")
    return 0


if __name__ == "__main__":
    sys.exit(main())
