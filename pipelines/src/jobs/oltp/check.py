"""Pre-merge checks of one table's Bronze rows, or of the foreign keys across tables; a failure stops the run."""

from contextlib import closing

from jobs.oltp.args import parse, run_parser
from lakehouse.oltp.checks import check_foreign_keys, check_table, source_reconciliation
from lakehouse.oltp.sources import TABLES_BY_NAME
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader, source_read_transaction


def parse_args(argv=None):
    parser = run_parser(__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--table", choices=sorted(TABLES_BY_NAME))
    target.add_argument("--foreign-keys", action="store_true")
    return parse(parser, argv)


def main(argv=None):
    args = parse_args(argv)
    full = args.mode == "full"
    if args.foreign_keys:
        if not full:
            print("CHECKS foreign_keys skipped", flush=True)
            return
        check_foreign_keys(iceberg_catalog(), args.namespace_prefix, args.run_id)
        return
    table, source = TABLES_BY_NAME[args.table], None
    if full:
        with closing(postgres_reader()) as connection, source_read_transaction(connection):
            source = source_reconciliation(connection, table, args.run_started_at)
    check_table(iceberg_catalog(), args.namespace_prefix, table, args.run_id, source)


if __name__ == "__main__":
    main()
