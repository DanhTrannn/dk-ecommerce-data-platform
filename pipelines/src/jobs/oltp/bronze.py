"""Copy one source table to Bronze for one run: the cursor window, or every row created before the run start."""

from contextlib import closing
from datetime import UTC, datetime

from jobs.oltp.args import parse, run_parser
from lakehouse.oltp.bronze import ingest_table
from lakehouse.oltp.sources import TABLES_BY_NAME
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def parse_args(argv=None):
    parser = run_parser(__doc__)
    parser.add_argument("--table", choices=sorted(TABLES_BY_NAME), required=True)
    parser.add_argument("--page-size", type=int, default=10000)
    return parse(parser, argv)


def main(argv=None):
    args = parse_args(argv)
    full = args.mode == "full"
    with closing(postgres_reader()) as connection:
        ingest_table(
            iceberg_catalog(),
            connection,
            args.namespace_prefix,
            TABLES_BY_NAME[args.table],
            args.run_id,
            datetime.now(UTC),
            start=None if full else args.interval_start,
            end=None if full else args.interval_end,
            created_before=args.run_started_at if full else None,
            page_size=args.page_size,
        )


if __name__ == "__main__":
    main()
