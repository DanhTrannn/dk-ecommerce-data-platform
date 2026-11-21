"""Merge one table's Bronze rows of a run into Silver: replace in a full run, upsert in an incremental one."""

from jobs.oltp.args import parse, run_parser
from lakehouse.oltp.silver import merge_silver
from lakehouse.oltp.sources import TABLES_BY_NAME
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.namespaces import namespace as ns


def parse_args(argv=None):
    parser = run_parser(__doc__)
    parser.add_argument("--table", choices=sorted(TABLES_BY_NAME), required=True)
    return parse(parser, argv)


def main(argv=None):
    args = parse_args(argv)
    catalog = iceberg_catalog()
    catalog.create_namespace_if_not_exists(ns(args.namespace_prefix, "silver"))
    merge_silver(catalog, args.namespace_prefix, TABLES_BY_NAME[args.table], args.run_id, full=args.mode == "full")


if __name__ == "__main__":
    main()
