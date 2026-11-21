"""Create the OLTP namespaces and Bronze/Silver tables of one namespace prefix, before the first load."""

import argparse
from contextlib import closing

from lakehouse.oltp.schema import create_oltp_schema
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", required=True)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    with closing(postgres_reader()) as connection:
        create_oltp_schema(iceberg_catalog(), connection, args.namespace_prefix)


if __name__ == "__main__":
    main()
