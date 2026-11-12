"""Apply the bounded CDC cart/wishlist hard-delete policy using PyIceberg."""

import argparse
import json
from contextlib import closing
from datetime import datetime

from lakehouse.oltp.hard_delete import sync_hard_deletes
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-prefix", default="serving")
    parser.add_argument("--as-of", required=True)
    args = parser.parse_args()
    with closing(postgres_reader()) as connection:
        result = sync_hard_deletes(
            iceberg_catalog(),
            connection,
            datetime.fromisoformat(args.as_of.replace("Z", "+00:00")),
            args.namespace_prefix,
        )
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
