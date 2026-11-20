"""Python Iceberg catalog access using the existing workload credential files."""

import os
import shlex
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError


def catalog_properties():
    required = (
        "POLARIS_CATALOG_URI",
        "POLARIS_CATALOG_NAME",
        "MINIO_ENDPOINT",
        "MINIO_BATCH_ACCESS_KEY",
        "MINIO_BATCH_SECRET_KEY",
    )
    if any(not os.environ.get(key, "").strip() for key in required):
        raise ValueError("Configure the Polaris catalog and MinIO batch workload credentials")
    path = Path(os.environ.get("POLARIS_CREDENTIAL_FILE", "/run/polaris/clients.env"))
    values = {}
    for line in path.read_text().splitlines():
        fields = shlex.split(line, comments=True)
        if fields and fields[0] == "export":
            fields = fields[1:]
        if not fields:
            continue
        if len(fields) != 1 or "=" not in fields[0]:
            raise ValueError("Polaris credential file is malformed")
        key, value = fields[0].split("=", 1)
        values[key] = value
    if not all(values.get(key) for key in ("POLARIS_BATCH_CLIENT_ID", "POLARIS_BATCH_CLIENT_SECRET")):
        raise ValueError("Polaris credential file is missing the batch principal")
    uri = os.environ["POLARIS_CATALOG_URI"].rstrip("/")
    return {
        "type": "rest",
        "uri": uri,
        "warehouse": os.environ["POLARIS_CATALOG_NAME"],
        "credential": values["POLARIS_BATCH_CLIENT_ID"] + ":" + values["POLARIS_BATCH_CLIENT_SECRET"],
        "oauth2-server-uri": uri + "/v1/oauth/tokens",
        "scope": "PRINCIPAL_ROLE:ALL",
        "header.Polaris-Realm": os.environ.get("POLARIS_REALM", "POLARIS"),
        "header.X-Iceberg-Access-Delegation": "",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": os.environ["MINIO_ENDPOINT"],
        "s3.access-key-id": os.environ["MINIO_BATCH_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["MINIO_BATCH_SECRET_KEY"],
        "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        "s3.force-virtual-addressing": "false",
    }


def iceberg_catalog():
    return load_catalog(os.environ.get("POLARIS_CATALOG_NAME", "lakehouse"), **catalog_properties())


def load_or_create(catalog, key, **settings):
    try:
        return catalog.load_table(key)
    except NoSuchTableError:
        return catalog.create_table_if_not_exists(key, **settings)


def evolve_table_schema(table, schema, *, drop=()):
    added = set(schema.names) - set(table.schema().column_names)
    current = table.current_snapshot()
    stale = current is not None and current.schema_id != table.schema().schema_id
    if not (added or drop or stale):
        return
    with table.transaction() as transaction:
        if added or drop:
            with transaction.update_schema() as update:
                update.union_by_name(schema)
                for name in sorted(drop):
                    update.delete_column(name)
        if current is not None:
            # Existing files stay referenced while scans of main use the evolved schema.
            transaction.append(pa.Table.from_pylist([], schema=transaction.table_metadata.schema().as_arrow()))
