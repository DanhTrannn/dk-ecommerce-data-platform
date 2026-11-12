"""Python Iceberg catalog access using the existing workload credential files."""

import os
from pathlib import Path

from pyiceberg.catalog import load_catalog


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
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
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
