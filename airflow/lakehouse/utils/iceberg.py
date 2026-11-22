import os
import shlex
from pathlib import Path

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

PROPERTIES = {"format-version": "2", "write.parquet.compression-codec": "zstd"}


def catalog_properties():
    required = ("POLARIS_CATALOG_URI", "POLARIS_CATALOG_NAME")
    if any(not os.environ.get(key, "").strip() for key in required):
        raise ValueError("Configure the Polaris catalog URI and name")
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
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }


def iceberg_catalog():
    return load_catalog(os.environ.get("POLARIS_CATALOG_NAME", "lakehouse"), **catalog_properties())


def load_table(catalog, key):
    try:
        return catalog.load_table(key)
    except NoSuchTableError:
        raise NoSuchTableError(f"{'.'.join(key)} does not exist; run the lakehouse_oltp_init DAG first") from None
