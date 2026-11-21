import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa

from lakehouse.utils import maintenance
from lakehouse.utils.iceberg import catalog_properties

VENDED = {
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "temporary-key",
    "s3.secret-access-key": "temporary-secret",
    "s3.session-token": "temporary-token",
    "client.region": "us-east-1",
}


class PyIcebergCatalogTests(unittest.TestCase):
    def test_requests_vended_credentials_without_storage_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            credentials = Path(directory) / "clients.env"
            credentials.write_text("POLARIS_BATCH_CLIENT_ID=batch\nPOLARIS_BATCH_CLIENT_SECRET=secret\n")
            environment = {
                "POLARIS_CATALOG_URI": "http://polaris:8181/api/catalog",
                "POLARIS_CATALOG_NAME": "lakehouse",
                "POLARIS_CREDENTIAL_FILE": str(credentials),
            }
            with patch.dict(os.environ, environment, clear=True):
                properties = catalog_properties()
        self.assertEqual(properties["header.X-Iceberg-Access-Delegation"], "vended-credentials")
        self.assertEqual(properties["credential"], "batch:secret")
        self.assertFalse([key for key in properties if key.startswith("s3.")])


class OrphanScanTests(unittest.TestCase):
    def test_lists_the_table_location_with_its_vended_credentials(self):
        table = SimpleNamespace(
            location=lambda: "s3://lakehouse/warehouse/silver/orders",
            io=SimpleNamespace(properties=VENDED),
            inspect=SimpleNamespace(all_files=lambda: pa.table({"file_path": pa.array([], pa.string())})),
        )
        listed = []
        client = SimpleNamespace(
            get_paginator=lambda name: SimpleNamespace(
                paginate=lambda **kwargs: listed.append(kwargs) or [{"Contents": []}]
            )
        )
        with (
            patch.dict(os.environ, {"MINIO_LAKEHOUSE_BUCKET": "lakehouse"}, clear=True),
            patch.object(maintenance.boto3, "client", return_value=client) as make_client,
        ):
            result = maintenance.orphan_files(table)
        self.assertEqual(
            make_client.call_args.kwargs,
            {
                "endpoint_url": "http://minio:9000",
                "aws_access_key_id": "temporary-key",
                "aws_secret_access_key": "temporary-secret",
                "aws_session_token": "temporary-token",
                "region_name": "us-east-1",
            },
        )
        self.assertEqual(listed, [{"Bucket": "lakehouse", "Prefix": "warehouse/silver/orders/data/"}])
        self.assertEqual(result["orphan_candidates"], 0)


if __name__ == "__main__":
    unittest.main()
