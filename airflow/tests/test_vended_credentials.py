import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lakehouse.utils.iceberg import catalog_properties

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


if __name__ == "__main__":
    unittest.main()
