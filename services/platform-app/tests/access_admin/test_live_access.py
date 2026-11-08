"""Reuse the isolated storage lifecycle; additionally exercise the access API."""

import os
import unittest
from unittest.mock import patch

from provisioning import test_live as provision_tests


@unittest.skipUnless(os.environ.get("RUN_ACCESS_TESTS") == "1", "opt-in disposable access administration")
class LiveAccessTests(unittest.TestCase):
    command = provision_tests.LiveProvisionTests.command

    def test_real_access_api_and_providers(self):
        with patch.dict(
            os.environ,
            {
                "RUN_ACCESS_EXERCISE": "1",
                "PROVISION_TEST_IMAGE": os.environ.get("ACCESS_TEST_IMAGE", "tlcn-access-check:local"),
            },
        ):
            provision_tests.LiveProvisionTests.test_minio_and_polaris_bootstrap_permissions_and_rerun(self)
