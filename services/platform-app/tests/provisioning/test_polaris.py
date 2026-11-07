"""Initialization/recovery guarantees without external services or credentials."""

import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from platform_app.provisioning.client import PolarisClient, ProvisionError
from platform_app.provisioning.polaris import catalog_payload, provision
from platform_app.provisioning.state import atomic_private, export_credentials, read_credentials


class FakePolaris:
    def __init__(self):
        self.resources = {
            "/api/management/v1/principal-roles": {"roles": []},
            "/api/management/v1/principals": {"principals": []},
        }
        self.calls = []
        self.credentials = {}

    def credentials_token(self, client_id, secret):
        if self.credentials.get(client_id) != secret:
            raise ProvisionError("Invalid credentials")
        return "token"

    def call(self, method, path, payload=None, missing=False):
        self.calls.append((method, path))
        if method == "GET":
            if missing and path not in self.resources:
                return None
            return copy.deepcopy(self.resources.get(path, {"roles": [], "grants": []}))
        if method == "POST":
            if path.endswith("/catalogs"):
                self.resources[path + "/" + payload["catalog"]["name"]] = payload["catalog"]
            elif path.endswith("/catalog-roles"):
                self.resources[path + "/" + payload["catalogRole"]["name"]] = payload["catalogRole"]
            elif path.endswith("/principal-roles"):
                self.resources[path + "/" + payload["principalRole"]["name"]] = payload["principalRole"]
            elif path.endswith("/principals"):
                name = payload["principal"]["name"]
                principal = {"name": name, "clientId": "id-" + name}
                self.resources[path + "/" + name] = principal
                self.credentials[principal["clientId"]] = "secret-" + name
                return {
                    "principal": principal,
                    "credentials": {"clientId": principal["clientId"], "clientSecret": "secret-" + name},
                }
            elif path.endswith("/namespaces"):
                self.resources[path + "/" + payload["namespace"][0]] = payload
        elif method == "PUT":
            if path.endswith("/grants"):
                self.resources.setdefault(path, {"grants": []})["grants"].append(payload["grant"])
            else:
                role = payload.get("principalRole") or payload["catalogRole"]
                self.resources.setdefault(path, {"roles": []})["roles"].append(role)
        return {}


class ProvisionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.spec = {
            "catalog": {
                "name": "lakehouse",
                "default_base_location": "s3://bucket/warehouse",
                "allowed_locations": ["s3://bucket/"],
                "s3": {
                    "endpoint": "http://minio:9000",
                    "endpoint_internal": "http://minio:9000",
                    "region": "us-east-1",
                    "path_style_access": True,
                },
            },
            "namespaces": ["gold"],
            "catalog_roles": {"reader": {"grants": [{"type": "catalog", "privilege": "TABLE_READ_DATA"}]}},
            "principal_roles": {"reader_role": {"catalog_roles": ["reader"]}},
            "principals": {
                "trino_reader": {
                    "principal_roles": ["reader_role"],
                    "credentials_export": {
                        "client_id_var": "POLARIS_TRINO_CLIENT_ID",
                        "client_secret_var": "POLARIS_TRINO_CLIENT_SECRET",
                    },
                }
            },
            "credential_outputs": [
                {
                    "principal": "trino_reader",
                    "directory": str(self.root / "output"),
                    "uid": os.getuid(),
                    "gid": os.getgid(),
                }
            ],
        }
        self.scope = {"origin": "http://polaris", "realm": "POLARIS", "catalog": "lakehouse"}
        self.client = FakePolaris()
        # Export logic is exercised on a real filesystem; chown is unnecessary for same-user tests.
        patcher = patch("platform_app.provisioning.state.os.chown")
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = patch("platform_app.provisioning.state.os.fchown")
        patcher.start()
        self.addCleanup(patcher.stop)

    def initialize(self):
        return provision(self.client, self.spec, self.state, self.scope)

    def test_first_init_then_rerun_preserves_revoked_grants_and_credentials(self):
        self.assertEqual(self.initialize()["status"], "ready")
        credential_file = self.root / "output/clients.env"
        before = credential_file.read_bytes()
        self.client.resources["/api/management/v1/catalogs/lakehouse/catalog-roles/reader/grants"] = {"grants": []}
        self.client.calls.clear()
        self.initialize()
        self.assertTrue(all(method == "GET" for method, _ in self.client.calls))
        self.assertEqual(credential_file.read_bytes(), before)
        self.assertEqual(credential_file.stat().st_mode & 0o777, 0o600)
        self.assertEqual(credential_file.parent.stat().st_mode & 0o777, 0o700)

    def test_missing_journal_recovers_from_matching_consumer_file(self):
        self.initialize()
        (self.state / "trino_reader.json").unlink()
        self.client.calls.clear()
        self.initialize()
        self.assertTrue((self.state / "trino_reader.json").exists())
        self.assertTrue(all(method == "GET" for method, _ in self.client.calls))

    def test_existing_principal_without_secret_never_rotates(self):
        self.initialize()
        (self.state / "trino_reader.json").unlink()
        (self.root / "output/clients.env").unlink()
        self.client.calls.clear()
        with self.assertRaisesRegex(ProvisionError, "No valid saved credential"):
            self.initialize()
        self.assertTrue(all(method == "GET" for method, _ in self.client.calls))
        self.assertFalse(any("rotate" in path or "reset" in path for _, path in self.client.calls))

    def test_different_existing_output_is_not_overwritten(self):
        self.initialize()
        path = self.root / "output/clients.env"
        path.write_text("POLARIS_TRINO_CLIENT_ID=other\nPOLARIS_TRINO_CLIENT_SECRET=private\n")
        with self.assertRaisesRegex(ProvisionError, "Existing consumer credentials differ"):
            self.initialize()
        self.assertIn("private", path.read_text())

    def test_matching_legacy_file_keeps_extra_settings(self):
        self.initialize()
        path = self.root / "output/clients.env"
        path.write_text(path.read_text() + "POLARIS_REALM=POLARIS\n")
        before = path.read_bytes()
        self.initialize()
        self.assertEqual(path.read_bytes(), before)

    def test_wrong_target_state_fails_before_requests(self):
        self.initialize()
        self.client.calls.clear()
        with self.assertRaisesRegex(ProvisionError, "different Polaris target"):
            provision(self.client, self.spec, self.state, {"realm": "OTHER"})
        self.assertEqual(self.client.calls, [])

    def test_existing_catalog_location_is_not_replaced(self):
        self.client.resources["/api/management/v1/catalogs/lakehouse"] = catalog_payload(self.spec)
        self.client.resources["/api/management/v1/catalogs/lakehouse"]["properties"]["default-base-location"] = (
            "s3://other/warehouse"
        )
        with self.assertRaisesRegex(ProvisionError, "storage differs"):
            self.initialize()
        self.assertTrue(all(method == "GET" for method, _ in self.client.calls))

    def test_resume_partial_roles_with_original_template(self):
        original = self.client.call
        interrupted = False

        def call(method, path, payload=None, missing=False):
            nonlocal interrupted
            if method == "PUT" and path.endswith("/grants") and not interrupted:
                interrupted = True
                raise ProvisionError("Interrupted")
            return original(method, path, payload, missing)

        self.client.call = call
        with self.assertRaisesRegex(ProvisionError, "Interrupted"):
            self.initialize()
        self.assertEqual(json.loads((self.state / "bootstrap.json").read_text())["phase"], "initializing")
        self.assertEqual(self.initialize()["status"], "ready")

    def test_lost_create_response_journal_never_resets_existing_principal(self):
        from platform_app.provisioning.state import save_json

        def write(path, value):
            if Path(path).name == "trino_reader.json":
                raise OSError("simulated persistence failure")
            return save_json(path, value)

        with patch("platform_app.provisioning.polaris.save_json", side_effect=write):
            with self.assertRaises(OSError):
                self.initialize()
        self.client.calls.clear()
        with self.assertRaisesRegex(ProvisionError, "No valid saved credential"):
            self.initialize()
        self.assertFalse(any("rotate" in path or "reset" in path for _, path in self.client.calls))
        self.assertFalse(any(method == "POST" and path.endswith("/principals") for method, path in self.client.calls))

    def test_shell_values_round_trip_without_execution(self):
        output = self.spec["credential_outputs"][0]
        mapping = self.spec["principals"]["trino_reader"]["credentials_export"]
        value = "s' ecret$(touch never-created);x"
        export_credentials(output, {"clientId": "id", "clientSecret": value}, mapping)
        self.assertEqual(read_credentials(self.root / "output/clients.env")[mapping["client_secret_var"]], value)

    def test_symlink_output_is_rejected(self):
        elsewhere = self.root / "elsewhere"
        elsewhere.write_text("retained")
        output = self.root / "output"
        output.mkdir()
        (output / "clients.env").symlink_to(elsewhere)
        with self.assertRaises(ProvisionError):
            atomic_private(output / "clients.env", "replacement")
        self.assertEqual(elsewhere.read_text(), "retained")


class ClientTests(unittest.TestCase):
    def test_write_failure_is_redacted_and_not_retried(self):
        calls = []

        def handle(request):
            calls.append(request)
            return httpx.Response(503, text="private secret")

        client = PolarisClient("http://polaris.test", "POLARIS", transport=httpx.MockTransport(handle))
        self.addCleanup(client.close)
        with self.assertRaises(ProvisionError) as caught:
            client.call("POST", "/api/management/v1/principals", {"principal": {"name": "test"}})
        self.assertEqual(len(calls), 1)
        self.assertNotIn("private secret", str(caught.exception))

    def test_auth_failure_does_not_include_body_or_secret(self):
        client = PolarisClient(
            "http://polaris.test",
            "POLARIS",
            transport=httpx.MockTransport(lambda _: httpx.Response(401, text="private secret")),
        )
        self.addCleanup(client.close)
        with self.assertRaises(ProvisionError) as caught:
            client.credentials_token("id", "private secret")
        self.assertNotIn("private secret", str(caught.exception))
