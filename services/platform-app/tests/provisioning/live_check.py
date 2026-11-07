"""Container-side permissions/repeatability checks against disposable real Polaris."""

import json
import os
from pathlib import Path

from platform_app.provisioning.client import PolarisClient, ProvisionError
from platform_app.provisioning.polaris import load_spec, provision
from platform_app.provisioning.state import read_json


def main():
    client = PolarisClient("http://polaris:8181", "POLARIS")
    try:
        client.token = client.credentials_token(
            os.environ["POLARIS_ROOT_CLIENT_ID"], os.environ["POLARIS_ROOT_CLIENT_SECRET"]
        )
        spec = load_spec("/app/infrastructure/polaris/rbac.yml")
        state = Path("/run/polaris-state")
        scope = dict(origin="http://polaris:8181", realm="POLARIS", catalog="lakehouse")
        result = provision(client, spec, state, scope)
        files = {}
        for output in spec["credential_outputs"]:
            path = Path(output["directory"]) / "clients.env"
            files[str(path)] = path.read_bytes()
            assert path.stat().st_uid == output["uid"] and path.stat().st_gid == output["gid"]
            assert path.stat().st_mode & 0o777 == 0o600
            assert path.parent.stat().st_mode & 0o777 == 0o700
        writer = read_json(state / "spark_writer.json")
        reader = read_json(state / "trino_reader.json")
        root_token = client.token
        client.token = client.credentials_token(writer["clientId"], writer["clientSecret"])
        path = "/api/catalog/v1/lakehouse/namespaces/gold/tables"
        client.call(
            "POST",
            path,
            {
                "name": "provisioning_check",
                "schema": {"type": "struct", "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}]},
                "properties": {"format-version": "2"},
            },
        )
        client.token = client.credentials_token(reader["clientId"], reader["clientSecret"])
        table = client.call("GET", path + "/provisioning_check")
        assert table["metadata"]["location"].startswith("s3://lakehouse/warehouse/")
        for method, target, payload in (
            ("POST", path, {"name": "forbidden", "schema": {"type": "struct", "fields": []}}),
            ("DELETE", path + "/provisioning_check", None),
            ("GET", "/api/management/v1/principals", None),
        ):
            try:
                client.call(method, target, payload)
            except ProvisionError as error:
                assert "403" in str(error), str(error)
            else:
                raise AssertionError("Reader mutation/admin access unexpectedly allowed")
        client.token = root_token
        grants = "/api/management/v1/catalogs/lakehouse/catalog-roles/trino_reader_catalog_role/grants"
        client.call("POST", grants, {"grant": {"type": "catalog", "privilege": "TABLE_READ_DATA"}})
        client.call(
            "POST", "/api/catalog/v1/lakehouse/namespaces", {"namespace": ["manual_namespace"], "properties": {}}
        )
        # A completed bootstrap must not undo this deliberate manual revocation.
        provision(client, spec, state, scope)
        assert not any(grant["privilege"] == "TABLE_READ_DATA" for grant in client.call("GET", grants)["grants"])
        assert client.call("GET", "/api/catalog/v1/lakehouse/namespaces/manual_namespace")
        for name, content in files.items():
            assert Path(name).read_bytes() == content
        # Recover an accidentally missing private journal from a valid consumer copy.
        (state / "spark_writer.json").unlink()
        provision(client, spec, state, scope)
        assert read_json(state / "spark_writer.json") == writer
        print("POLARIS_PROVISION_CHECK_OK " + json.dumps(result))
    finally:
        client.close()


if __name__ == "__main__":
    main()
