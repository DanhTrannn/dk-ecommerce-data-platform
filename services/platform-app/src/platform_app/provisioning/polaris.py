"""Create missing Polaris resources once; preserve later manual RBAC changes."""

import fcntl
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit

import yaml

from platform_app.provisioning.client import PolarisClient, ProvisionError
from platform_app.provisioning.state import (
    export_credentials,
    private_directory,
    read_credentials,
    read_json,
    save_json,
)


def load_spec(path):
    text = Path(path).read_text()
    text = re.sub(r"\$\{([A-Z0-9_]+):-([^}]*)\}", lambda match: os.environ.get(match[1]) or match[2], text)
    spec = yaml.safe_load(text)
    catalog = spec["catalog"]
    bucket = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
    catalog["name"] = os.environ.get("POLARIS_CATALOG_NAME", catalog["name"])
    catalog["default_base_location"] = os.environ.get("POLARIS_WAREHOUSE_LOCATION", f"s3://{bucket}/warehouse")
    catalog["allowed_locations"] = [os.environ.get("POLARIS_ALLOWED_LOCATION", f"s3://{bucket}/")]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", catalog["name"]):
        raise ProvisionError("Catalog name must use letters, digits, underscores or hyphens")
    for group in ("catalog_roles", "principal_roles", "principals"):
        if not all(re.fullmatch(r"[A-Za-z0-9_-]+", name) for name in spec[group]):
            raise ProvisionError("Invalid role/principal name")
    for output in spec["credential_outputs"]:
        output["uid"], output["gid"] = int(output["uid"]), int(output["gid"])
        if min(output["uid"], output["gid"]) < 0:
            raise ProvisionError("Invalid consumer ownership")
        path = Path(output["directory"])
        if path.parent != Path("/run/polaris-output") or path.is_symlink():
            raise ProvisionError("Credential outputs must be private consumer mounts")
        if output["principal"] not in spec["principals"]:
            raise ProvisionError("Unknown output principal")
    for principal in spec["principals"].values():
        if not all(re.fullmatch(r"POLARIS_[A-Z_]+", value) for value in principal["credentials_export"].values()):
            raise ProvisionError("Invalid credential variable name")
    return spec


def catalog_payload(spec):
    source = spec["catalog"]
    return {
        "name": source["name"],
        "type": "INTERNAL",
        "properties": {"default-base-location": source["default_base_location"]},
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": source["allowed_locations"],
            "endpoint": source["s3"]["endpoint"],
            "endpointInternal": source["s3"]["endpoint_internal"],
            "region": source["s3"]["region"],
            "pathStyleAccess": source["s3"]["path_style_access"],
            "stsUnavailable": True,
            "kmsUnavailable": True,
        },
    }


def _credential(client, spec, name, principal, state_dir):
    path = state_dir / f"{name}.json"
    candidates = []
    saved = read_json(path)
    if saved:
        candidates.append(saved)
    mapping = spec["principals"][name]["credentials_export"]
    for output in spec["credential_outputs"]:
        if output["principal"] == name:
            values = read_credentials(Path(output["directory"]) / "clients.env")
            if values:
                candidates.append(
                    {
                        "clientId": values.get(mapping["client_id_var"]),
                        "clientSecret": values.get(mapping["client_secret_var"]),
                    }
                )
    for candidate in candidates:
        if candidate.get("clientId") != principal.get("clientId") or not candidate.get("clientSecret"):
            continue
        try:
            client.credentials_token(candidate["clientId"], candidate["clientSecret"])
        except ProvisionError:
            continue
        save_json(path, candidate)
        return candidate
    raise ProvisionError(
        f"No valid saved credential for {name}; restore private files or rotate explicitly in Polaris. No reset was attempted."
    )


def provision(client, spec, state_dir, scope):
    state_dir = private_directory(state_dir)
    manifest_path = state_dir / "bootstrap.json"
    plan = read_json(manifest_path)
    if plan and plan["scope"] != scope:
        raise ProvisionError("Bootstrap state belongs to a different Polaris target/realm/catalog")
    desired = catalog_payload(spec)
    name = quote(desired["name"], safe="")
    base = "/api/management/v1"
    catalog_path = f"{base}/catalogs/{name}"
    existing = client.call("GET", catalog_path, missing=True)
    if existing:
        compatible = (
            existing.get("type") == desired["type"]
            and existing.get("properties", {}).get("default-base-location")
            == desired["properties"]["default-base-location"]
            and all(
                existing.get("storageConfigInfo", {}).get(key) == value
                for key, value in desired["storageConfigInfo"].items()
            )
        )
        if not compatible:
            raise ProvisionError("Existing catalog storage differs; no location/configuration was overwritten")
    digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    if plan is None:
        catalog_roles = (
            {role["name"] for role in client.call("GET", catalog_path + "/catalog-roles")["roles"]}
            if existing
            else set()
        )
        principal_roles = {role["name"] for role in client.call("GET", base + "/principal-roles")["roles"]}
        principals = {item["name"] for item in client.call("GET", base + "/principals")["principals"]}
        plan = dict(
            scope=scope,
            phase="initializing",
            spec_sha256=digest,
            catalog_roles=sorted(set(spec["catalog_roles"]) - catalog_roles),
            principal_roles=sorted(set(spec["principal_roles"]) - principal_roles),
            principals=sorted(set(spec["principals"]) - principals),
        )
        save_json(manifest_path, plan)  # Persist ownership intent before any POST.
    if plan["phase"] != "complete" and plan["spec_sha256"] != digest:
        raise ProvisionError(
            "Bootstrap template changed during partial initialization; restore its original configuration"
        )
    if not existing:
        if plan["phase"] == "complete":
            raise ProvisionError("Previously initialized catalog is missing; state was retained")
        client.call("POST", base + "/catalogs", {"catalog": desired})
    if plan["phase"] != "complete":
        for role in plan["catalog_roles"]:
            path = catalog_path + "/catalog-roles/" + role
            if client.call("GET", path, missing=True) is None:
                client.call("POST", catalog_path + "/catalog-roles", {"catalogRole": {"name": role}})
            grants_path = path + "/grants"
            current = client.call("GET", grants_path)["grants"]
            for grant in spec["catalog_roles"][role]["grants"]:
                if grant not in current:
                    client.call("PUT", grants_path, {"grant": grant})
        for role in plan["principal_roles"]:
            path = base + "/principal-roles/" + role
            if client.call("GET", path, missing=True) is None:
                client.call("POST", base + "/principal-roles", {"principalRole": {"name": role}})
            assigned = {item["name"] for item in client.call("GET", path + "/catalog-roles/" + name)["roles"]}
            for catalog_role in spec["principal_roles"][role]["catalog_roles"]:
                if catalog_role not in assigned:
                    client.call("PUT", path + "/catalog-roles/" + name, {"catalogRole": {"name": catalog_role}})
        for namespace in spec["namespaces"]:
            path = f"/api/catalog/v1/{name}/namespaces"
            if client.call("GET", path + "/" + quote(namespace, safe=""), missing=True) is None:
                client.call("POST", path, {"namespace": [namespace], "properties": {}})
    credentials = {}
    for principal_name, principal_spec in spec["principals"].items():
        path = base + "/principals/" + principal_name
        principal = client.call("GET", path, missing=True)
        if principal is None:
            if plan["phase"] == "complete" or principal_name not in plan["principals"]:
                raise ProvisionError("Previously existing principal is missing; no automatic recreation")
            response = client.call(
                "POST",
                base + "/principals",
                {
                    "principal": {"name": principal_name},
                    "credentialRotationRequired": False,
                },
            )
            # Create/rotate are the only credential-returning APIs. Persist immediately.
            save_json(state_dir / f"{principal_name}.json", response["credentials"])
            principal = response["principal"]
        credentials[principal_name] = _credential(client, spec, principal_name, principal, state_dir)
        if plan["phase"] != "complete" and principal_name in plan["principals"]:
            assigned = {item["name"] for item in client.call("GET", path + "/principal-roles")["roles"]}
            for role in principal_spec["principal_roles"]:
                if role not in assigned:
                    client.call("PUT", path + "/principal-roles", {"principalRole": {"name": role}})
    for output in spec["credential_outputs"]:
        export_credentials(
            output, credentials[output["principal"]], spec["principals"][output["principal"]]["credentials_export"]
        )
    plan["phase"] = "complete"
    save_json(manifest_path, plan)
    return {
        "catalog": desired["name"],
        "principals": len(credentials),
        "credential_outputs": len(spec["credential_outputs"]),
        "status": "ready",
        "rbac": "initialization-only",
    }


def main():
    client = None
    try:
        origin = os.environ.get("POLARIS_PROVISION_URL", "http://polaris:8181").rstrip("/")
        url = urlsplit(origin)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.password
            or url.path
            or url.query
            or url.fragment
        ):
            raise ProvisionError("Polaris origin must not contain credentials, paths or query parameters")
        secret = os.environ.get("POLARIS_ROOT_CLIENT_SECRET")
        if not secret:
            raise ProvisionError("POLARIS_ROOT_CLIENT_SECRET is required")
        realm = os.environ.get("POLARIS_REALM", "POLARIS")
        spec = load_spec(os.environ.get("POLARIS_RBAC_PATH", "/app/infrastructure/polaris/rbac.yml"))
        state_dir = private_directory(os.environ.get("POLARIS_PROVISION_STATE_DIR", "/run/polaris-state"))
        with (state_dir / ".lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            client = PolarisClient(origin, realm)
            client.token = client.credentials_token(os.environ.get("POLARIS_ROOT_CLIENT_ID", "root"), secret)
            result = provision(
                client, spec, state_dir, {"origin": origin, "realm": realm, "catalog": spec["catalog"]["name"]}
            )
        print(json.dumps(result))
    except ProvisionError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
    except Exception:
        print(
            "Provisioning failed; inspect private state and target configuration. No credentials are logged or reset.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
