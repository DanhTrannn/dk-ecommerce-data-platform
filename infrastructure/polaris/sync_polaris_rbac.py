#!/usr/bin/env python3
"""Bootstrap Polaris once; subsequent init preserves Console-managed access.

The historical filename remains the Compose entrypoint. YAML is a first-run
seed, not an authoritative configuration for an already initialized realm.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [polaris-bootstrap] %(message)s")
logger = logging.getLogger("polaris-bootstrap")
STATE_VERSION = 1
OWNER_PROPERTY = "tlcn.bootstrap-id"


def expand_env(val: Any) -> Any:
    """Recursively expand environment variables formatted as ${VAR} or ${VAR:-default}."""
    if isinstance(val, str):
        pattern = re.compile(r"\$\{([^}:]+)(?::-([^}]*))?\}")

        def replacer(match: re.Match) -> str:
            var_name = match.group(1)
            default_val = match.group(2) if match.group(2) is not None else ""
            return os.environ.get(var_name, default_val)

        return pattern.sub(replacer, val)
    elif isinstance(val, dict):
        return {k: expand_env(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [expand_env(item) for item in val]
    return val


class PolarisClient:
    """HTTP client for Apache Polaris REST and Management APIs."""

    def __init__(
        self,
        base_url: str,
        realm: str,
        root_client_id: str,
        root_client_secret: str,
    ):
        self.base_url = base_url.rstrip("/")
        self.realm = realm
        self.root_client_id = root_client_id
        self.root_client_secret = root_client_secret
        self.token: Optional[str] = None
        self.token_expiry: float = 0

    def obtain_token(self, client_id: str, client_secret: str) -> Optional[str]:
        """Obtain an OAuth2 token using client credentials."""
        token_url = f"{self.base_url}/api/catalog/v1/oauth/tokens"
        data = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            token_url,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "Polaris-Realm": self.realm,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                res = json.loads(resp.read().decode("utf-8"))
                return res.get("access_token")
        except Exception:
            return None

    def ensure_root_token(self, max_retries: int = 30, retry_delay: float = 2.0) -> str:
        """Ensure a valid root Bearer token is available, retrying if Polaris is starting."""
        now = time.time()
        if self.token and now < self.token_expiry - 60:
            return self.token

        for attempt in range(1, max_retries + 1):
            token = self.obtain_token(self.root_client_id, self.root_client_secret)
            if token:
                self.token = token
                self.token_expiry = time.time() + 3600
                logger.info("Successfully authenticated with Polaris as root.")
                return token
            logger.warning(
                "Waiting for Polaris root authentication (attempt %d/%d)...",
                attempt,
                max_retries,
            )
            time.sleep(retry_delay)

        raise RuntimeError("Failed to obtain root Bearer token from Polaris.")

    def request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        expected_statuses: Tuple[int, ...] = (200, 201, 204),
    ) -> Tuple[int, Any]:
        """Perform an authenticated request against Polaris Management API."""
        token = self.ensure_root_token()
        url = f"{self.base_url}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None

        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Authorization": f"Bearer {token}",
                "Polaris-Realm": self.realm,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method=method,
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                status = resp.status
                if status not in expected_statuses:
                    raise RuntimeError(f"Unexpected HTTP {status} for {method} {path}")
                body = resp.read().decode("utf-8")
                res = json.loads(body) if body else None
                return status, res
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8")
            res = None
            try:
                res = json.loads(body) if body else None
            except Exception:
                res = body
            if e.code in expected_statuses:
                return e.code, res
            raise RuntimeError(f"Polaris returned HTTP {e.code} for {method} {path}") from None


def load_saved_credentials(cred_file: Path) -> Dict[str, str]:
    """Parse existing credentials from the clients.env file."""
    if not cred_file.is_file() or cred_file.stat().st_size == 0:
        return {}
    creds: Dict[str, str] = {}
    for line in cred_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        creds[key] = val
    return creds


class PolarisBootstrap:
    """One local initialization lock, one durable marker, separate secret state."""

    def __init__(self, client, state_file, ready_file, config_path, catalog=None):
        self.client = client
        self.state_file = Path(state_file)
        self.ready_file = Path(ready_file)
        self.config_path = Path(config_path)
        self.expected_catalog = catalog
        self.state = None
        self.config = None
        self.credentials = {}

    @staticmethod
    def private_write(path, content, uid=None, gid=None):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
                handle.flush()
                os.fchmod(handle.fileno(), 0o600)
                if uid is not None or gid is not None:
                    os.fchown(handle.fileno(), uid if uid is not None else -1, gid if gid is not None else -1)
                os.fsync(handle.fileno())
            os.replace(name, path)
            if uid is not None or gid is not None:
                os.chown(path.parent, uid if uid is not None else -1, gid if gid is not None else -1)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            Path(name).unlink(missing_ok=True)

    def save_state(self):
        self.private_write(self.ready_file, json.dumps(self.state, indent=2) + "\n")

    def save_credentials(self):
        lines = ["# Polaris runtime credentials; private bootstrap state."]
        for key, value in sorted(self.credentials.items()):
            if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key) or not value or any(c in value for c in "'\r\n"):
                raise RuntimeError("Invalid credential state encoding; restore the private credential file")
            lines.append(f"{key}='{value}'")
        self.private_write(self.state_file, "\n".join(lines) + "\n")

    def load_state(self):
        if not self.ready_file.exists() or self.ready_file.stat().st_size == 0:
            return None  # Empty ready files are from the previous reconciler.
        try:
            state = json.loads(self.ready_file.read_text())
        except (ValueError, OSError) as exc:
            raise RuntimeError("Invalid Polaris bootstrap marker; restore its backup, do not erase it") from exc
        if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
            raise RuntimeError("Unsupported Polaris bootstrap marker")
        if state.get("status") not in ("initializing", "complete") or not isinstance(state.get("config"), dict):
            raise RuntimeError("Incomplete Polaris bootstrap marker")
        identities = state.get("principal_client_ids")
        if not isinstance(identities, dict):
            raise RuntimeError("Invalid principal identities in bootstrap marker")
        if state["status"] == "complete" and set(identities) != set(state["config"].get("principals", {})):
            raise RuntimeError("Completed bootstrap marker is missing principal identities")
        if state["status"] == "initializing" and not state.get("bootstrap_id"):
            raise RuntimeError("Partial bootstrap marker is missing its initialization identity")
        if state.get("realm") != self.client.realm:
            raise RuntimeError("Polaris realm differs from the saved bootstrap state")
        if self.expected_catalog and self.expected_catalog != state.get("catalog"):
            raise RuntimeError("Polaris catalog differs from the saved bootstrap state")
        return state

    def validate_config(self):
        config = self.config
        if not isinstance(config, dict) or not isinstance(config.get("catalog"), dict):
            raise RuntimeError("Bootstrap template requires a catalog")
        self.catalog = config["catalog"].get("name")
        if not isinstance(self.catalog, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", self.catalog):
            raise RuntimeError("Invalid bootstrap catalog name")
        if self.expected_catalog and self.expected_catalog != self.catalog:
            raise RuntimeError("Bootstrap catalog and selected runtime catalog differ")
        for group in ("catalog_roles", "principal_roles"):
            roles = config.get(group, {})
            if not isinstance(roles, dict) or any(not re.fullmatch(r"[A-Za-z0-9_-]+", name) for name in roles):
                raise RuntimeError(f"Invalid {group} in bootstrap template")
        for role in config.get("principal_roles", {}).values():
            if set(role.get("catalog_roles", [])) - set(config.get("catalog_roles", {})) - {"catalog_admin"}:
                raise RuntimeError("Principal role references an unknown catalog role")
        if any(
            not isinstance(ns, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", ns) for ns in config.get("namespaces", [])
        ):
            raise RuntimeError("Invalid bootstrap namespace")
        principals = config.get("principals")
        if not isinstance(principals, dict) or not principals:
            raise RuntimeError("Bootstrap template requires service principals")
        self.exports = {}
        names = set()
        for principal, definition in principals.items():
            if not re.fullmatch(r"[A-Za-z0-9_-]+", principal):
                raise RuntimeError("Invalid bootstrap principal name")
            if set(definition.get("principal_roles", [])) - set(config.get("principal_roles", {})):
                raise RuntimeError(f"Principal {principal} references an unknown principal role")
            export = definition.get("credentials_export", {})
            keys = [export.get("client_id_var"), export.get("client_secret_var")]
            if any(not isinstance(k, str) or not re.fullmatch(r"[A-Z_][A-Z0-9_]*", k) for k in keys):
                raise RuntimeError(f"Invalid credential variable mapping for {principal}")
            if len(set(keys)) != 2 or names.intersection(keys):
                raise RuntimeError("Credential variable mappings must be distinct per principal")
            names.update(keys)
            self.exports[principal] = keys
        outputs = config.get("credential_outputs", [])
        paths = set()
        for output in outputs:
            if output.get("principal") not in self.exports:
                raise RuntimeError("Credential output references an unknown principal")
            path = Path(output["directory"]) / "clients.env"
            if not path.is_absolute() or path in paths or path in (self.ready_file, self.state_file):
                raise RuntimeError("Credential outputs must have distinct absolute paths")
            if any(not isinstance(output.get(k), int) or output[k] < 0 for k in ("uid", "gid")):
                raise RuntimeError("Credential output requires numeric UID/GID")
            paths.add(path)
        if set(self.exports) != {o["principal"] for o in outputs}:
            raise RuntimeError("Every service principal requires a credential output")

    def catalog_record(self):
        return self.client.request("GET", f"/api/management/v1/catalogs/{self.catalog}", expected_statuses=(200, 404))

    @staticmethod
    def creation_time(catalog):
        created = catalog.get("createTimestamp")
        if created is None:
            raise RuntimeError("Catalog identity is missing its creation timestamp")
        return created

    def verify_existing(self, catalog):
        if self.state.get("catalog_created_at") != self.creation_time(catalog):
            raise RuntimeError("Catalog was replaced or bootstrap state belongs to another environment")
        previous = self.state.get("principal_client_ids", {})
        identities = {}
        for principal, keys in self.exports.items():
            status, record = self.client.request(
                "GET", f"/api/management/v1/principals/{principal}", expected_statuses=(200, 404)
            )
            if status == 404:
                raise RuntimeError(
                    f"Principal {principal} is missing; restore it explicitly, init will not recreate it"
                )
            client_id, secret = (self.credentials.get(k) for k in keys)
            if not client_id or not secret or record.get("clientId") != client_id:
                raise RuntimeError(f"Missing/mismatched saved credentials for {principal}; automatic reset is disabled")
            if principal in previous and previous[principal] != client_id:
                raise RuntimeError(f"Principal {principal} was replaced; restore matching bootstrap state")
            if not self.client.obtain_token(client_id, secret):
                raise RuntimeError(
                    f"Saved credentials for {principal} cannot authenticate; restore/update credentials explicitly"
                )
            identities[principal] = client_id
        self.state["principal_client_ids"] = identities

    def restore_outputs(self):
        # Authentication for ALL principals finishes before any output is changed.
        for output in self.config["credential_outputs"]:
            path = Path(output["directory"]) / "clients.env"
            values = {key: self.credentials[key] for key in self.exports[output["principal"]]}
            valid_encoding = all(v and not any(c in v for c in "'\r\n") for v in values.values())
            if not valid_encoding:
                raise RuntimeError("Credential output contains unsupported characters")
            if path.is_file() and load_saved_credentials(path) == values:
                os.chmod(path.parent, 0o700)
                os.chown(path.parent, output["uid"], output["gid"])
                os.chmod(path, 0o600)
                os.chown(path, output["uid"], output["gid"])
                continue
            text = "# Polaris runtime credentials; this file belongs to one consumer.\n"
            text += "".join(f"{key}='{value}'\n" for key, value in sorted(values.items()))
            self.private_write(path, text, output["uid"], output["gid"])
            logger.info("Restored private credential output %s", path)
        os.chmod(self.state_file, 0o600)

    def create_catalog(self):
        status, record = self.catalog_record()
        if status == 404:
            if self.state.get("catalog_created_at") is not None:
                raise RuntimeError("Catalog disappeared during bootstrap; restore metadata instead of recreating it")
            conf = self.config["catalog"]
            s3 = conf.get("s3", {})
            self.client.request(
                "POST",
                "/api/management/v1/catalogs",
                payload={
                    "catalog": {
                        "name": self.catalog,
                        "type": "INTERNAL",
                        "readOnly": False,
                        "properties": {
                            "default-base-location": conf.get(
                                "default_base_location", f"s3://{self.catalog}/warehouse"
                            ),
                            OWNER_PROPERTY: self.state["bootstrap_id"],
                        },
                        "storageConfigInfo": {
                            "storageType": "S3",
                            "allowedLocations": conf.get("allowed_locations", [f"s3://{self.catalog}/"]),
                            "endpoint": s3.get("endpoint", "http://minio:9000"),
                            "endpointInternal": s3.get("endpoint_internal", "http://minio:9000"),
                            "pathStyleAccess": s3.get("path_style_access", True),
                            "region": s3.get("region", "us-east-1"),
                        },
                    }
                },
            )
            _, record = self.catalog_record()
        if record.get("properties", {}).get(OWNER_PROPERTY) != self.state["bootstrap_id"]:
            raise RuntimeError("Partial bootstrap found a catalog it did not create; explicit adoption is required")
        if self.state.get("catalog_created_at") not in (None, self.creation_time(record)):
            raise RuntimeError("Catalog changed during bootstrap")
        self.state["catalog_created_at"] = self.creation_time(record)
        self.save_state()

    def create_roles(self):
        for name, definition in self.config.get("catalog_roles", {}).items():
            base = f"/api/management/v1/catalogs/{self.catalog}/catalog-roles"
            status, _ = self.client.request("GET", f"{base}/{name}", expected_statuses=(200, 404))
            if status == 404:
                self.client.request(
                    "POST",
                    base,
                    payload={"catalogRole": {"name": name, "properties": {OWNER_PROPERTY: self.state["bootstrap_id"]}}},
                )
            _, grants = self.client.request("GET", f"{base}/{name}/grants")
            existing = {g["privilege"] for g in grants.get("grants", [])}
            for grant in definition.get("grants", []):
                if grant["privilege"] not in existing:
                    self.client.request("PUT", f"{base}/{name}/grants", payload={"grant": grant})
        for name, definition in self.config.get("principal_roles", {}).items():
            base = "/api/management/v1/principal-roles"
            status, role = self.client.request("GET", f"{base}/{name}", expected_statuses=(200, 404))
            if status == 404:
                self.client.request(
                    "POST",
                    base,
                    payload={
                        "principalRole": {"name": name, "properties": {OWNER_PROPERTY: self.state["bootstrap_id"]}}
                    },
                )
            elif role.get("properties", {}).get(OWNER_PROPERTY) != self.state["bootstrap_id"]:
                raise RuntimeError(f"Principal role {name} was not created by this bootstrap")
            _, assigned = self.client.request("GET", f"{base}/{name}/catalog-roles/{self.catalog}")
            existing = {r["name"] for r in assigned.get("roles", [])}
            for role in definition.get("catalog_roles", []):
                if role not in existing:
                    self.client.request(
                        "PUT", f"{base}/{name}/catalog-roles/{self.catalog}", payload={"catalogRole": {"name": role}}
                    )

    def create_principals(self):
        for principal, keys in self.exports.items():
            base = "/api/management/v1/principals"
            status, record = self.client.request("GET", f"{base}/{principal}", expected_statuses=(200, 404))
            if status == 404:
                if any(k in self.credentials for k in keys) or principal in self.state["principal_client_ids"]:
                    raise RuntimeError(f"Principal {principal} disappeared during bootstrap; refusing recreation")
                _, result = self.client.request(
                    "POST",
                    base,
                    payload={
                        "principal": {"name": principal, "properties": {"service": self.catalog}},
                        "credentialRotationRequired": False,
                    },
                )
                issued = result.get("credentials", {})
                if not issued.get("clientId") or not issued.get("clientSecret"):
                    raise RuntimeError(
                        f"No credentials issued for {principal}; recover explicitly, do not reset bootstrap state"
                    )
                self.credentials.update(dict(zip(keys, (issued["clientId"], issued["clientSecret"]))))
                self.save_credentials()  # Persist each issued secret before further API calls.
            else:
                client_id, secret = (self.credentials.get(k) for k in keys)
                if (
                    not client_id
                    or not secret
                    or record.get("clientId") != client_id
                    or not self.client.obtain_token(client_id, secret)
                ):
                    raise RuntimeError(
                        f"Cannot resume principal {principal}; restore saved credentials, automatic reset is disabled"
                    )
            self.state["principal_client_ids"][principal] = self.credentials[keys[0]]
            self.save_state()
            _, assigned = self.client.request("GET", f"{base}/{principal}/principal-roles")
            existing = {r["name"] for r in assigned.get("roles", [])}
            for role in self.config["principals"][principal].get("principal_roles", []):
                if role not in existing:
                    self.client.request(
                        "PUT", f"{base}/{principal}/principal-roles", payload={"principalRole": {"name": role}}
                    )

    def initialize(self):
        self.create_catalog()
        self.create_roles()
        self.create_principals()
        for namespace in self.config.get("namespaces", []):
            self.client.request(
                "POST",
                f"/api/catalog/v1/{self.catalog}/namespaces",
                payload={"namespace": [namespace], "properties": {}},
                expected_statuses=(200, 201, 409),
            )
        _, catalog = self.catalog_record()
        self.verify_existing(catalog)
        self.restore_outputs()
        self.state["status"] = "complete"
        self.save_state()
        logger.info("Polaris bootstrap completed; future permissions are managed through Console/API")

    def _run(self):
        self.state = self.load_state()
        self.credentials = load_saved_credentials(self.state_file)
        if self.state:
            self.config = self.state["config"]
        else:
            with self.config_path.open(encoding="utf-8") as handle:
                self.config = expand_env(yaml.safe_load(handle))
        self.validate_config()
        status, catalog = self.catalog_record()
        if self.state is not None:
            if self.state.get("catalog") != self.catalog:
                raise RuntimeError("Saved bootstrap catalog/config mismatch")
            if self.state["status"] == "initializing":
                self.initialize()
                return
            if status == 404:
                raise RuntimeError("Bootstrapped catalog is missing; restore metadata, init will not recreate it")
            self.verify_existing(catalog)
            self.restore_outputs()
            logger.info(
                "Polaris already bootstrapped; Console-managed roles, grants and catalog configuration preserved"
            )
            return
        if status == 200:
            self.state = {
                "version": STATE_VERSION,
                "status": "complete",
                "realm": self.client.realm,
                "catalog": self.catalog,
                "config": self.config,
                "catalog_created_at": self.creation_time(catalog),
                "principal_client_ids": {},
            }
            self.verify_existing(catalog)
            self.restore_outputs()
            self.save_state()
            logger.info("Adopted existing Polaris catalog and credentials without modifying remote RBAC")
            return
        if self.credentials or self.ready_file.exists():
            raise RuntimeError(
                "Existing local state has no matching catalog; restore metadata instead of reinitializing"
            )
        # Detect global-name collisions before creating any remote objects.
        for principal in self.exports:
            found, _ = self.client.request(
                "GET", f"/api/management/v1/principals/{principal}", expected_statuses=(200, 404)
            )
            if found == 200:
                raise RuntimeError(f"Principal {principal} already exists without matching local state")
        for role in self.config.get("principal_roles", {}):
            found, _ = self.client.request(
                "GET", f"/api/management/v1/principal-roles/{role}", expected_statuses=(200, 404)
            )
            if found == 200:
                raise RuntimeError(f"Principal role {role} already exists outside this bootstrap")
        self.state = {
            "version": STATE_VERSION,
            "status": "initializing",
            "realm": self.client.realm,
            "catalog": self.catalog,
            "bootstrap_id": uuid.uuid4().hex,
            "config": self.config,
            "principal_client_ids": {},
        }
        self.save_state()
        self.initialize()

    def run(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self.state_file.parent, 0o700)
        lock_path = self.state_file.parent / "bootstrap.lock"
        with lock_path.open("a") as lock:
            os.chmod(lock_path, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX)
            self._run()


def main():
    parser = argparse.ArgumentParser(description="Bootstrap Polaris once; preserve Console-managed RBAC on restart")
    parser.add_argument(
        "--config", "-c", default=os.environ.get("POLARIS_RBAC_CONFIG", str(Path(__file__).with_name("rbac.yml")))
    )
    parser.add_argument("--polaris-url", default=os.environ.get("POLARIS_URL", "http://polaris:8181"))
    parser.add_argument("--realm", default=os.environ.get("POLARIS_REALM", "POLARIS"))
    parser.add_argument("--catalog", default=os.environ.get("POLARIS_CATALOG"))
    parser.add_argument("--root-client-id", default=os.environ.get("POLARIS_ROOT_CLIENT_ID", "root"))
    parser.add_argument("--root-client-secret", default=os.environ.get("POLARIS_ROOT_CLIENT_SECRET", "password"))
    parser.add_argument(
        "--credential-state-file",
        default=os.environ.get("POLARIS_CREDENTIAL_STATE_FILE", "/run/polaris-state/clients.env"),
    )
    parser.add_argument("--ready-file", default=os.environ.get("POLARIS_READY_FILE", "/run/polaris-state/ready"))
    args = parser.parse_args()
    client = PolarisClient(args.polaris_url, args.realm, args.root_client_id, args.root_client_secret)
    bootstrap = PolarisBootstrap(client, args.credential_state_file, args.ready_file, args.config, args.catalog)
    try:
        bootstrap.run()
    except (RuntimeError, OSError, ValueError, yaml.YAMLError) as exc:
        logger.error("Polaris initialization failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
