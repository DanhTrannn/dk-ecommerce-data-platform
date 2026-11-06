#!/usr/bin/env python3
"""Converge configured MinIO users in place; preserve other identities."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid
from contextlib import closing
from pathlib import Path
from urllib.parse import quote, urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger("minio-init")
IDENTITIES = {
    "lakehouse-polaris": "POLARIS",
    "lakehouse-spark": "SPARK",
    "lakehouse-flink": "FLINK",
    "lakehouse-trino": "TRINO",
    "lakehouse-uploader": "UPLOADER",
}
AUTH_ERRORS = {"SignatureDoesNotMatch", "InvalidAccessKeyId"}


def canonical(value, key=""):
    """IAM statement, action and resource order do not change permission meaning."""
    if key in {"Action", "NotAction", "Resource", "NotResource", "Statement"} and not isinstance(value, list):
        value = [value]
    if isinstance(value, dict):
        return {k: canonical(v, k) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return sorted((canonical(v) for v in value), key=lambda v: json.dumps(v, sort_keys=True))
    return value


class MinioSync:
    def __init__(self, env=None, mc_binary="mc"):
        self.env = dict(os.environ if env is None else env)
        self.mc_binary = mc_binary
        self.endpoint = self.env.get("MINIO_ENDPOINT", "http://minio:9000")
        self.bucket = self.env.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
        self.policy_dir = Path(self.env.get("POLICY_DIR", str(Path(__file__).with_name("policies"))))
        self.region = self.env.get("AWS_REGION", "us-east-1")
        self.credentials = {}
        self.desired = {}
        self.counts = {
            k: 0
            for k in (
                "policies_created",
                "policies_updated",
                "users_created",
                "secrets_updated",
                "users_enabled",
                "policies_attached",
                "policies_detached",
                "groups_detached",
                "buckets_created",
                "bucket_policies_removed",
            )
        }

    def validate(self):
        endpoint = urlsplit(self.endpoint)
        if (
            endpoint.scheme not in ("http", "https")
            or not endpoint.netloc
            or endpoint.username
            or endpoint.password
            or endpoint.query
            or endpoint.fragment
            or endpoint.path not in ("", "/")
        ):
            raise RuntimeError("MINIO_ENDPOINT must be an HTTP(S) server URL without credentials or a path")
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket):
            raise RuntimeError("Invalid MINIO_LAKEHOUSE_BUCKET")
        for key in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"):
            if not self.env.get(key) or any(c in self.env[key] for c in "\r\n\0"):
                raise RuntimeError(f"Missing or invalid {key}")
        if self.env["MINIO_ROOT_USER"] in IDENTITIES:
            raise RuntimeError("The root identity cannot be one of the managed workload users")
        for name, prefix in IDENTITIES.items():
            access_var, secret_var = f"MINIO_{prefix}_ACCESS_KEY", f"MINIO_{prefix}_SECRET_KEY"
            access, secret = self.env.get(access_var), self.env.get(secret_var)
            if access != name or not secret or len(secret) < 8 or any(c in secret for c in "\r\n\0"):
                raise RuntimeError(
                    f"Invalid {access_var}/{secret_var}; expected access key {name} and a secret of at least 8 characters"
                )
            self.credentials[name] = secret
            raw = (self.policy_dir / f"{name}.json").read_text().replace("__MINIO_BUCKET__", self.bucket)
            if "__MINIO_BUCKET__" in raw:
                raise RuntimeError(f"Unresolved bucket placeholder in {name}")
            policy = json.loads(raw)
            if (
                not isinstance(policy, dict)
                or policy.get("Version") != "2012-10-17"
                or not isinstance(policy.get("Statement"), list)
                or not policy["Statement"]
            ):
                raise RuntimeError(f"Invalid IAM policy document for {name}")
            location_allowed = False
            for statement in policy["Statement"]:
                if (
                    not isinstance(statement, dict)
                    or statement.get("Effect") not in ("Allow", "Deny")
                    or not statement.get("Action")
                    or not statement.get("Resource")
                ):
                    raise RuntimeError(f"Incomplete IAM statement for {name}")
                actions = statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]]
                resources = (
                    statement["Resource"] if isinstance(statement["Resource"], list) else [statement["Resource"]]
                )
                if any(not isinstance(v, str) for v in actions + resources):
                    raise RuntimeError(f"IAM actions/resources for {name} must be strings")
                if any(
                    r != f"arn:aws:s3:::{self.bucket}" and not r.startswith(f"arn:aws:s3:::{self.bucket}/")
                    for r in resources
                ):
                    raise RuntimeError(f"Policy {name} references storage outside the configured bucket")
                if (
                    statement["Effect"] == "Allow"
                    and "s3:GetBucketLocation" in actions
                    and f"arn:aws:s3:::{self.bucket}" in resources
                ):
                    location_allowed = True
            if not location_allowed:
                raise RuntimeError(f"Policy {name} must allow GetBucketLocation for credential verification")
            self.desired[name] = policy

    def mc(self, *args, input_text=None):
        result = subprocess.run(
            [self.mc_binary, "--json", "--no-color", *args],
            input=input_text,
            text=True,
            capture_output=True,
            env=self.mc_env,
            timeout=45,
        )
        records = []
        for line in result.stdout.splitlines():
            start = line.find("{")
            if start >= 0:
                try:
                    record = json.loads(line[start:])
                except ValueError:
                    raise RuntimeError("MinIO CLI returned invalid JSON") from None
                if not isinstance(record, dict):
                    raise RuntimeError("MinIO CLI returned an unexpected response shape")
                records.append(record)
            elif line.strip() and not line.startswith(("Enter Access Key:", "Enter Secret Key:")):
                raise RuntimeError("MinIO CLI returned unexpected non-JSON output")
        if result.returncode or any(r.get("status") == "error" for r in records):
            # Do not echo CLI output: a failed credential command can include input.
            raise RuntimeError(f"MinIO command failed: {' '.join(args[:3])}")
        return records

    def one(self, *args):
        rows = self.mc(*args)
        if len(rows) != 1:
            raise RuntimeError(f"Expected one MinIO JSON record for {' '.join(args[:3])}")
        return rows[0]

    def s3(self, access, secret):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=3,
                read_timeout=5,
                retries={"max_attempts": 1},
            ),
        )

    def authenticate(self, name):
        try:
            with closing(self.s3(name, self.credentials[name])) as client:
                client.get_bucket_location(Bucket=self.bucket)
            return True
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in AUTH_ERRORS:
                return False
            raise RuntimeError(
                f"S3 credential verification for {name} failed with {code}; secret was not reset"
            ) from None
        except BotoCoreError:
            raise RuntimeError(
                f"S3 credential verification for {name} could not connect; secret was not reset"
            ) from None

    def policy(self, name):
        row = self.one("admin", "policy", "info", "local", name)
        result = row.get("policyInfo", {}).get("Policy")
        if not isinstance(result, dict):
            raise RuntimeError(f"Missing policy JSON for {name}")
        return result

    def user(self, name):
        row = self.one("admin", "user", "info", "local", name)
        if row.get("accessKey") != name or row.get("userStatus") not in ("enabled", "disabled"):
            raise RuntimeError(f"Invalid user metadata for {name}")
        return row

    @staticmethod
    def attached(info):
        return {p.strip() for p in (info.get("policyName") or "").split(",") if p.strip()}

    @staticmethod
    def groups(info):
        result = []
        for membership in info.get("memberOf") or []:
            name = (
                membership
                if isinstance(membership, str)
                else membership.get("name")
                if isinstance(membership, dict)
                else None
            )
            if not isinstance(name, str) or not name:
                raise RuntimeError("Invalid group membership metadata")
            result.append(name)
        return result

    def check_policy_scope(self, name):
        row = self.one("admin", "policy", "entities", "local", "--policy", name)
        result = row.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"Missing policy association metadata for {name}")
        mappings = result.get("policyMappings") or []
        if not isinstance(mappings, list):
            raise RuntimeError(f"Invalid policy association metadata for {name}")
        for mapping in mappings:
            if set(mapping.get("users") or []) - IDENTITIES.keys():
                raise RuntimeError(
                    f"Policy {name} is used by unmanaged users; resolve sharing explicitly before changing it"
                )
            for group in mapping.get("groups") or []:
                members = self.one("admin", "group", "info", "local", group).get("members") or []
                if set(members) - IDENTITIES.keys():
                    raise RuntimeError(f"Policy {name} is shared with an unmanaged group; resolve sharing explicitly")

    def preflight(self, changed, files):
        temporary = []
        try:
            for name in changed:
                temporary_name = "tlcn-verify-" + uuid.uuid4().hex[:12] + "-" + name
                temporary.append(temporary_name)
                self.mc("admin", "policy", "create", "local", temporary_name, str(files[name]))
        finally:
            for name in temporary:
                try:
                    current = {r["policy"] for r in self.mc("admin", "policy", "list", "local")}
                    if name in current:
                        self.mc("admin", "policy", "remove", "local", name)
                except RuntimeError:
                    raise RuntimeError(
                        "Unable to clean up a private preflight policy; initialization did not complete"
                    ) from None

    def bucket_private(self):
        with closing(self.s3(self.env["MINIO_ROOT_USER"], self.env["MINIO_ROOT_PASSWORD"])) as root:
            try:
                root.head_bucket(Bucket=self.bucket)
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                    raise
                root.create_bucket(Bucket=self.bucket)
                self.counts["buckets_created"] += 1
            try:
                root.get_bucket_policy(Bucket=self.bucket)
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "NoSuchBucketPolicy":
                    raise
            else:
                root.delete_bucket_policy(Bucket=self.bucket)
                self.counts["bucket_policies_removed"] += 1

    def converge(self, directory):
        users = {r["accessKey"] for r in self.mc("admin", "user", "list", "local")}
        policies = {r["policy"] for r in self.mc("admin", "policy", "list", "local")}
        changed = [
            name
            for name in IDENTITIES
            if name not in policies or canonical(self.policy(name)) != canonical(self.desired[name])
        ]
        for name in changed:
            if name in policies:
                self.check_policy_scope(name)
        files = {}
        for name, policy in self.desired.items():
            files[name] = directory / f"{name}.json"
            files[name].write_text(json.dumps(policy))
        self.preflight(changed, files)
        self.bucket_private()
        for name in changed:
            self.mc("admin", "policy", "create", "local", name, str(files[name]))
            self.counts["policies_updated" if name in policies else "policies_created"] += 1
            log.info("Applied managed policy %s", name)
        for name in IDENTITIES:
            if name not in users:
                self.mc("admin", "user", "add", "local", input_text=f"{name}\n{self.credentials[name]}\n")
                self.counts["users_created"] += 1
                log.info("Created managed user %s", name)
            info = self.user(name)
            if name not in self.attached(info):
                self.mc("admin", "policy", "attach", "local", name, "--user", name)
                self.counts["policies_attached"] += 1
            for extra in self.attached(info) - {name}:
                self.mc("admin", "policy", "detach", "local", extra, "--user", name)
                self.counts["policies_detached"] += 1
            for group in self.groups(info):
                self.mc("admin", "group", "remove", "local", group, name)
                self.counts["groups_detached"] += 1
            if info["userStatus"] == "disabled":
                self.mc("admin", "user", "enable", "local", name)
                self.counts["users_enabled"] += 1
            if not self.authenticate(name):
                self.mc("admin", "user", "add", "local", input_text=f"{name}\n{self.credentials[name]}\n")
                self.counts["secrets_updated"] += 1
                if not self.authenticate(name):
                    raise RuntimeError(f"Managed credentials for {name} still cannot authenticate after update")
                log.info("Updated configured secret for %s", name)
        for name in IDENTITIES:
            info = self.user(name)
            if self.attached(info) != {name} or self.groups(info) or info["userStatus"] != "enabled":
                raise RuntimeError(f"Managed user {name} has unexpected effective policy attachments")
            if canonical(self.policy(name)) != canonical(self.desired[name]) or not self.authenticate(name):
                raise RuntimeError(f"Managed policy/credential verification failed for {name}")
        extras = sorted((users | policies) - IDENTITIES.keys())
        extras = [name for name in extras if name.startswith("lakehouse-")]
        if extras:
            log.warning("Unmanaged/retired lakehouse-* names preserved: %s", extras)
        log.info("MinIO synchronization complete: %s", json.dumps(self.counts, sort_keys=True))
        return self.counts

    def run(self):
        self.validate()  # No remote changes before every local input has been checked.
        endpoint = urlsplit(self.endpoint)
        root_url = f"{endpoint.scheme}://{quote(self.env['MINIO_ROOT_USER'], safe='')}:{quote(self.env['MINIO_ROOT_PASSWORD'], safe='')}@{endpoint.netloc}"
        with tempfile.TemporaryDirectory(prefix="minio-init-") as folder:
            directory = Path(folder)
            self.mc_env = {
                **os.environ,
                "MC_HOST_local": root_url,
                "MC_CONFIG_DIR": str(directory / "mc"),
                "MC_DISABLE_PAGER": "1",
            }
            for attempt in range(30):
                try:
                    self.mc("admin", "info", "local")
                    break
                except RuntimeError:
                    if attempt == 29:
                        raise
                    time.sleep(1)
            return self.converge(directory)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s [minio-init] %(message)s")
    os.umask(0o077)
    try:
        MinioSync().run()
    except (RuntimeError, OSError, ValueError, ClientError, BotoCoreError, subprocess.TimeoutExpired) as exc:
        log.error("MinIO initialization failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
