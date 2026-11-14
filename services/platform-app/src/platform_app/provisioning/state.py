"""Private atomic bootstrap state and consumer-specific credential files."""

import json
import os
import shlex
import stat
import tempfile
from pathlib import Path

from platform_app.provisioning.client import ProvisionError


def private_directory(path):
    path = Path(path)
    if path.is_symlink():
        raise ProvisionError("Private directory must not be a symlink")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def atomic_private(path, content, uid=None, gid=None):
    path = Path(path)
    if path.is_symlink():
        raise ProvisionError("Private file must not be a symlink")
    parent = private_directory(path.parent)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=parent, delete=False) as output:
            temporary = Path(output.name)
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
            os.fchmod(output.fileno(), 0o600)
            if uid is not None:
                os.fchown(output.fileno(), uid, gid)
        os.replace(temporary, path)
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


def read_json(path):
    path = Path(path)
    if not path.exists():
        return None
    if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ProvisionError("Private state must be a regular 0600 file")
    return json.loads(path.read_text())


def save_json(path, value):
    atomic_private(path, json.dumps(value, sort_keys=True))


def read_credentials(path):
    path = Path(path)
    if not path.exists():
        return {}
    if path.is_symlink():
        raise ProvisionError("Credential file must not be a symlink")
    result = {}
    for line in path.read_text().splitlines():
        line = line.removeprefix("export ").strip()
        if not line or line.startswith("#"):
            continue
        fields = shlex.split(line, comments=True)
        if len(fields) != 1 or "=" not in fields[0]:
            raise ProvisionError("Credential file is malformed")
        key, value = fields[0].split("=", 1)
        result[key] = value
    return result


def export_credentials(output, credential, mapping):
    directory = private_directory(output["directory"])
    path = directory / "clients.env"
    expected = {
        mapping["client_id_var"]: credential["clientId"],
        mapping["client_secret_var"]: credential["clientSecret"],
    }
    if path.exists():
        existing = read_credentials(path)
        if any(existing.get(key) != value for key, value in expected.items()):
            raise ProvisionError("Existing consumer credentials differ; restore them explicitly, no automatic rotation")
        os.chmod(path, 0o600)
        os.chown(path, output["uid"], output["gid"])
    else:
        content = "".join(f"{key}={shlex.quote(value)}\n" for key, value in expected.items())
        atomic_private(path, content, output["uid"], output["gid"])
    os.chown(directory, output["uid"], output["gid"])
