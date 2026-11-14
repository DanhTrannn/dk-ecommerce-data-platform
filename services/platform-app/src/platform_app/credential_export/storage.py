import json
import os
import shlex
import stat
import uuid
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from platform_app.access_admin.audit import Audit
from platform_app.access_admin.schemas import editable_name
from platform_app.credential_export.auth import canonical_digest
from platform_app.provisioning.state import atomic_private, read_credentials, read_json


class ExportSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='CREDENTIAL_EXPORT_', extra='ignore')
    sources_dir: Path = Path('/run/access-secrets/principals')
    key_file: Path = Path('/run/access-secrets/exporter.key')
    state_dir: Path = Path('/run/export-state')
    outputs_root: Path = Path('/run/polaris-output')
    airflow_uid: int = Field(default=50000, ge=0)


class ExportError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


def inspect_path(path):
    path = Path(path).absolute()
    for part in [*reversed(path.parents), path]:
        if part.is_symlink():
            raise OSError('Symlink paths are forbidden')
    if path.exists():
        info = path.stat()
        if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
            raise OSError('Unsupported file type')
        return {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
                'type': 'dir' if path.is_dir() else 'file', 'inode': info.st_ino, 'mtime': info.st_mtime_ns}
    return None


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class ExportStore:
    def __init__(self, settings):
        self.settings = settings
        self.audit = Audit(settings.state_dir / 'audit.jsonl')
        self.manifest = settings.state_dir / 'assignments.json'
        self.workloads = {}
        for name, label, uid, gid, prefix, volume, hint in [
            ('batch-airflow', 'Batch / Airflow', settings.airflow_uid, 0, 'BATCH', 'polaris-batch-airflow-credentials', 'Job batch kế tiếp; tiến trình giữ credential cần khởi động lại'),
            ('flink', 'Flink', 9999, 9999, 'FLINK', 'polaris-flink-credentials', 'Khởi động lại hoặc submit lại Flink job'),
            ('trino-reader', 'Trino reader', 1000, 1000, 'TRINO', 'polaris-trino-reader-credentials', 'Khởi động lại Trino reader'),
            ('trino-writer', 'Trino writer', 1000, 1000, 'BATCH', 'polaris-trino-writer-credentials', 'Khởi động lại Trino writer'),
        ]:
            self.workloads[name] = dict(label=label, uid=uid, gid=gid, client_id_var=f'POLARIS_{prefix}_CLIENT_ID',
                                       client_secret_var=f'POLARIS_{prefix}_CLIENT_SECRET', volume=volume, reload_hint=hint)

    def _preflight(self):
        for path in [self.settings.sources_dir, self.settings.outputs_root, self.settings.state_dir,
                     self.manifest, self.audit.path, self.audit.path.with_suffix('.lock')]:
            inspect_path(path)
        for name in self.workloads:
            inspect_path(self.settings.outputs_root / name)
            inspect_path(self.settings.outputs_root / name / 'clients.env')

    def _source(self, name):
        if not editable_name(name):
            raise OSError('Invalid source')
        path = self.settings.sources_dir / f'{name}.json'
        metadata = inspect_path(path)
        if not metadata or metadata['type'] != 'file' or metadata['mode'] != 0o600:
            raise OSError('Invalid credential source')
        value = read_json(path)
        if not isinstance(value, dict):
            raise OSError('Invalid credential source')
        for key in ('clientId', 'clientSecret'):
            item = value.get(key)
            if not isinstance(item, str) or not item or len(item) > 8192 or any(ord(c) < 32 or ord(c) > 126 for c in item):
                raise OSError('Invalid credential source')
        return value

    def _manifest(self):
        value = read_json(self.manifest) or {}
        if not isinstance(value, dict):
            raise OSError('Invalid exporter state')
        return value

    def _snapshot(self):
        self._preflight()
        manifest = self._manifest()
        private = {'manifest': manifest, 'manifest_metadata': inspect_path(self.manifest),
                   'outputs_root': inspect_path(self.settings.outputs_root), 'targets': {}}
        result = []
        for name, spec in self.workloads.items():
            directory = self.settings.outputs_root / name
            path = directory / 'clients.env'
            metadata = inspect_path(path)
            directory_metadata = inspect_path(directory)
            content = path.read_bytes() if metadata else None
            digest = canonical_digest(content.hex()) if content is not None else None
            values = {}
            if metadata:
                try:
                    values = read_credentials(path)
                except Exception:
                    pass
            client_id = values.get(spec['client_id_var'])
            secret = values.get(spec['client_secret_var'])
            principal = None
            source = 'external' if metadata else 'missing'
            entry = manifest.get(name, {})
            if (metadata and isinstance(entry, dict) and entry.get('digest') == digest
                    and entry.get('client_id') == client_id and isinstance(entry.get('principal'), str)
                    and editable_name(entry['principal'])):
                principal = entry['principal']
                source = 'console'
            elif metadata and client_id and secret:
                for candidate in sorted(self.settings.sources_dir.glob('*.json')):
                    if not editable_name(candidate.stem):
                        continue
                    try:
                        credential = self._source(candidate.stem)
                    except Exception:
                        continue
                    if credential['clientId'] == client_id and credential['clientSecret'] == secret:
                        principal = candidate.stem
                        break
            permission_ok = bool(metadata and directory_metadata and metadata['type'] == 'file'
                and directory_metadata['type'] == 'dir' and metadata['mode'] == 0o600 and directory_metadata['mode'] == 0o700
                and all(item['uid'] == spec['uid'] and item['gid'] == spec['gid'] for item in [metadata, directory_metadata]))
            private['targets'][name] = {'digest': digest, 'file': metadata, 'directory': directory_metadata}
            result.append(dict(id=name, label=spec['label'], configured=bool(client_id and secret), file_present=bool(metadata), principal=principal,
                               client_id=client_id, source=source, permission_ok=permission_ok,
                               reload_hint=spec['reload_hint'], volume=spec['volume']))
        return {'revision': canonical_digest(private), 'workloads': result}

    def snapshot(self):
        try:
            self._preflight()
            with self.audit.lock():
                return self._snapshot()
        except Exception:
            raise ExportError(503, 'Credential state is unavailable') from None

    def apply(self, assignment, actor):
        record = dict(operation_id=uuid.uuid4().hex, actor=str(actor), action=assignment.action,
                      workload=assignment.workload, principal=assignment.principal)
        started = False
        try:
            self._preflight()
            with self.audit.lock():
                before = self._snapshot()
                if before['revision'] != assignment.revision:
                    raise ExportError(409, 'Workload state changed; reload before applying')
                spec = self.workloads[assignment.workload]
                directory = self.settings.outputs_root / assignment.workload
                path = directory / 'clients.env'
                manifest = self._manifest()
                content = None
                noop = False
                if assignment.action == 'assign':
                    credential = self._source(assignment.principal)
                    if canonical_digest(credential) != assignment.credential_fingerprint:
                        raise ExportError(409, 'Saved credential changed; reload before applying')
                    expected = {spec['client_id_var']: credential['clientId'], spec['client_secret_var']: credential['clientSecret']}
                    existing = None
                    if path.exists():
                        try:
                            existing = read_credentials(path)
                        except Exception:
                            pass
                    same_credentials = existing == expected
                    safe_lines = {f'{key}={shlex.quote(value)}' for key, value in expected.items()}
                    noop = same_credentials and all(
                        line.strip().removeprefix('export ') in safe_lines
                        for line in path.read_text().splitlines() if line.strip() and not line.lstrip().startswith('#')
                    )
                    if path.exists() and not noop and not assignment.confirm_replace:
                        raise ExportError(409, 'Confirm replacement of the existing workload credential')
                    content = ''.join(f'{key}={shlex.quote(value)}\n' for key, value in expected.items())
                self.audit.append({**record, 'status': 'started'})
                started = True
                if assignment.action == 'assign':
                    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
                    fsync_directory(directory.parent)
                    if not noop:
                        atomic_private(path, content, spec['uid'], spec['gid'])
                    os.chmod(path, 0o600)
                    os.chown(path, spec['uid'], spec['gid'])
                    os.chmod(directory, 0o700)
                    os.chown(directory, spec['uid'], spec['gid'])
                    with path.open('rb') as handle:
                        os.fsync(handle.fileno())
                    fsync_directory(directory)
                    manifest[assignment.workload] = dict(principal=assignment.principal, client_id=credential['clientId'],
                        digest=canonical_digest(path.read_bytes().hex()))
                else:
                    noop = not path.exists() and assignment.workload not in manifest
                    if path.exists():
                        path.unlink()
                        fsync_directory(directory)
                    manifest.pop(assignment.workload, None)
                atomic_private(self.manifest, json.dumps(manifest, sort_keys=True))
                after = self._snapshot()
                actual = next(row for row in after['workloads'] if row['id'] == assignment.workload)
                if assignment.action == 'assign':
                    if read_credentials(path) != expected or not actual['permission_ok'] or actual['principal'] != assignment.principal:
                        raise OSError('Readback failed')
                elif path.exists() or assignment.workload in self._manifest():
                    raise OSError('Readback failed')
                status = 'noop' if noop else 'verified'
                self.audit.append({**record, 'status': status})
                return {**after, 'status': status, 'operation_id': record['operation_id'], 'reload_required': True}
        except ExportError:
            raise
        except Exception:
            if started:
                try:
                    self.audit.append({**record, 'status': 'unknown'})
                except Exception:
                    pass
            raise ExportError(503, 'Credential operation could not be verified; inspect state before retrying') from None
