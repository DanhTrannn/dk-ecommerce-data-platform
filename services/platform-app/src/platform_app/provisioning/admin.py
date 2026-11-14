import os
import secrets
import stat
from pathlib import Path

import psycopg2
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from platform_app.api.core.config import get_settings
from platform_app.provisioning.state import atomic_private
from platform_app.api.db.base import Base
from platform_app.api import models  # noqa: F401
from database.seeds.seed_catalog import _seed_admin


def initialize_source(database_url, settings, schema_path):
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        raise ValueError("Admin bootstrap requires PostgreSQL")
    engine = create_engine(database_url)
    connection = psycopg2.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(783129004251)")
            tables = set(inspect(engine).get_table_names(schema="public"))
            if not tables:
                cursor.execute(Path(schema_path).read_text())
            elif not set(Base.metadata.tables).issubset(tables):
                raise RuntimeError("Existing source schema is incomplete; bootstrap will not migrate or reset it")
            factory = sessionmaker(engine, expire_on_commit=False)
            with factory.begin() as session:
                _seed_admin(session, settings)
    finally:
        connection.close()
        engine.dispose()


def prepare_storage():
    uid, gid = int(os.environ.get("PLATFORM_APP_UID", "1000")), int(os.environ.get("PLATFORM_APP_GID", "1000"))
    if uid <= 0 or gid < 0:
        raise ValueError("Platform runtime must use a non-root UID and a valid GID")
    for path in (Path("/run/access-secrets"), Path("/data/access-admin")):
        if path.is_symlink():
            raise RuntimeError("Storage directory must not be a symlink")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chown(path, uid, gid)
        path.chmod(0o700)
    key_file = Path("/run/access-secrets/exporter.key")
    if key_file.is_symlink():
        raise RuntimeError("Exporter signing key must not be a symlink")
    if not key_file.exists():
        atomic_private(key_file, secrets.token_urlsafe(48), uid=uid, gid=gid)
    elif not stat.S_ISREG(key_file.stat().st_mode) or len(key_file.read_bytes()) < 32:
        raise RuntimeError("Existing exporter signing key is invalid; restore it explicitly")
    else:
        key_file.chmod(0o600)
        os.chown(key_file, uid, gid)
    os.setgroups([])
    os.setgid(gid)
    os.setuid(uid)


def main():
    prepare_storage()
    settings = get_settings()
    root = Path(__file__).resolve()
    schema_path = next(
        (parent / "database/schema.sql" for parent in root.parents if (parent / "database/schema.sql").is_file()), None
    )
    if schema_path is None:
        raise RuntimeError("Source schema asset is missing")
    initialize_source(settings.database_url, settings, schema_path)
    print("Platform admin initialized; existing accounts and passwords retained")


if __name__ == "__main__":
    main()
