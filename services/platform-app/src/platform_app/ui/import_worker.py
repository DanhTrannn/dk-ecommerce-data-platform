"""Import a checksummed job SQL snapshot; never expose database error payloads."""

import hashlib
import shutil
import sys
import tempfile
from pathlib import Path

from psycopg2 import Error as DriverError
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from platform_app.api.db.generated_sql import read_metadata
from platform_app.api.db.import_dataset import import_dataset
from platform_app.ui.import_store import ImportStore, configured_target, target_for
from platform_app.ui.job_store import JobStore, JobStoreError


def run_import(job_dir: Path, attempt_id: str, engine=None) -> int:
    job_dir = Path(job_dir).absolute()
    store = ImportStore(JobStore(job_dir.parent))
    record = None
    owned_engine = False
    committed = False
    commit_started = False

    def committing(connection):
        nonlocal commit_started
        commit_started = True

    try:
        with store.lock(execution=True):
            record = store.claim(job_dir.name, attempt_id)
            target = target_for(engine.url) if engine is not None else configured_target()
            if target["id"] != record["target"]["id"]:
                raise JobStoreError("Cấu hình database đã thay đổi; tạo lần nạp mới với đích hiện tại.")
            path, artifact = store.sql_artifact(job_dir.name)
            if artifact["path"] != record["artifact"] or artifact["sha256"] != record["sql_sha256"]:
                raise JobStoreError("SQL artifact đã thay đổi so với yêu cầu nạp.")
            # Hash the private snapshot, not a path that may be replaced between
            # verification and import. Only generated job SQL can reach this step.
            with tempfile.TemporaryDirectory(prefix="tlcn-import-") as temporary:
                snapshot = Path(temporary) / "dataset.sql"
                with path.open("rb") as source, snapshot.open("wb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
                with snapshot.open("rb") as source:
                    checksum = hashlib.file_digest(source, "sha256").hexdigest()
                if checksum != record["sql_sha256"]:
                    raise JobStoreError("Checksum SQL không khớp; sinh lại job hoặc khôi phục artifact gốc.")
                with snapshot.open(encoding="utf-8", newline="") as source:
                    metadata = read_metadata(source)
                if metadata["logical_identity"] != record["dataset_identity"]:
                    raise JobStoreError("Dataset identity của SQL không khớp job.")
                if engine is None:
                    from platform_app.api.db.session import engine

                    owned_engine = True
                event.listen(engine, "commit", committing)
                try:
                    receipt = import_dataset(engine, snapshot)
                finally:
                    event.remove(engine, "commit", committing)
                committed = True
            store.finish(record, "succeeded", receipt=receipt)
            return 0
    except BlockingIOError:
        return 1  # Another worker owns execution; never change its record.
    except Exception as error:
        if record is None:
            return 1
        status, code = "failed", type(error).__name__
        message = "Nạp thất bại; kiểm tra cấu hình/schema và thử lại. Chi tiết SQL/credential không được lưu."
        if committed:
            status, code = "unknown", "receipt_write_failed"
            message = "Database đã commit nhưng chưa lưu được kết quả đầy đủ; cần xác minh trước khi nạp lại."
        elif isinstance(error, JobStoreError):
            message = str(error)
        elif type(error) is ValueError and str(error) == "Dataset already imported; no rows were changed":
            status, code = "already_present", "dataset_present"
            message = "Dataset đã có trong database; lần này không ghi thêm dữ liệu."
        elif isinstance(error, (DBAPIError, DriverError)):
            original = error.orig if isinstance(error, DBAPIError) else error
            state = getattr(original, "pgcode", None)
            if commit_started and (not state or state.startswith("08")):
                status, code = "unknown", "connection_outcome_unknown"
                message = "Kết nối database bị gián đoạn; cần xác minh kết quả trước khi nạp lại."
        elif not isinstance(error, (ValueError, OSError, SQLAlchemyError, KeyError)):
            status, code = "unknown", "outcome_unknown"
            message = "Không xác định được kết quả tác vụ; kiểm tra database trước khi nạp lại."
        # A post-commit receipt-write failure is also ambiguous. Do not overwrite
        # a persisted success or attempt an automatic database retry.
        try:
            store.finish(record, status, code=code, message=message)
        except Exception:
            pass  # The next reconciliation marks an abandoned record unknown.
        return 1
    finally:
        if owned_engine:
            engine.dispose()


def main():
    if len(sys.argv) != 3:
        raise SystemExit(2)
    raise SystemExit(run_import(Path(sys.argv[1]), sys.argv[2]))


if __name__ == "__main__":
    main()
