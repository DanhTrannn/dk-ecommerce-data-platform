"""Independent local writer locks for batch and log-mart publication."""

import fcntl
import os
from contextlib import contextmanager


@contextmanager
def _writer_lock(path):
    with open(path, "a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def oltp_writer_lock():
    return _writer_lock(os.environ.get("OLTP_WRITER_LOCK", "/run/lakehouse-writer/oltp.lock"))


def logs_writer_lock():
    return _writer_lock(
        os.path.join(
            os.path.dirname(os.environ.get("OLTP_WRITER_LOCK", "/run/lakehouse-writer/oltp.lock")), "logs.lock"
        )
    )
