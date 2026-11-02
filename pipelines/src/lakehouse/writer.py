"""One local writer for the curated OLTP tables across CDC and batch drivers."""

import fcntl
import os
from contextlib import contextmanager


@contextmanager
def oltp_writer_lock():
    path = os.environ.get("OLTP_WRITER_LOCK", "/run/lakehouse-writer/oltp.lock")
    with open(path, "a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
