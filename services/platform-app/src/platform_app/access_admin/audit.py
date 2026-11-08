"""Local serialized, durable mutation journal; contains no tokens or credentials."""

import fcntl
import json
import os
from collections import deque
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path


class Audit:
    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def lock(self):
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path.parent.chmod(0o700)
        with self.path.with_suffix(".lock").open("a+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def append(self, record):
        if self.path.is_symlink():
            raise OSError("Invalid audit path")
        descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "a") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(
                json.dumps({**record, "recorded_at": datetime.now(UTC).isoformat()}, ensure_ascii=False) + "\n"
            )
            output.flush()
            os.fsync(output.fileno())

    def recent(self):
        if not self.path.exists():
            return []
        with self.path.open() as source:
            return [json.loads(line) for line in reversed(deque(source, maxlen=50))]
