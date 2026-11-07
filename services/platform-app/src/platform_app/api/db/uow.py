import random
import time
from collections.abc import Callable
from typing import TypeVar

from psycopg2.errorcodes import DEADLOCK_DETECTED, LOCK_NOT_AVAILABLE, SERIALIZATION_FAILURE
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from platform_app.api.core.config import get_settings
from platform_app.api.core.errors import CONCURRENCY_RETRY_EXHAUSTED, AppError
from platform_app.api.db.session import SessionLocal

T = TypeVar("T")

# Retry only transaction-abort conditions; connection failures can leave the
# commit outcome unknown and must not replay non-idempotent work automatically.
_TRANSIENT_POSTGRES_CODES = {DEADLOCK_DETECTED, SERIALIZATION_FAILURE, LOCK_NOT_AVAILABLE}


def _is_transient(error: OperationalError) -> bool:
    return getattr(error.orig, "pgcode", None) in _TRANSIENT_POSTGRES_CODES


def run_in_transaction(work: Callable[[Session], T]) -> T:
    """Run ``work`` in a READ COMMITTED transaction with bounded retry on
    deadlock / lock-wait-timeout. ``work`` must be side-effect-free outside the
    session so it can be retried safely."""
    settings = get_settings()
    attempts = settings.checkout_max_retries
    last_error: OperationalError | None = None

    for attempt in range(attempts):
        session = SessionLocal()
        try:
            result = work(session)
            session.commit()
            return result
        except OperationalError as error:
            session.rollback()
            if not _is_transient(error):
                raise
            last_error = error
            sleep_s = (2**attempt) * 0.05 + random.uniform(0, 0.05)
            time.sleep(sleep_s)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    raise AppError(
        CONCURRENCY_RETRY_EXHAUSTED,
        "Hệ thống đang bận, vui lòng thử lại.",
        status_code=409,
    ) from last_error
