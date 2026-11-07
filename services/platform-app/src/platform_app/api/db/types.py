import uuid

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.types import TypeDecorator


class GUID(TypeDecorator):
    """Store PostgreSQL UUID; accept canonical strings and uuid.UUID values."""

    impl = UUID(as_uuid=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
