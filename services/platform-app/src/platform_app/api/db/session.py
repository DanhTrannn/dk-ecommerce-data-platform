from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from platform_app.api.core.config import get_settings

settings = get_settings()

database_url = settings.database_url
if make_url(database_url).get_backend_name() != "postgresql":
    raise ValueError("API_DATABASE_URL must use PostgreSQL for the source schema")

engine = create_engine(
    database_url,
    isolation_level="READ COMMITTED",
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={"options": "-c timezone=UTC -c search_path=public"},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
