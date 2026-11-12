"""Local admin-only BI PoC; warehouse writes remain denied by Trino/Polaris."""

import os

from sqlalchemy.engine import URL

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
if len(SECRET_KEY) < 32:
    raise ValueError("Set SUPERSET_SECRET_KEY to at least 32 random characters")
if not os.environ.get("SUPERSET_DB_PASSWORD"):
    raise ValueError("Set SUPERSET_DB_PASSWORD before starting BI")
SQLALCHEMY_DATABASE_URI = URL.create(
    "postgresql+psycopg2",
    username=os.environ.get("SUPERSET_DB_USER", "superset"),
    password=os.environ["SUPERSET_DB_PASSWORD"],
    host=os.environ.get("SUPERSET_DB_HOST", "postgres"),
    port=5432,
    database=os.environ.get("SUPERSET_DB_NAME", "superset"),
).render_as_string(hide_password=False)
SQLALCHEMY_TRACK_MODIFICATIONS = False
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = os.environ.get("SUPERSET_COOKIE_SECURE", "false").lower() == "true"
ENABLE_PROXY_FIX = os.environ.get("SUPERSET_PROXY_FIX", "false").lower() == "true"
TALISMAN_ENABLED = False  # Local HTTP only; shared/public deployment is outside this PoC.
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": False, "GLOBAL_ASYNC_QUERIES": False, "THUMBNAILS_SQLA_LISTENERS": False}
CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
APP_NAME = "Retail Lakehouse BI"
ROW_LIMIT = 10000
SQL_MAX_ROW = 10000
SUPERSET_WEBSERVER_TIMEOUT = 120
