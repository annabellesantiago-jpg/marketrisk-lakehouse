import os
from superset.stats_logger import DummyStatsLogger

# Metadata database — separate 'superset' DB in the shared Postgres container
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.environ['SUPERSET_DB_USER']}:{os.environ['SUPERSET_DB_PASSWORD']}"
    f"@{os.environ.get('SUPERSET_DB_HOST', 'postgres')}:{os.environ.get('SUPERSET_DB_PORT', '5432')}"
    f"/{os.environ.get('SUPERSET_DB_NAME', 'superset')}"
)

# Secret key — required, used for session cookies and CSRF
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Redis cache
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": os.environ.get("REDIS_URL", "redis://redis:6379/0"),
}

# Data query cache (caches SQL results)
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": os.environ.get("REDIS_URL", "redis://redis:6379/1"),
}

# Prevent Superset from phoning home
STATS_LOGGER = DummyStatsLogger()
TALISMAN_ENABLED = False

# Feature flags
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}