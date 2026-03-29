"""
pipeline/resources/__init__.py
Shared resources: MySQL engine, dbt CLI resource, path constants.
Single-DB setup: semua layer (bronze/silver/gold) dalam astraworld_dw.
"""
import os
from pathlib import Path

from sqlalchemy import create_engine

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _mysql_url(db: str) -> str:
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    user = os.environ["MYSQL_USER"]
    pw   = os.environ["MYSQL_PASS"]
    return (
        f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"
        "?charset=utf8mb4&ssl_disabled=true"
    )


def get_engine():
    """Single engine – astraworld_dw (semua layer)"""
    db = os.environ.get("DB_NAME", "astraworld_dw")
    return create_engine(_mysql_url(db), pool_pre_ping=True)


# Backward-compat aliases (dipakai di bronze.py untuk Sling ingest)
get_astraworld_dw_engine       = get_engine

# Path constants
CSV_DROP_DIR     = PROJECT_ROOT / "data" / "dummy"
SLING_CONFIG_DIR = PROJECT_ROOT / "sling_config"
