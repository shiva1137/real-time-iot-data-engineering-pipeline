"""
Database connection pool for FastAPI (Topic 7).

Read-only PostgreSQL connections. Connection pooling for performance.
"""

import os
import logging
from contextlib import contextmanager
from typing import Generator, Tuple, Any

import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)

# Simple connection pool (min 2, max 10)
_pool: pool.SimpleConnectionPool = None


def init_pool() -> None:
    global _pool
    if _pool is not None:
        return
    _pool = pool.SimpleConnectionPool(
        minconn=2,
        maxconn=10,
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DATABASE", os.getenv("POSTGRES_DB", "iot_analytics")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        connect_timeout=10,
    )
    logger.info("Database connection pool initialized")


@contextmanager
def get_connection() -> Generator:
    """Yield a connection from the pool. Caller must not hold long."""
    global _pool
    if _pool is None:
        init_pool()
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)


def check_connection() -> Tuple[bool, str]:
    """Return (success, message) for health check."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, "ok"
    except Exception as e:
        return False, str(e)


def get_latest_data_timestamp() -> Tuple[bool, Any]:
    """Return (success, latest window_start from real_time_aggregates)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(window_start) FROM real_time_aggregates"
                )
                row = cur.fetchone()
                return True, row[0] if row else None
    except Exception:
        return False, None
