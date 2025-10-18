"""
Shared PostgreSQL connection helpers for the Speed Limit tooling.

All scripts default to the production database credentials but can be
overridden through standard PostgreSQL environment variables:
PGHOST, PGPORT, PGDATABASE, PGUSER, and PGPASSWORD. A full DATABASE_URL
style connection string is also supported via the SPEEDLIMIT_DATABASE_URL
environment variable to make local testing easier.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from threading import Lock
from typing import Iterator, Optional

import psycopg
from psycopg.rows import dict_row, RowFactory
try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - handled lazily when pooling is requested
    ConnectionPool = None  # type: ignore[assignment]



_POOL: Optional["ConnectionPool"] = None
_POOL_LOCK: Lock = Lock()


def _env_int(name: str, default: int) -> int:
    """Return an integer environment variable value or the default if invalid."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default

def connect(*, dict_rows: bool = False) -> psycopg.Connection:
    """Return a psycopg connection to the configured PostgreSQL database."""
    row_factory = dict_row if dict_rows else None
    return psycopg.connect(**_build_connect_kwargs(row_factory=row_factory))


def get_pool() -> "ConnectionPool":
    """Initialise (once) and return a psycopg pooled connection object."""
    if ConnectionPool is None:
        raise RuntimeError(
            "psycopg_pool is required for connection pooling. "
            "Install the 'psycopg-pool' package or set SPEEDLIMIT_DISABLE_POOLING=1."
        )

    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                min_size = max(1, _env_int("SPEEDLIMIT_POOL_MIN_SIZE", 1))
                max_size = max(min_size, _env_int("SPEEDLIMIT_POOL_MAX_SIZE", 5))
                connect_kwargs = _build_connect_kwargs()
                conninfo = connect_kwargs.pop("conninfo", None)
                pool_kwargs = {"min_size": min_size, "max_size": max_size}

                if conninfo:
                    if connect_kwargs:
                        pool_kwargs["kwargs"] = connect_kwargs
                    _POOL = ConnectionPool(conninfo, **pool_kwargs)
                else:
                    pool_kwargs["kwargs"] = connect_kwargs or None
                    _POOL = ConnectionPool(**pool_kwargs)
    return _POOL


@contextmanager
def connection_scope(*, dict_rows: bool = False) -> Iterator[psycopg.Connection]:
    """Context manager that handles commit/rollback automatically."""

    disable_pooling = os.getenv("SPEEDLIMIT_DISABLE_POOLING", "0") == "1"

    if disable_pooling or ConnectionPool is None:
        conn = connect(dict_rows=dict_rows)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return

    pool = get_pool()
    with pool.connection() as conn:
        previous_row_factory = conn.row_factory
        if dict_rows:
            conn.row_factory = dict_row
        try:
            yield conn
            if not conn.autocommit:
                conn.commit()
        except Exception:
            if not conn.autocommit:
                conn.rollback()
            raise
        finally:
            conn.row_factory = previous_row_factory


def fetch_value(query: str, params=None):
    """Execute a scalar query and return the first column of the first row."""
    with connection_scope() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return row[0] if row else None
