"""Small SQLite/PostgreSQL compatibility layer for TradeScanner storage."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import tuple_row

DEFAULT_SQLITE_PATH = Path(os.getenv("TRADESCANNER_DB_PATH", "data/tradescanner.sqlite3"))
DB_ERRORS = (sqlite3.Error, psycopg.Error)


def is_postgres(database: str | Path | None = None) -> bool:
    value = str(database or os.getenv("DATABASE_URL", "")).strip().lower()
    return value.startswith(("postgres://", "postgresql://"))


class Connection:
    """Keep the existing qmark SQL API while using psycopg for PostgreSQL."""
    def __init__(self, raw: Any, postgres: bool) -> None:
        self.raw = raw
        self.postgres = postgres

    def __enter__(self):
        self.raw.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            return self.raw.__exit__(exc_type, exc, tb)
        finally:
            if not self.postgres:
                self.raw.close()

    def execute(self, sql: str, params: Any = ()):
        if self.postgres:
            cursor = self.raw.cursor()
            cursor.execute(sql.replace("?", "%s"), params)
            return cursor
        return self.raw.execute(sql, params)

    def executemany(self, sql: str, params: Any):
        if self.postgres:
            cursor = self.raw.cursor()
            cursor.executemany(sql.replace("?", "%s"), params)
            return cursor
        return self.raw.executemany(sql, params)


def connect(database: str | Path | None = None) -> Connection:
    """Connect to DATABASE_URL when it is PostgreSQL; otherwise use SQLite."""
    configured = str(database) if database is not None else os.getenv("DATABASE_URL", "").strip()
    if configured.lower().startswith(("postgres://", "postgresql://")):
        raw = psycopg.connect(configured, connect_timeout=10, row_factory=tuple_row)
        return Connection(raw, True)
    if database is None:
        configured = os.getenv("TRADESCANNER_DB_PATH", "data/tradescanner.sqlite3")
    path = Path(configured or "data/tradescanner.sqlite3")
    path.parent.mkdir(parents=True, exist_ok=True)
    return Connection(sqlite3.connect(str(path), timeout=10), False)
