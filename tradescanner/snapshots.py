"""SQLite-backed snapshots and bounded Schwab quote collection for TradeScanner."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from tradescanner.core import normalize_schwab_quotes
from tradescanner.schwab import get_quotes

DEFAULT_DB_PATH = Path(os.getenv("TRADESCANNER_DB_PATH", "data/tradescanner.sqlite3"))
BATCH_SIZE = 100


class SnapshotStore:
    """Persist observed quote snapshots; configure TRADESCANNER_DB_PATH for durable storage."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else DEFAULT_DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS quote_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    data_source TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_quote_snapshots_symbol_time "
                "ON quote_snapshots(symbol, captured_at)"
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.path), timeout=10)

    def save(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        captured_at = datetime.now(timezone.utc).isoformat()
        records = []
        for row in rows:
            symbol = row.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                continue
            records.append((
                captured_at,
                symbol.strip().upper(),
                str(row.get("data_source") or "unknown"),
                json.dumps(row, separators=(",", ":"), allow_nan=False),
            ))
        with self._connect() as connection:
            connection.executemany(
                "INSERT INTO quote_snapshots "
                "(captured_at, symbol, data_source, payload_json) VALUES (?, ?, ?, ?)",
                records,
            )
        return len(records)

    def stats(self) -> dict[str, int | str | None]:
        with self._connect() as connection:
            total, symbols, latest = connection.execute(
                "SELECT COUNT(*), COUNT(DISTINCT symbol), MAX(captured_at) "
                "FROM quote_snapshots"
            ).fetchone()
        return {
            "snapshot_count": int(total),
            "symbols_seen": int(symbols),
            "latest_snapshot_at": latest,
        }

    def latest(self, limit: int = 100) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 1000))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json FROM quote_snapshots AS current
                WHERE captured_at = (
                    SELECT MAX(previous.captured_at)
                    FROM quote_snapshots AS previous
                    WHERE previous.symbol = current.symbol
                )
                ORDER BY symbol LIMIT ?
                """,
                (bounded_limit,),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]


def collect_snapshots(
    symbols: list[str],
    store: SnapshotStore,
    quote_fetcher: Callable[[list[str]], dict[str, Any]] | None = None,
) -> dict[str, int]:
    """Fetch real quotes in provider-sized batches and persist normalized observations."""
    normalized = list(dict.fromkeys(
        symbol.strip().upper() for symbol in symbols if isinstance(symbol, str) and symbol.strip()
    ))
    if not normalized:
        raise ValueError("Provide at least one ticker symbol.")
    fetch = quote_fetcher or get_quotes
    all_rows: list[dict[str, Any]] = []
    for start in range(0, len(normalized), BATCH_SIZE):
        payload = fetch(normalized[start:start + BATCH_SIZE])
        if not isinstance(payload, dict):
            raise ValueError("Quote provider returned an invalid payload.")
        all_rows.extend(normalize_schwab_quotes(payload))
    saved = store.save(all_rows)
    return {
        "requested_count": len(normalized),
        "received_count": len(all_rows),
        "snapshots_saved": saved,
    }
