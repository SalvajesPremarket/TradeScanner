"""SQLite-backed snapshots and bounded Schwab quote collection for TradeScanner."""
from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from concurrent.futures import ThreadPoolExecutor

from tradescanner.core import normalize_schwab_quotes
from tradescanner.db import connect as db_connect, is_postgres
from tradescanner.schwab import get_quotes

DEFAULT_DB_PATH = Path(os.getenv("TRADESCANNER_DB_PATH", "data/tradescanner.sqlite3"))
BATCH_SIZE = 100
QUOTE_FRESHNESS_SECONDS = 300
QUOTE_FUTURE_TOLERANCE_SECONDS = 30


def _refresh_quote_freshness(row: dict[str, Any]) -> dict[str, Any]:
    """Recompute quote freshness at read time; stored flags age out after capture."""
    stamp = row.get("quote_time_ms")
    if isinstance(stamp, bool) or not isinstance(stamp, (int, float)) or not math.isfinite(float(stamp)) or stamp <= 0:
        row["quote_age_seconds"] = None
        row["quote_is_fresh"] = False
        return row
    age = time.time() - float(stamp) / 1000.0
    row["quote_age_seconds"] = round(age, 3)
    row["quote_is_fresh"] = -QUOTE_FUTURE_TOLERANCE_SECONDS <= age <= QUOTE_FRESHNESS_SECONDS
    return row


class SnapshotStore:
    """Persist observed quote snapshots; configure TRADESCANNER_DB_PATH for durable storage."""

    def __init__(self, path: str | Path | None = None) -> None:
        configured_url = os.getenv("DATABASE_URL", "").strip()
        self.path = (str(path) if path is not None else configured_url) or DEFAULT_DB_PATH
        if not is_postgres(self.path):
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            snapshot_id = "BIGSERIAL PRIMARY KEY" if is_postgres(self.path) else "INTEGER PRIMARY KEY AUTOINCREMENT"
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS quote_snapshots (
                    id {snapshot_id},
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
            # Keep the global latest-radar query index-backed. The existing
            # symbol-first index serves per-symbol lookups; this time-first
            # index lets PostgreSQL walk newest batches and stop at the limit.
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_quote_snapshots_captured_symbol "
                "ON quote_snapshots(captured_at DESC, symbol)"
            )
            # Keep one current quote per symbol for the live scanner. Historical
            # snapshots are sampled separately so faster scan cadence cannot grow
            # the time-series table without bound.
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS quote_latest (
                    symbol TEXT PRIMARY KEY,
                    captured_at TEXT NOT NULL,
                    data_source TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            # Backfill only once on upgrade. Avoid a full historical-table scan
            # on every process restart after the current-quote table is populated.
            current_quote_count = connection.execute(
                "SELECT COUNT(*) FROM quote_latest"
            ).fetchone()[0]
            if not current_quote_count and is_postgres(self.path):
                connection.execute(
                    """
                    INSERT INTO quote_latest (symbol, captured_at, data_source, payload_json)
                    SELECT DISTINCT ON (symbol) symbol, captured_at, data_source, payload_json
                    FROM quote_snapshots
                    ORDER BY symbol, captured_at DESC, id DESC
                    """
                )
            elif not current_quote_count:
                connection.execute(
                    """
                    INSERT INTO quote_latest (symbol, captured_at, data_source, payload_json)
                    SELECT q.symbol, q.captured_at, q.data_source, q.payload_json
                    FROM quote_snapshots q
                    WHERE q.id = (
                        SELECT previous.id FROM quote_snapshots previous
                        WHERE previous.symbol = q.symbol
                        ORDER BY previous.captured_at DESC, previous.id DESC
                        LIMIT 1
                    )
                    """
                )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS scanner_state ("
                "state_key TEXT PRIMARY KEY, state_json TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS scanner_leases ("
                "lease_key TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at REAL NOT NULL)"
            )

    def _connect(self):
        return db_connect(self.path)

    def acquire_lease(self, key: str, owner: str, ttl_seconds: float = 180.0) -> bool:
        """Atomically acquire an expiring cross-instance lease."""
        now = time.time()
        expires_at = now + max(10.0, float(ttl_seconds))
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO scanner_leases(lease_key, owner, expires_at) VALUES (?, ?, ?) "
                "ON CONFLICT(lease_key) DO UPDATE SET owner=excluded.owner, expires_at=excluded.expires_at "
                "WHERE scanner_leases.expires_at <= ? OR scanner_leases.owner = ?",
                (key, owner, expires_at, now, owner),
            )
            return cursor.rowcount == 1

    def release_lease(self, key: str, owner: str) -> None:
        """Release only a lease owned by this process."""
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM scanner_leases WHERE lease_key = ? AND owner = ?",
                (key, owner),
            )

    def get_state(self, key: str, default: Any = None) -> Any:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state_json FROM scanner_state WHERE state_key = ?", (key,)
            ).fetchone()
        if row is None:
            return default
        return json.loads(row[0])

    def set_state(self, key: str, value: Any) -> None:
        encoded = json.dumps(value, separators=(",", ":"), allow_nan=False)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO scanner_state(state_key, state_json) VALUES (?, ?) "
                "ON CONFLICT(state_key) DO UPDATE SET state_json = excluded.state_json",
                (key, encoded),
            )

    def save(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        captured_at = datetime.now(timezone.utc).isoformat()
        history_cutoff = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() - 4 * 60 * 60,
            timezone.utc,
        ).isoformat()
        latest_records = []
        history_records = []
        for row in rows:
            symbol = row.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                continue
            normalized_symbol = symbol.strip().upper()
            data_source = str(row.get("data_source") or "unknown")
            payload = json.dumps(row, separators=(",", ":"), allow_nan=False)
            latest_records.append((normalized_symbol, captured_at, data_source, payload))
            history_records.append((
                captured_at, normalized_symbol, data_source, payload,
                normalized_symbol, history_cutoff,
            ))
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO quote_latest (symbol, captured_at, data_source, payload_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    captured_at = excluded.captured_at,
                    data_source = excluded.data_source,
                    payload_json = excluded.payload_json
                WHERE excluded.captured_at >= quote_latest.captured_at
                """,
                latest_records,
            )
            # Keep at most one historical observation per symbol per four hours.
            # The live quote table above is still refreshed on every scan cycle.
            connection.executemany(
                """
                INSERT INTO quote_snapshots
                    (captured_at, symbol, data_source, payload_json)
                SELECT ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM quote_snapshots
                    WHERE symbol = ? AND captured_at >= ?
                )
                """,
                history_records,
            )
        return len(latest_records)

    def stats(self) -> dict[str, int | str | None]:
        with self._connect() as connection:
            total, symbols, latest = connection.execute(
                "SELECT COUNT(*), COUNT(DISTINCT symbol), MAX(captured_at) "
                "FROM quote_snapshots"
            ).fetchone()
            current_count, current_latest = connection.execute(
                "SELECT COUNT(*), MAX(captured_at) FROM quote_latest"
            ).fetchone()
        return {
            "snapshot_count": int(total),
            "symbols_seen": int(symbols),
            "latest_snapshot_at": latest,
            "current_quote_count": int(current_count),
            "latest_current_quote_at": current_latest,
            "historical_sample_interval_seconds": 14400,
        }

    def latest_for_symbols(self, symbols: list[str]) -> list[dict[str, Any]]:
        normalized = list(dict.fromkeys(
            symbol.strip().upper() for symbol in symbols
            if isinstance(symbol, str) and symbol.strip()
        ))
        if not normalized:
            return []
        placeholders = ",".join("?" for _ in normalized)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT payload_json FROM quote_latest WHERE symbol IN ({placeholders}) ORDER BY symbol",
                normalized,
            ).fetchall()
        return [_refresh_quote_freshness(json.loads(row[0])) for row in rows]

    def latest(self, limit: int = 100) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 10000))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json FROM quote_latest
                ORDER BY captured_at DESC, symbol LIMIT ?
                """,
                (bounded_limit,),
            ).fetchall()
        return [_refresh_quote_freshness(json.loads(row[0])) for row in rows]


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
    batches = [
        normalized[start:start + BATCH_SIZE]
        for start in range(0, len(normalized), BATCH_SIZE)
    ]

    def fetch_and_normalize(batch: list[str]) -> list[dict[str, Any]]:
        payload = fetch(batch)
        if not isinstance(payload, dict):
            raise ValueError("Quote provider returned an invalid payload.")
        return normalize_schwab_quotes(payload)

    # A bounded worker pool overlaps Schwab network latency without exceeding
    # the provider's 100-symbol request limit or creating unbounded concurrency.
    workers = min(4, len(batches))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="schwab-quotes") as pool:
        all_rows = [
            row
            for batch_rows in pool.map(fetch_and_normalize, batches)
            for row in batch_rows
        ]
    saved = store.save(all_rows)
    return {
        "requested_count": len(normalized),
        "received_count": len(all_rows),
        "snapshots_saved": saved,
    }
