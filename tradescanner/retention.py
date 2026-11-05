"""Bounded seven-day retention for persisted market-data tables.

The scanner's quote and candle tables are operational market data, not the
trade/strategy ledger. Retention is intentionally limited to those two tables.
Deletes are committed in small batches so maintenance cannot hold one enormous
transaction or monopolize the database.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from tradescanner.db import is_postgres

RETENTION_DAYS = 7
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_MAX_BATCHES_PER_TABLE = 10


def prune_expired_market_data(
    store: Any,
    *,
    now: datetime | None = None,
    retention_days: int = RETENTION_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batches_per_table: int = DEFAULT_MAX_BATCHES_PER_TABLE,
) -> dict[str, Any]:
    """Delete only expired quote snapshots and intraday candles, in bounded batches."""
    if retention_days != RETENTION_DAYS:
        # Keep the production retention contract explicit and prevent accidental
        # configuration from silently expanding or shrinking the history window.
        raise ValueError(f"retention_days must remain {RETENTION_DAYS}")
    if batch_size < 1 or max_batches_per_table < 1:
        raise ValueError("batch_size and max_batches_per_table must be positive")

    from tradescanner.candles import CandleStore

    # Ensure the candle table exists even on a fresh deployment before the first
    # history request has populated it.
    CandleStore(store.path)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    cutoff = current.astimezone(timezone.utc) - timedelta(days=RETENTION_DAYS)
    cutoff_text = cutoff.isoformat()
    cutoff_ms = int(cutoff.timestamp() * 1000)
    postgres = is_postgres(store.path)

    if postgres:
        quote_delete = """
            DELETE FROM quote_snapshots
            WHERE id IN (
                SELECT id FROM quote_snapshots
                WHERE captured_at < ?
                ORDER BY captured_at
                LIMIT ?
            )
        """
        candle_delete = """
            DELETE FROM intraday_candles
            WHERE (symbol, candle_time_ms) IN (
                SELECT symbol, candle_time_ms FROM intraday_candles
                WHERE candle_time_ms < ?
                ORDER BY candle_time_ms
                LIMIT ?
            )
        """
    else:
        quote_delete = """
            DELETE FROM quote_snapshots
            WHERE id IN (
                SELECT id FROM quote_snapshots
                WHERE captured_at < ?
                ORDER BY captured_at
                LIMIT ?
            )
        """
        candle_delete = """
            DELETE FROM intraday_candles
            WHERE (symbol, candle_time_ms) IN (
                SELECT symbol, candle_time_ms FROM intraday_candles
                WHERE candle_time_ms < ?
                ORDER BY candle_time_ms
                LIMIT ?
            )
        """

    quote_deleted = _delete_in_batches(
        store, quote_delete, cutoff_text, batch_size, max_batches_per_table
    )
    candles_deleted = _delete_in_batches(
        store, candle_delete, cutoff_ms, batch_size, max_batches_per_table
    )

    # ANALYZE updates planner statistics without a table rewrite or the extra
    # temporary disk space required by VACUUM FULL. PostgreSQL autovacuum can
    # reclaim dead tuples for reuse; no blocking full-table rewrite is attempted.
    if quote_deleted or candles_deleted:
        with store._connect() as connection:
            connection.execute("ANALYZE quote_snapshots")
            connection.execute("ANALYZE intraday_candles")

    with store._connect() as connection:
        expired_quotes = connection.execute(
            "SELECT COUNT(*) FROM quote_snapshots WHERE captured_at < ?",
            (cutoff_text,),
        ).fetchone()[0]
        expired_candles = connection.execute(
            "SELECT COUNT(*) FROM intraday_candles WHERE candle_time_ms < ?",
            (cutoff_ms,),
        ).fetchone()[0]

    return {
        "retention_days": RETENTION_DAYS,
        "cutoff_utc": cutoff_text,
        "quote_snapshots_deleted": quote_deleted,
        "intraday_candles_deleted": candles_deleted,
        "expired_quote_snapshots_remaining": int(expired_quotes),
        "expired_intraday_candles_remaining": int(expired_candles),
        "batch_size": batch_size,
        "max_batches_per_table": max_batches_per_table,
    }


def _delete_in_batches(
    store: Any,
    sql: str,
    cutoff: str | int,
    batch_size: int,
    max_batches: int,
) -> int:
    deleted = 0
    for _ in range(max_batches):
        with store._connect() as connection:
            cursor = connection.execute(sql, (cutoff, batch_size))
            batch_deleted = max(0, int(cursor.rowcount))
        deleted += batch_deleted
        if batch_deleted < batch_size:
            break
    return deleted
