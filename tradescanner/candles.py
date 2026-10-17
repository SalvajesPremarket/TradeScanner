"""Intraday candle storage and time-of-day relative-volume calculations."""
from __future__ import annotations

import math
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tradescanner.snapshots import DEFAULT_DB_PATH

MARKET_TZ = ZoneInfo("America/New_York")


def normalize_price_history(symbol: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Keep only well-formed OHLCV candles supplied by the provider."""
    raw = payload.get("candles")
    if not isinstance(raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        timestamp = item.get("datetime")
        volume = item.get("volume")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            continue
        if isinstance(volume, bool) or not isinstance(volume, (int, float)) or not math.isfinite(volume) or volume < 0:
            continue
        values: dict[str, float] = {}
        valid = True
        for field in ("open", "high", "low", "close"):
            value = item.get(field)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                valid = False
                break
            values[field] = float(value)
        if not valid or not math.isfinite(timestamp):
            continue
        normalized.append({
            "symbol": symbol.strip().upper(),
            "candle_time_ms": int(timestamp),
            **values,
            "volume": int(volume),
        })
    return normalized


class CandleStore:
    """Persist provider candles in the same SQLite database as quote snapshots."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else DEFAULT_DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS intraday_candles (
                    symbol TEXT NOT NULL,
                    candle_time_ms INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, candle_time_ms)
                )"""
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_candles_symbol_time "
                "ON intraday_candles(symbol, candle_time_ms)"
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.path), timeout=10)

    def save(self, candles: list[dict[str, Any]]) -> int:
        if not candles:
            return 0
        records = [
            (
                row["symbol"], row["candle_time_ms"], row["open"], row["high"],
                row["low"], row["close"], row["volume"],
            )
            for row in candles
        ]
        with self._connect() as connection:
            connection.executemany(
                """INSERT INTO intraday_candles
                   (symbol, candle_time_ms, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(symbol, candle_time_ms) DO UPDATE SET
                     open=excluded.open, high=excluded.high, low=excluded.low,
                     close=excluded.close, volume=excluded.volume,
                     fetched_at=CURRENT_TIMESTAMP""",
                records,
            )
        return len(records)

    def latest(self, symbol: str, limit: int = 3000) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 10000))
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT symbol, candle_time_ms, open, high, low, close, volume
                   FROM intraday_candles WHERE symbol = ?
                   ORDER BY candle_time_ms DESC LIMIT ?""",
                (symbol.strip().upper(), bounded_limit),
            ).fetchall()
        keys = ("symbol", "candle_time_ms", "open", "high", "low", "close", "volume")
        return [dict(zip(keys, row)) for row in reversed(rows)]

    def rvol(self, symbol: str) -> dict[str, Any]:
        candles = self.latest(symbol)
        if not candles:
            return {"rvol": None, "current_volume": None, "average_volume": None, "sessions_used": 0}
        sessions: dict[Any, list[tuple[int, int]]] = {}
        for candle in candles:
            local = datetime.fromtimestamp(candle["candle_time_ms"] / 1000, MARKET_TZ)
            minute = local.hour * 60 + local.minute
            sessions.setdefault(local.date(), []).append((minute, int(candle["volume"])))
        current_day = max(sessions)
        current_bars = sessions[current_day]
        cutoff = max(minute for minute, _ in current_bars)
        current_volume = sum(volume for minute, volume in current_bars if minute <= cutoff)

        previous_days = sorted((day for day in sessions if day < current_day), reverse=True)[:5]
        historical_totals: list[int] = []
        for day in previous_days:
            bars = sessions[day]
            if not any(minute <= cutoff for minute, _ in bars):
                continue
            historical_totals.append(sum(volume for minute, volume in bars if minute <= cutoff))
        if not historical_totals:
            return {
                "rvol": None,
                "current_volume": current_volume,
                "average_volume": None,
                "sessions_used": 0,
                "as_of": datetime.fromtimestamp(candles[-1]["candle_time_ms"] / 1000, MARKET_TZ).isoformat(),
            }
        average = sum(historical_totals) / len(historical_totals)
        value = current_volume / average if average > 0 else None
        return {
            "rvol": round(value, 4) if value is not None else None,
            "current_volume": current_volume,
            "average_volume": round(average, 2),
            "sessions_used": len(historical_totals),
            "as_of": datetime.fromtimestamp(candles[-1]["candle_time_ms"] / 1000, MARKET_TZ).isoformat(),
        }
