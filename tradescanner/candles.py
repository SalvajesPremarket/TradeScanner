"""Intraday candle storage and time-of-day relative-volume calculations."""
from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tradescanner.snapshots import DEFAULT_DB_PATH
from tradescanner.db import connect as db_connect, is_postgres

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
        if (
            isinstance(volume, bool)
            or not isinstance(volume, (int, float))
            or not math.isfinite(volume)
            or volume < 0
            or not float(volume).is_integer()
        ):
            continue
        values: dict[str, float] = {}
        valid = True
        for field in ("open", "high", "low", "close"):
            value = item.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                valid = False
                break
            values[field] = float(value)
        if (
            not valid
            or not math.isfinite(timestamp)
            or timestamp <= 0
            or not float(timestamp).is_integer()
        ):
            continue
        if (
            values["high"] < max(values["open"], values["close"], values["low"])
            or values["low"] > min(values["open"], values["close"], values["high"])
        ):
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
        configured_url = __import__("os").getenv("DATABASE_URL", "").strip()
        self.path = (str(path) if path is not None else configured_url) or DEFAULT_DB_PATH
        if not is_postgres(self.path):
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS intraday_candles (
                    symbol TEXT NOT NULL,
                    candle_time_ms BIGINT NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    volume BIGINT NOT NULL,
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, candle_time_ms)
                )"""
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_candles_symbol_time "
                "ON intraday_candles(symbol, candle_time_ms)"
            )

    def _connect(self):
        return db_connect(self.path)

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

    def latest(self, symbol: str, limit: int = 6000) -> list[dict[str, Any]]:
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
        current_by_minute = {
            minute: volume for minute, volume in current_bars if minute <= cutoff
        }
        current_volume = sum(current_by_minute.values())

        previous_days = sorted((day for day in sessions if day < current_day), reverse=True)[:5]
        historical_totals: list[int] = []
        for day in previous_days:
            bars = sessions[day]
            # Compare cumulative volume at the same clock time. Providers may omit
            # one-minute candles for intervals with no trades, so requiring every
            # exact minute key would reject otherwise usable liquid-session history.
            # Still reject a historical session that ends before the current cutoff.
            if not bars or max(minute for minute, _ in bars) < cutoff:
                continue
            historical_totals.append(sum(
                volume for minute, volume in bars if minute <= cutoff
            ))
        # One prior session is too fragile to establish a reliable baseline.
        # Keep RVOL unavailable until at least two comparable historical sessions exist.
        if len(historical_totals) < 2:
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
