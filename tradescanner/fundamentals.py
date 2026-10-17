"""Optional FMP float enrichment with a daily SQLite cache."""
from __future__ import annotations

import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

from tradescanner.snapshots import DEFAULT_DB_PATH

FMP_FLOAT_URL = "https://financialmodelingprep.com/stable/shares-float"


class FloatDataError(RuntimeError):
    pass


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0 else None


def normalize_float_payload(symbol: str, payload: Any) -> dict[str, Any] | None:
    """Normalize FMP's documented list response; unknown values stay unavailable."""
    if not isinstance(payload, list):
        return None
    target = symbol.strip().upper()
    for item in payload:
        if not isinstance(item, dict) or str(item.get("symbol", "")).strip().upper() != target:
            continue
        float_shares = _number(item.get("floatShares"))
        if float_shares is None:
            return None
        free_float = _number(item.get("freeFloat"))
        outstanding = _number(item.get("outstandingShares"))
        date = item.get("date") if isinstance(item.get("date"), str) else None
        source = item.get("source") if isinstance(item.get("source"), str) else None
        return {
            "symbol": target,
            "float_shares": float_shares,
            "free_float_pct": free_float,
            "outstanding_shares": outstanding,
            "data_date": date,
            "source_url": source,
        }
    return None


class FloatStore:
    """Cache FMP float data by ticker for up to 24 hours."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else DEFAULT_DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS float_cache (
                    symbol TEXT PRIMARY KEY,
                    float_shares REAL,
                    free_float_pct REAL,
                    outstanding_shares REAL,
                    data_date TEXT,
                    source_url TEXT,
                    fetched_at INTEGER NOT NULL
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS float_request_budget (
                    request_day TEXT PRIMARY KEY,
                    requests INTEGER NOT NULL
                )"""
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.path), timeout=10)

    def get_many(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        normalized = list(dict.fromkeys(
            symbol.strip().upper() for symbol in symbols
            if isinstance(symbol, str) and symbol.strip()
        ))
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        with self._connect() as connection:
            rows = connection.execute(
                f"""SELECT symbol, float_shares, free_float_pct, outstanding_shares,
                           data_date, source_url, fetched_at
                    FROM float_cache WHERE symbol IN ({placeholders})""",
                normalized,
            ).fetchall()
        keys = ("symbol", "float_shares", "free_float_pct", "outstanding_shares",
                "data_date", "source_url", "fetched_at")
        return {
            row[0]: dict(zip(keys, row))
            for row in rows
        }

    def _reserve_daily_request(self, max_requests: int) -> bool:
        if max_requests <= 0:
            return False
        today = datetime.now(timezone.utc).date().isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                """INSERT INTO float_request_budget(request_day, requests) VALUES (?, 1)
                   ON CONFLICT(request_day) DO UPDATE SET requests=requests + 1
                   WHERE float_request_budget.requests < ?""",
                (today, max_requests),
            )
            return cursor.rowcount == 1

    def get_or_fetch(
        self,
        symbol: str,
        *,
        fetcher: Callable[[str], Any] | None = None,
        ttl_seconds: int = 86400,
    ) -> dict[str, Any] | None:
        normalized = symbol.strip().upper()
        cached = self.get_many([normalized]).get(normalized)
        now = int(time.time())
        if cached is not None and now - int(cached["fetched_at"]) < ttl_seconds:
            return cached
        api_key = os.getenv("FMP_API_KEY", "").strip()
        if fetcher is None and not api_key:
            return cached

        if fetcher is not None:
            payload = fetcher(normalized)
        else:
            try:
                daily_limit = max(0, int(os.getenv("TRADESCANNER_FLOAT_MAX_REQUESTS_PER_DAY", "200")))
            except ValueError:
                daily_limit = 200
            if not self._reserve_daily_request(daily_limit):
                return cached
            query = urllib.parse.urlencode({"symbol": normalized, "apikey": api_key})
            request = urllib.request.Request(
                f"{FMP_FLOAT_URL}?{query}",
                headers={"Accept": "application/json", "User-Agent": "TradeScanner/1.0"},
            )
            try:
                with urllib.request.urlopen(request, timeout=15) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raise FloatDataError(f"FMP float endpoint returned HTTP {exc.code}.") from None
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                raise FloatDataError(f"FMP float endpoint unavailable ({type(exc).__name__}).") from None
            except json.JSONDecodeError:
                raise FloatDataError("FMP float endpoint returned invalid JSON.") from None

        normalized_data = normalize_float_payload(normalized, payload)
        if normalized_data is None:
            # Do not cache missing values as valid float data.
            return None
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO float_cache
                   (symbol, float_shares, free_float_pct, outstanding_shares,
                    data_date, source_url, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(symbol) DO UPDATE SET
                     float_shares=excluded.float_shares,
                     free_float_pct=excluded.free_float_pct,
                     outstanding_shares=excluded.outstanding_shares,
                     data_date=excluded.data_date,
                     source_url=excluded.source_url,
                     fetched_at=excluded.fetched_at""",
                (
                    normalized_data["symbol"], normalized_data["float_shares"],
                    normalized_data["free_float_pct"], normalized_data["outstanding_shares"],
                    normalized_data["data_date"], normalized_data["source_url"], now,
                ),
            )
        return {**normalized_data, "fetched_at": now}
