"""FMP integration limited strictly to public share-float data.

The all-float endpoint is refreshed at most once per 24 hours, with a six-hour
cooldown after failures. Results are persisted in the scanner's SQLite database
so UI requests never call FMP and quote scanning remains independent of FMP.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

FMP_FLOAT_URL = "https://financialmodelingprep.com/stable/shares-float-all"
CACHE_TTL_SECONDS = 24 * 60 * 60
FAILURE_COOLDOWN_SECONDS = 6 * 60 * 60
PAGE_SIZE = 5000
MAX_PAGES = 2
HTTP_TIMEOUT_SECONDS = 5


def _connect(store: Any) -> sqlite3.Connection:
    connection = sqlite3.connect(str(store.path), timeout=10)
    connection.execute(
        """CREATE TABLE IF NOT EXISTS float_cache (
            symbol TEXT PRIMARY KEY,
            float_shares REAL NOT NULL,
            source_date TEXT,
            cached_at REAL NOT NULL,
            source TEXT NOT NULL
        )"""
    )
    return connection


def _fetch_page(page: int, api_key: str) -> list[dict[str, Any]]:
    query = urlencode({"page": page, "limit": PAGE_SIZE, "apikey": api_key})
    request = Request(
        f"{FMP_FLOAT_URL}?{query}",
        headers={"Accept": "application/json", "User-Agent": "TradeScanner/1.0"},
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError("FMP float endpoint returned an unexpected response.")
    return [item for item in payload if isinstance(item, dict)]


def refresh_float_cache(
    store: Any,
    *,
    fetcher: Callable[[int, str], list[dict[str, Any]]] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Refresh cached float data infrequently; never raise into the quote scanner."""
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {"status": "not_configured", "provider": "FMP", "rows_saved": 0}

    current = time.time() if now is None else now
    try:
        last_success = store.get_state("fmp_float_last_success_at", 0)
        last_attempt = store.get_state("fmp_float_last_attempt_at", 0)
    except Exception as exc:
        return {"status": "unavailable", "provider": "FMP", "rows_saved": 0, "error": type(exc).__name__}
    try:
        last_success = float(last_success or 0)
        last_attempt = float(last_attempt or 0)
    except (TypeError, ValueError):
        last_success = last_attempt = 0.0

    if current - last_success < CACHE_TTL_SECONDS:
        return {"status": "cached", "provider": "FMP", "rows_saved": 0}
    if current - last_attempt < FAILURE_COOLDOWN_SECONDS:
        return {"status": "cooldown", "provider": "FMP", "rows_saved": 0}

    # Persist the attempt before networking, preventing rapid retry loops after a crash.
    try:
        store.set_state("fmp_float_last_attempt_at", current)
    except Exception as exc:
        return {"status": "unavailable", "provider": "FMP", "rows_saved": 0, "error": type(exc).__name__}
    fetch_page = fetcher or (lambda page, key: _fetch_page(page, key))
    records: dict[str, tuple[float, str | None]] = {}
    try:
        for page in range(MAX_PAGES):
            items = fetch_page(page, api_key)
            for item in items:
                symbol = item.get("symbol")
                value = item.get("floatShares")
                if not isinstance(symbol, str) or not symbol.strip():
                    continue
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    continue
                if value <= 0:
                    continue
                normalized = symbol.strip().upper()
                source_date = item.get("date")
                records[normalized] = (
                    float(value),
                    source_date if isinstance(source_date, str) else None,
                )
            if len(items) < PAGE_SIZE:
                break

        if not records:
            raise ValueError("FMP float endpoint returned no usable float values.")

        with _connect(store) as connection:
            connection.executemany(
                """INSERT INTO float_cache(symbol, float_shares, source_date, cached_at, source)
                   VALUES (?, ?, ?, ?, 'fmp')
                   ON CONFLICT(symbol) DO UPDATE SET
                     float_shares=excluded.float_shares,
                     source_date=excluded.source_date,
                     cached_at=excluded.cached_at,
                     source=excluded.source""",
                [
                    (symbol, value, source_date, current)
                    for symbol, (value, source_date) in records.items()
                ],
            )
        store.set_state("fmp_float_last_success_at", current)
        return {
            "status": "updated",
            "provider": "FMP",
            "rows_saved": len(records),
            "pages_fetched": page + 1,
            "updated_at": datetime.fromtimestamp(current, timezone.utc).isoformat(),
        }
    except Exception as exc:
        # Only record the exception class; never expose keys, URLs, or provider bodies.
        return {
            "status": "unavailable",
            "provider": "FMP",
            "rows_saved": 0,
            "error": type(exc).__name__,
        }


def apply_cached_float(rows: list[dict[str, Any]], store: Any) -> list[dict[str, Any]]:
    """Decorate quote rows from local cache only; this function never calls FMP."""
    if not rows:
        return rows
    symbols = list(dict.fromkeys(
        row.get("symbol", "").strip().upper()
        for row in rows
        if isinstance(row.get("symbol"), str) and row.get("symbol", "").strip()
    ))
    if not symbols:
        return rows
    placeholders = ",".join("?" for _ in symbols)
    try:
        with _connect(store) as connection:
            found = connection.execute(
                f"SELECT symbol, float_shares, source_date FROM float_cache WHERE symbol IN ({placeholders})",
                symbols,
            ).fetchall()
    except (OSError, sqlite3.Error):
        return rows
    cache = {
        symbol: {"float_shares": float_shares, "float_as_of": source_date, "float_source": "fmp"}
        for symbol, float_shares, source_date in found
    }
    for row in rows:
        if row.get("float_shares") is not None:
            continue
        cached = cache.get(str(row.get("symbol", "")).strip().upper())
        if cached:
            row.update(cached)
    return rows
