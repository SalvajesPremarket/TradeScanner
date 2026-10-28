"""Massive FLOAT integration with bounded refresh and a local-only read path.

Schwab remains the quote provider. Massive supplies free-float values only.
Legacy FMP cache rows are deliberately ignored and FMP is never contacted.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from tradescanner.db import DB_ERRORS, connect as db_connect

MASSIVE_FLOAT_URL = "https://api.massive.com/stocks/vX/float"
CACHE_TTL_SECONDS = 24 * 60 * 60
FAILURE_COOLDOWN_SECONDS = 6 * 60 * 60
PAGE_SIZE = 5000
MAX_PAGES = 3
HTTP_TIMEOUT_SECONDS = 10


def _connect(store: Any):
    connection = db_connect(store.path)
    connection.execute(
        """CREATE TABLE IF NOT EXISTS float_cache (
            symbol TEXT PRIMARY KEY,
            float_shares DOUBLE PRECISION NOT NULL,
            source_date TEXT,
            cached_at DOUBLE PRECISION NOT NULL,
            source TEXT NOT NULL
        )"""
    )
    return connection


def _request_json(url: str, api_key: str) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "api.massive.com":
        raise ValueError("Unexpected Massive pagination URL.")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("apiKey", api_key)
    safe_url = urlunparse(parsed._replace(query=urlencode(query)))
    request = Request(
        safe_url,
        headers={"Accept": "application/json", "User-Agent": "TradeScanner/1.0"},
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Massive FLOAT endpoint returned an unexpected response.")
    if payload.get("status") not in (None, "OK", "DELAYED"):
        raise ValueError("Massive FLOAT endpoint returned an unsuccessful status.")
    return payload


def _normalize_record(item: dict[str, Any]) -> tuple[str, float, str | None] | None:
    symbol = item.get("ticker") or item.get("symbol")
    value = item.get("free_float")
    if not isinstance(symbol, str) or not symbol.strip():
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        return None
    effective_date = item.get("effective_date")
    return (
        symbol.strip().upper(),
        float(value),
        effective_date if isinstance(effective_date, str) else None,
    )


def refresh_float_cache(
    store: Any,
    *,
    fetcher: Callable[[str, str], dict[str, Any]] | None = None,
    now: float | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Refresh Massive FLOAT at most once per day; failures are cooled down."""
    api_key = os.getenv("MASSIVE_API_KEY", "").strip()
    if not api_key:
        return {"status": "not_configured", "provider": "Massive", "rows_saved": 0}

    current = time.time() if now is None else now
    try:
        last_success = float(store.get_state("massive_float_last_success_at", 0) or 0)
        last_attempt = float(store.get_state("massive_float_last_attempt_at", 0) or 0)
        with _connect(store) as connection:
            cached_rows = connection.execute(
                "SELECT COUNT(*) FROM float_cache WHERE source = 'massive'"
            ).fetchone()[0]
    except Exception as exc:
        return {"status": "unavailable", "provider": "Massive", "rows_saved": 0,
                "error": type(exc).__name__}

    if not force and cached_rows > 0 and current - last_success < CACHE_TTL_SECONDS:
        return {"status": "cached", "provider": "Massive", "rows_saved": 0,
                "cached_rows": int(cached_rows)}
    if not force and current - last_attempt < FAILURE_COOLDOWN_SECONDS:
        return {"status": "cooldown", "provider": "Massive", "rows_saved": 0}

    try:
        store.set_state("massive_float_last_attempt_at", current)
    except Exception as exc:
        return {"status": "unavailable", "provider": "Massive", "rows_saved": 0,
                "error": type(exc).__name__}

    get_page = fetcher or _request_json
    records: dict[str, tuple[float, str | None]] = {}
    try:
        next_url: str | None = MASSIVE_FLOAT_URL + "?" + urlencode(
            {"limit": PAGE_SIZE, "sort": "ticker.asc"}
        )
        pages_fetched = 0
        while next_url and pages_fetched < MAX_PAGES:
            payload = get_page(next_url, api_key)
            pages_fetched += 1
            results = payload.get("results", [])
            if not isinstance(results, list):
                raise ValueError("Massive FLOAT results were not a list.")
            for item in results:
                if not isinstance(item, dict):
                    continue
                normalized = _normalize_record(item)
                if normalized:
                    symbol, value, effective_date = normalized
                    records[symbol] = (value, effective_date)
            candidate_next = payload.get("next_url")
            next_url = candidate_next if isinstance(candidate_next, str) and candidate_next else None

        if not records:
            raise ValueError("Massive returned no usable free-float records.")

        with _connect(store) as connection:
            connection.executemany(
                """INSERT INTO float_cache(symbol, float_shares, source_date, cached_at, source)
                   VALUES (?, ?, ?, ?, 'massive')
                   ON CONFLICT(symbol) DO UPDATE SET
                     float_shares=excluded.float_shares,
                     source_date=excluded.source_date,
                     cached_at=excluded.cached_at,
                     source=excluded.source""",
                [(symbol, value, effective_date, current)
                 for symbol, (value, effective_date) in records.items()],
            )
        store.set_state("massive_float_last_success_at", current)
        store.set_state("massive_float_last_error", None)
        return {
            "status": "updated", "provider": "Massive",
            "rows_saved": len(records), "pages_fetched": pages_fetched,
            "updated_at": datetime.fromtimestamp(current, timezone.utc).isoformat(),
        }
    except urllib.error.HTTPError as exc:
        try:
            store.set_state("massive_float_last_error", f"HTTP {exc.code}")
        except Exception:
            pass
        return {"status": "unavailable", "provider": "Massive",
                "rows_saved": 0, "error": f"HTTP {exc.code}"}
    except Exception as exc:
        try:
            store.set_state("massive_float_last_error", type(exc).__name__)
        except Exception:
            pass
        return {"status": "unavailable", "provider": "Massive",
                "rows_saved": 0, "error": type(exc).__name__}


def refresh_symbol_float(
    store: Any,
    symbol: str,
    *,
    fetcher: Callable[[str, str], dict[str, Any]] | None = None,
    now: float | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Request a single symbol for an explicit diagnostic; never falls back to FMP."""
    normalized = symbol.strip().upper() if isinstance(symbol, str) else ""
    if not normalized:
        return {"status": "invalid_symbol", "provider": "Massive"}
    api_key = os.getenv("MASSIVE_API_KEY", "").strip()
    if not api_key:
        return {"status": "not_configured", "provider": "Massive", "symbol": normalized}
    current = time.time() if now is None else now
    try:
        with _connect(store) as connection:
            cached = connection.execute(
                "SELECT float_shares, source_date, cached_at FROM float_cache "
                "WHERE symbol = ? AND source = 'massive'", (normalized,)
            ).fetchone()
        if cached and not force and current - float(cached[2]) < CACHE_TTL_SECONDS:
            return {"status": "cached", "provider": "Massive", "symbol": normalized,
                    "float_shares": float(cached[0]), "effective_date": cached[1]}
        get_page = fetcher or _request_json
        url = MASSIVE_FLOAT_URL + "?" + urlencode({"ticker": normalized})
        payload = get_page(url, api_key)
        results = payload.get("results", [])
        record = next((r for r in results if isinstance(r, dict)
                       and str(r.get("ticker", "")).strip().upper() == normalized
                       and _normalize_record(r)), None) if isinstance(results, list) else None
        if record is None:
            return {"status": "not_found", "provider": "Massive", "symbol": normalized}
        parsed = _normalize_record(record)
        assert parsed is not None
        _, value, effective_date = parsed
        with _connect(store) as connection:
            connection.execute(
                """INSERT INTO float_cache(symbol, float_shares, source_date, cached_at, source)
                   VALUES (?, ?, ?, ?, 'massive')
                   ON CONFLICT(symbol) DO UPDATE SET
                     float_shares=excluded.float_shares,
                     source_date=excluded.source_date,
                     cached_at=excluded.cached_at,
                     source=excluded.source""",
                (normalized, value, effective_date, current),
            )
        return {"status": "updated", "provider": "Massive", "symbol": normalized,
                "float_shares": value, "effective_date": effective_date, "rows_saved": 1}
    except urllib.error.HTTPError as exc:
        return {"status": "unavailable", "provider": "Massive", "symbol": normalized,
                "error": f"HTTP {exc.code}"}
    except Exception as exc:
        return {"status": "unavailable", "provider": "Massive", "symbol": normalized,
                "error": type(exc).__name__}


def apply_cached_float(rows: list[dict[str, Any]], store: Any) -> list[dict[str, Any]]:
    """Decorate quote rows from Massive cache only; never make a network request."""
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
                f"SELECT symbol, float_shares, source_date, source FROM float_cache "
                f"WHERE symbol IN ({placeholders}) AND source = 'massive'",
                symbols,
            ).fetchall()
    except DB_ERRORS + (OSError,):
        return rows
    cache = {
        symbol: {"float_shares": float_shares, "float_as_of": source_date,
                 "float_source": source}
        for symbol, float_shares, source_date, source in found
    }
    for row in rows:
        if row.get("float_shares") is not None:
            continue
        cached = cache.get(str(row.get("symbol", "")).strip().upper())
        if cached:
            row.update(cached)
    return rows
