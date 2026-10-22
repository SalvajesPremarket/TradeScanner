"""Bounded FMP stock-news cache used by native Momentum scans.

Network requests are optional and rate-limited. Missing or inaccessible news stays
missing; no headline is synthesized.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tradescanner.db import DB_ERRORS, connect as db_connect

NEWS_URL = "https://financialmodelingprep.com/stable/news/stock"
CACHE_TTL_SECONDS = 300
FAILURE_COOLDOWN_SECONDS = 6 * 60 * 60
HTTP_TIMEOUT_SECONDS = 5
MAX_SYMBOLS = 100
MAX_ARTICLES = 250


def _connect(store: Any):
    connection = db_connect(store.path)
    connection.execute(
        """CREATE TABLE IF NOT EXISTS stock_news_cache (
            symbol TEXT NOT NULL,
            published_at TEXT NOT NULL,
            headline TEXT NOT NULL,
            publisher TEXT,
            article_url TEXT,
            cached_at DOUBLE PRECISION NOT NULL,
            PRIMARY KEY(symbol, published_at, headline)
        )"""
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_news_symbol_time ON stock_news_cache(symbol, published_at)"
    )
    return connection


def _fetch_news(symbols: list[str], api_key: str) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    query = urlencode({
        "symbols": ",".join(symbols),
        "from": (now - timedelta(days=2)).date().isoformat(),
        "to": now.date().isoformat(),
        "limit": MAX_ARTICLES,
        "apikey": api_key,
    })
    request = Request(
        f"{NEWS_URL}?{query}",
        headers={"Accept": "application/json", "User-Agent": "TradeScanner/1.0"},
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError("FMP stock-news endpoint returned an unexpected response.")
    return [item for item in payload if isinstance(item, dict)]


def refresh_news_cache(
    store: Any, symbols: list[str], *,
    fetcher: Callable[[list[str], str], list[dict[str, Any]]] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Refresh news at most once every five minutes for a bounded symbol batch."""
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {"status": "not_configured", "provider": "FMP", "articles_saved": 0}
    normalized = list(dict.fromkeys(
        value.strip().upper() for value in symbols
        if isinstance(value, str) and value.strip()
    ))[:MAX_SYMBOLS]
    if not normalized:
        return {"status": "no_symbols", "provider": "FMP", "articles_saved": 0}
    current = time.time() if now is None else now
    try:
        cooldown_until = float(store.get_state("fmp_news_cooldown_until", 0) or 0)
        if current < cooldown_until:
            return {"status": "cooldown", "provider": "FMP", "articles_saved": 0}
        last_success = float(store.get_state("fmp_news_last_success_at", 0) or 0)
        if current - last_success < CACHE_TTL_SECONDS:
            return {"status": "cached", "provider": "FMP", "articles_saved": 0}
        store.set_state("fmp_news_last_attempt_at", current)
    except (OSError, RuntimeError) + DB_ERRORS:
        return {"status": "unavailable", "provider": "FMP", "articles_saved": 0}

    fetch = fetcher or _fetch_news
    try:
        articles = fetch(normalized, api_key)
        records: dict[tuple[str, str, str], tuple[str | None, str | None]] = {}
        for article in articles[:MAX_ARTICLES]:
            symbol = article.get("symbol")
            published = article.get("publishedDate")
            headline = article.get("title")
            if not isinstance(symbol, str) or not isinstance(published, str) or not isinstance(headline, str):
                continue
            symbol = symbol.strip().upper()
            if symbol not in normalized or not headline.strip():
                continue
            try:
                parsed = datetime.fromisoformat(published.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                published = parsed.astimezone(timezone.utc).isoformat()
            except ValueError:
                continue
            publisher = article.get("publisher")
            url = article.get("url")
            records[(symbol, published, headline.strip())] = (
                publisher if isinstance(publisher, str) else None,
                url if isinstance(url, str) else None,
            )
        with _connect(store) as connection:
            connection.executemany(
                """INSERT INTO stock_news_cache
                   (symbol, published_at, headline, publisher, article_url, cached_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(symbol, published_at, headline) DO UPDATE SET
                     publisher=excluded.publisher, article_url=excluded.article_url,
                     cached_at=excluded.cached_at""",
                [(symbol, published, headline, publisher, url, current)
                 for (symbol, published, headline), (publisher, url) in records.items()],
            )
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            connection.execute("DELETE FROM stock_news_cache WHERE published_at < ?", (cutoff,))
        store.set_state("fmp_news_last_success_at", current)
        return {"status": "updated", "provider": "FMP", "articles_saved": len(records)}
    except urllib.error.HTTPError as exc:
        if exc.code in {402, 429}:
            try:
                store.set_state("fmp_news_cooldown_until", current + FAILURE_COOLDOWN_SECONDS)
            except Exception:
                pass
        return {"status": "unavailable", "provider": "FMP", "articles_saved": 0, "error": f"HTTP {exc.code}"}
    except Exception as exc:
        return {"status": "unavailable", "provider": "FMP", "articles_saved": 0, "error": type(exc).__name__}


def cached_news_for_symbol(store: Any, symbol: str, limit: int = 50) -> list[dict[str, Any]]:
    normalized = symbol.strip().upper() if isinstance(symbol, str) else ""
    if not normalized:
        return []
    try:
        with _connect(store) as connection:
            rows = connection.execute(
                """SELECT published_at, headline, publisher, article_url
                   FROM stock_news_cache WHERE symbol = ?
                   ORDER BY published_at DESC LIMIT ?""",
                (normalized, max(1, min(int(limit), 100))),
            ).fetchall()
    except (OSError, RuntimeError) + DB_ERRORS:
        return []
    return [
        {"published_at": published, "headline": headline, "publisher": publisher, "url": url}
        for published, headline, publisher, url in rows
    ]
