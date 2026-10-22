"""Bounded, cached auxiliary market context for Momentum; missing values stay missing."""
from __future__ import annotations

import json
import os
import time
import urllib.error
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Any

from tradescanner.schwab import get_daily_history

DAILY_CACHE_SECONDS = 24 * 60 * 60
NEWS_CACHE_SECONDS = 30 * 60
NEWS_COOLDOWN_SECONDS = 6 * 60 * 60


def _cache_key(kind: str, symbol: str) -> str:
    return f"momentum_{kind}_{symbol.strip().upper()}"


def average_daily_volume_30d(store: Any, symbol: str, *, now: float | None = None) -> dict[str, Any]:
    current = time.time() if now is None else now
    key = _cache_key("avg_volume_30d", symbol)
    cached = store.get_state(key, {})
    if isinstance(cached, dict) and isinstance(cached.get("average"), (int, float)) and current - float(cached.get("cached_at", 0)) < DAILY_CACHE_SECONDS:
        return {"status": "cached", "average": cached["average"], "sessions": cached.get("sessions", 0)}
    try:
        payload = get_daily_history(symbol)
        rows = payload.get("candles", [])
        values = []
        for row in rows:
            if not isinstance(row, dict): continue
            volume = row.get("volume")
            if isinstance(volume, (int, float)) and not isinstance(volume, bool) and volume > 0:
                values.append(float(volume))
        values = values[-30:]
        if len(values) < 10:
            return {"status": "insufficient_history", "average": None, "sessions": len(values)}
        average = sum(values) / len(values)
        store.set_state(key, {"average": average, "sessions": len(values), "cached_at": current})
        return {"status": "updated", "average": average, "sessions": len(values)}
    except Exception as exc:
        return {"status": "unavailable", "average": None, "error": type(exc).__name__}


def recent_stock_news(store: Any, symbol: str, *, now: float | None = None) -> dict[str, Any]:
    current = time.time() if now is None else now
    key = _cache_key("news", symbol)
    cached = store.get_state(key, {})
    if isinstance(cached, dict):
        age = current - float(cached.get("cached_at", 0) or 0)
        if age < NEWS_CACHE_SECONDS and isinstance(cached.get("news"), list):
            return {"status": "cached", "news": cached["news"]}
        if age < NEWS_COOLDOWN_SECONDS and cached.get("status") == "unavailable":
            return {"status": "cooldown", "news": [], "error": cached.get("error")}
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        result = {"status": "not_configured", "news": [], "error": "FMP_API_KEY no está configurada"}
        store.set_state(key, {**result, "cached_at": current})
        return result
    query = urlencode({"symbols": symbol.strip().upper(), "limit": 20, "apikey": api_key})
    request = Request(
        "https://financialmodelingprep.com/stable/news/stock-latest?" + query,
        headers={"Accept": "application/json", "User-Agent": "TradeScanner/1.0"},
    )
    try:
        with urlopen(request, timeout=6) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, list):
            raise ValueError("news_response_malformed")
        news = []
        for item in payload:
            if not isinstance(item, dict): continue
            published = item.get("publishedDate") or item.get("published_at") or item.get("date")
            headline = item.get("title") or item.get("headline")
            if not isinstance(published, str) or not isinstance(headline, str) or not headline.strip(): continue
            news.append({"published_at": published, "headline": headline.strip()})
        result = {"status": "ok" if news else "empty", "news": news}
        store.set_state(key, {**result, "cached_at": current})
        return result
    except urllib.error.HTTPError as exc:
        result = {"status": "unavailable", "news": [], "error": f"HTTP {exc.code}"}
    except Exception as exc:
        result = {"status": "unavailable", "news": [], "error": type(exc).__name__}
    store.set_state(key, {**result, "cached_at": current})
    return result
