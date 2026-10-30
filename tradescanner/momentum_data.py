"""Bounded, cached auxiliary market context for Momentum; missing values stay missing."""
from __future__ import annotations

import json
import os
import time
import urllib.error
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from urllib.parse import urlencode, quote_plus
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
        # Exclude today's still-forming daily candle: the baseline must use
        # completed trading sessions only.
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today_et = datetime.now(ZoneInfo("America/New_York")).date()
        completed = []
        for row in rows:
            if not isinstance(row, dict): continue
            stamp = row.get("datetime")
            volume = row.get("volume")
            try:
                candle_date = datetime.fromtimestamp(float(stamp) / 1000, ZoneInfo("America/New_York")).date()
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            if candle_date >= today_et:
                continue
            if isinstance(volume, (int, float)) and not isinstance(volume, bool) and volume > 0:
                completed.append(float(volume))
        values = completed[-30:]
        if len(values) < 10:
            return {"status": "insufficient_history", "average": None, "sessions": len(values)}
        average = sum(values) / len(values)
        store.set_state(key, {"average": average, "sessions": len(values), "cached_at": current})
        return {"status": "updated", "average": average, "sessions": len(values)}
    except Exception as exc:
        return {"status": "unavailable", "average": None, "error": type(exc).__name__}


def recent_stock_news(store: Any, symbol: str, *, now: float | None = None) -> dict[str, Any]:
    """Read cached headlines or query Google News RSS; FMP is deliberately disabled."""
    current = time.time() if now is None else now
    key = _cache_key("news", symbol)
    cached = store.get_state(key, {})
    if isinstance(cached, dict):
        age = current - float(cached.get("cached_at", 0) or 0)
        if age < NEWS_CACHE_SECONDS and isinstance(cached.get("news"), list):
            return {"status": "cached", "news": cached["news"]}
        if age < NEWS_COOLDOWN_SECONDS and cached.get("status") == "unavailable":
            return {"status": "cooldown", "news": [], "error": cached.get("error")}

    result: dict[str, Any]
    try:
        rss_url = (
            "https://news.google.com/rss/search?q="
            + quote_plus(symbol.strip().upper() + " stock when:1d")
            + "&hl=en-US&gl=US&ceid=US:en"
        )
        rss_request = Request(
            rss_url,
            headers={"Accept": "application/rss+xml, application/xml", "User-Agent": "TradeScanner/1.0"},
        )
        with urlopen(rss_request, timeout=6) as response:
            root = ET.fromstring(response.read())
        rss_news = []
        for item in root.findall("./channel/item")[:20]:
            title = item.findtext("title")
            published = item.findtext("pubDate")
            if not title or not published:
                continue
            try:
                stamp = parsedate_to_datetime(published)
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                rss_news.append({
                    "published_at": stamp.astimezone(timezone.utc).isoformat(),
                    "headline": title.strip(),
                })
            except (TypeError, ValueError, OverflowError):
                continue
        result = {
            "status": "ok_google_news_rss" if rss_news else "empty",
            "news": rss_news,
            "provider": "Google News RSS",
        }
    except Exception as exc:
        result = {"status": "unavailable", "news": [], "error": type(exc).__name__}

    store.set_state(key, {**result, "cached_at": current})
    return result
