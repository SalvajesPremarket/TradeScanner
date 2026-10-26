"""OpenGapFranco: daily Bollinger-band proximity plus MACD histogram streak scan.

Only provider-supplied daily OHLCV and observed quote context are used. No uploaded
strategy source is executed and no orders are placed.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, time as dtime
from typing import Any
from zoneinfo import ZoneInfo

from tradescanner.candles import normalize_price_history
from tradescanner.schwab import get_chart_history

ET = ZoneInfo("America/New_York")
SESSION_START = dtime(4, 0)
SESSION_END = dtime(16, 0)
PRICE_MIN = 1.0
PRICE_MAX = 20.0
VOLUME_MAX = 1_000_000
FLOAT_MAX = 50_000_000
BB_PERIOD = 20
BB_STDDEV = 2.0
BB_NEAR_PCT = 0.01
MIN_MACD_STREAK = 5
MAX_MACD_STREAK = 7
_HISTORY_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
CACHE_SECONDS = 900


def _daily_candles(symbol: str) -> list[dict[str, Any]]:
    now = time.monotonic()
    cached = _HISTORY_CACHE.get(symbol)
    if cached and now - cached[0] < CACHE_SECONDS:
        return cached[1]
    payload = get_chart_history(symbol, "day")
    candles = normalize_price_history(symbol, payload)
    candles.sort(key=lambda item: item["candle_time_ms"])
    # Use completed daily bars for indicators; the live quote supplies current price/volume.
    today = datetime.now(ET).date()
    candles = [
        candle for candle in candles
        if datetime.fromtimestamp(candle["candle_time_ms"] / 1000, ET).date() < today
    ]
    _HISTORY_CACHE[symbol] = (now, candles)
    return candles


def _ema(values: list[float], period: int) -> list[float]:
    alpha = 2.0 / (period + 1.0)
    result: list[float] = []
    for value in values:
        result.append(value if not result else alpha * value + (1.0 - alpha) * result[-1])
    return result


def _histogram_streak(values: list[float]) -> tuple[str | None, int, float | None]:
    macd_line = [a - b for a, b in zip(_ema(values, 12), _ema(values, 26))]
    signal = _ema(macd_line, 9)
    histogram = [line - sig for line, sig in zip(macd_line, signal)]
    if not histogram:
        return None, 0, None
    positive = histogram[-1] > 0
    streak = 0
    for value in reversed(histogram):
        if (value > 0) != positive or value == 0:
            break
        streak += 1
    return ("positive" if positive else "negative"), streak, histogram[-1]


def evaluate(row: dict[str, Any]) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").strip().upper()
    result: dict[str, Any] = {
        "passed": False,
        "strategy": "OpenGapFranco",
        "symbol": symbol,
        "metrics": {},
        "reasons": [],
        "data_source": "schwab_quote_and_daily_history",
        "timeframe": "day",
    }
    try:
        stamp = row.get("quote_time_ms")
        if not isinstance(stamp, (int, float)) or stamp <= 0:
            result["reasons"].append("hora: falta hora de cotización verificable")
            return result
        quote_time = datetime.fromtimestamp(float(stamp) / 1000, ET)
        local_time = quote_time.timetz().replace(tzinfo=None)
        price = row.get("price")
        volume = row.get("volume")
        float_shares = row.get("float_shares")
        checks = [
            (SESSION_START <= local_time < SESSION_END, "hora", "fuera de 04:00-16:00 ET"),
            (isinstance(price, (int, float)) and not isinstance(price, bool) and PRICE_MIN <= price <= PRICE_MAX,
             "precio", "precio fuera de $1-$20"),
            (isinstance(volume, (int, float)) and not isinstance(volume, bool) and 0 <= volume < VOLUME_MAX,
             "volumen", "volumen desconocido o no menor de 1,000,000"),
            (isinstance(float_shares, (int, float)) and not isinstance(float_shares, bool) and 0 < float_shares < FLOAT_MAX,
             "float", "flotación desconocida o no menor de 50,000,000"),
        ]
        for ok, name, reason in checks:
            result["metrics"][name] = (
                round(float(price), 4) if name == "precio" and isinstance(price, (int, float))
                else int(volume) if name == "volumen" and isinstance(volume, (int, float))
                else int(float_shares) if name == "float" and isinstance(float_shares, (int, float))
                else local_time.strftime("%H:%M") if name == "hora" else None
            )
            if not ok:
                result["reasons"].append(f"{name}: {reason}")

        candles = _daily_candles(symbol)
        closes = [float(candle["close"]) for candle in candles if candle.get("close")]
        if len(closes) < 35:
            result["reasons"].append("velas diarias: historial insuficiente para Bollinger/MACD")
            return result
        sample = closes[-BB_PERIOD:]
        mean = sum(sample) / BB_PERIOD
        variance = sum((value - mean) ** 2 for value in sample) / BB_PERIOD
        deviation = math.sqrt(variance)
        upper = mean + BB_STDDEV * deviation
        lower = mean - BB_STDDEV * deviation
        macd_side, streak, histogram = _histogram_streak(closes)
        top_match = isinstance(price, (int, float)) and price >= upper * (1.0 - BB_NEAR_PCT) and macd_side == "positive" and MIN_MACD_STREAK <= streak <= MAX_MACD_STREAK
        bottom_match = isinstance(price, (int, float)) and price <= lower * (1.0 + BB_NEAR_PCT) and macd_side == "negative" and MIN_MACD_STREAK <= streak <= MAX_MACD_STREAK
        result["metrics"].update({
            "bollinger_top": round(upper, 4),
            "bollinger_bottom": round(lower, 4),
            "bollinger_near_pct": 1.0,
            "macd_histogram": round(histogram, 6) if histogram is not None else None,
            "macd_direction": macd_side,
            "macd_streak_bars": streak,
            "daily_candles": len(closes),
            "signal_side": "top" if top_match else "bottom" if bottom_match else None,
        })
        if not (top_match or bottom_match):
            result["reasons"].append("Bollinger/MACD: no cumple banda y racha MACD de 5, 6 o 7 barras")
        result["passed"] = not result["reasons"]
    except Exception as exc:
        result["reasons"].append("error de evaluación: " + type(exc).__name__)
    return result
