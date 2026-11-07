"""Vectorized technical analysis for completed Schwab OHLCV candles."""
from __future__ import annotations

from typing import Any
import math
import numpy as np


def _valid_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _ema(values: np.ndarray, period: int) -> np.ndarray:
    """EMA recurrence over a contiguous NumPy array."""
    alpha = 2.0 / (period + 1.0)
    result = np.empty(values.size, dtype=np.float64)
    result[0] = values[0]
    for index in range(1, values.size):
        result[index] = alpha * values[index] + (1.0 - alpha) * result[index - 1]
    return result


def _rolling_mean(values: np.ndarray, window: int) -> np.ndarray:
    result = np.full(values.size, np.nan, dtype=np.float64)
    if values.size >= window:
        sums = np.cumsum(np.insert(values, 0, 0.0), dtype=np.float64)
        result[window - 1:] = (sums[window:] - sums[:-window]) / window
    return result


def analyze_candles(
    candles: list[dict[str, Any]] | None,
    *,
    quote_time_ms: int | float | None = None,
    breakout_lookback: int = 20,
    volume_multiple: float = 1.5,
) -> dict[str, Any]:
    """Compute EMA, MACD, Bollinger and volume metrics; signal only on closed-bar breakout."""
    empty = {
        "ready": False, "signal": False, "reason": "insufficient_completed_candles",
        "candles_used": 0, "ema3": None, "ema9": None, "ema20": None,
        "ema50": None, "ema200": None, "macd": None, "macd_signal": None,
        "macd_histogram": None, "bollinger_upper": None, "bollinger_middle": None,
        "bollinger_lower": None, "volume_average_20": None, "volume_ratio": None,
        "breakout_level": None,
    }
    if not isinstance(candles, list):
        return empty
    cutoff_minute = (
        int(float(quote_time_ms) // 60_000)
        if _valid_number(quote_time_ms) and float(quote_time_ms) > 0 else None
    )
    valid = []
    for item in candles:
        if not isinstance(item, dict):
            continue
        stamp = item.get("candle_time_ms")
        fields = [item.get(key) for key in ("open", "high", "low", "close", "volume")]
        if not _valid_number(stamp) or float(stamp) <= 0 or not all(_valid_number(v) for v in fields):
            continue
        opened, high, low, close, volume = map(float, fields)
        if min(opened, high, low, close) <= 0 or volume < 0:
            continue
        if high < max(opened, close, low) or low > min(opened, close, high):
            continue
        minute = int(float(stamp) // 60_000)
        if cutoff_minute is not None and minute >= cutoff_minute:
            continue
        valid.append((minute, opened, high, low, close, volume))
    if not valid:
        return empty
    # Deduplicate provider revisions of the same minute and keep chronological order.
    by_minute = {row[0]: row for row in valid}
    data = [by_minute[key] for key in sorted(by_minute)]
    if len(data) < 200:
        return {**empty, "candles_used": len(data)}

    highs = np.asarray([row[2] for row in data], dtype=np.float64)
    closes = np.asarray([row[4] for row in data], dtype=np.float64)
    volumes = np.asarray([row[5] for row in data], dtype=np.float64)
    ema_values = {period: _ema(closes, period) for period in (3, 9, 20, 50, 200)}
    macd_line = _ema(closes, 12) - _ema(closes, 26)
    macd_signal_line = _ema(macd_line, 9)
    histogram = macd_line - macd_signal_line
    bb_middle = _rolling_mean(closes, 20)
    mean_square = _rolling_mean(closes * closes, 20)
    bb_std = np.sqrt(np.maximum(mean_square - bb_middle * bb_middle, 0.0))

    lookback = max(2, min(int(breakout_lookback), 100))
    prior_highs = highs[-(lookback + 1):-1]
    prior_volumes = volumes[-21:-1]
    if prior_highs.size != lookback or prior_volumes.size != 20:
        return {**empty, "candles_used": len(data)}
    breakout_level = float(np.max(prior_highs))
    average_volume = float(np.mean(prior_volumes))
    volume_ratio = float(volumes[-1] / average_volume) if average_volume > 0 else None
    signal = bool(
        closes[-1] > breakout_level
        and volume_ratio is not None
        and volume_ratio >= float(volume_multiple)
        and closes[-1] > ema_values[20][-1]
        and histogram[-1] > 0
    )
    def last(array: np.ndarray) -> float | None:
        value = float(array[-1])
        return value if math.isfinite(value) else None
    return {
        "ready": True, "signal": signal,
        "reason": "confirmed_close_breakout" if signal else "no_confirmed_breakout",
        "candles_used": len(data), "last_completed_candle_minute": int(data[-1][0]),
        "ema3": round(last(ema_values[3]), 6), "ema9": round(last(ema_values[9]), 6),
        "ema20": round(last(ema_values[20]), 6), "ema50": round(last(ema_values[50]), 6),
        "ema200": round(last(ema_values[200]), 6), "macd": round(last(macd_line), 6),
        "macd_signal": round(last(macd_signal_line), 6), "macd_histogram": round(last(histogram), 6),
        "bollinger_upper": round(last(bb_middle + 2.0 * bb_std), 6),
        "bollinger_middle": round(last(bb_middle), 6),
        "bollinger_lower": round(last(bb_middle - 2.0 * bb_std), 6),
        "volume_average_20": round(average_volume, 2),
        "volume_ratio": round(volume_ratio, 4) if volume_ratio is not None else None,
        "breakout_level": round(breakout_level, 6),
    }
