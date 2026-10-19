"""Pure, provider-agnostic normalization and filter logic for TradeScanner."""
from __future__ import annotations

import math
import time
from typing import Any


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def normalize_schwab_quotes(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize only fields actually present in Schwab's response.

    Missing values stay None. Shares outstanding are never mislabeled as float.
    """
    rows: list[dict[str, Any]] = []
    for symbol, record in payload.items():
        if not isinstance(record, dict):
            continue
        quote = record.get("quote")
        reference = record.get("reference")
        fundamental = record.get("fundamental")
        quote = quote if isinstance(quote, dict) else {}
        reference = reference if isinstance(reference, dict) else {}
        fundamental = fundamental if isinstance(fundamental, dict) else {}

        last = _number(quote.get("lastPrice"))
        previous_close = _number(quote.get("closePrice"))
        volume = _number(quote.get("totalVolume"))
        # Schwab REST quote responses document `quoteTime` (epoch milliseconds).
        # Keep compatibility with payloads that expose the older `quoteTimeInLong` name.
        quote_time_ms = _number(quote.get("quoteTime"))
        if quote_time_ms is None:
            quote_time_ms = _number(quote.get("quoteTimeInLong"))
        if last is not None and last <= 0:
            last = None
        if previous_close is not None and previous_close <= 0:
            previous_close = None
        if volume is not None and volume < 0:
            volume = None
        quote_age_seconds = (
            time.time() - quote_time_ms / 1000.0
            if quote_time_ms is not None and quote_time_ms > 0
            else None
        )
        quote_is_fresh = (
            quote_age_seconds is not None
            and -30 <= quote_age_seconds <= 300
        )
        gap_pct = None
        if last is not None and previous_close is not None and previous_close > 0:
            gap_pct = round(((last / previous_close) - 1.0) * 100.0, 10)

        # Prefer a directly supplied float-share count. Schwab payloads may instead
        # expose marketCapFloat (public-float market capitalization); when present,
        # derive an approximate share count from that value and the current price.
        # Never substitute sharesOutstanding, which is a different metric.
        float_shares = _number(fundamental.get("floatShares"))
        float_source = "schwab" if float_shares is not None and float_shares > 0 else None
        float_market_cap = _number(fundamental.get("marketCapFloat"))
        if (
            (float_shares is None or float_shares <= 0)
            and float_market_cap is not None
            and float_market_cap > 0
            and last is not None
            and last > 0
        ):
            float_shares = float_market_cap / last
            float_source = "schwab_market_cap_float_derived"
        if float_shares is not None and float_shares <= 0:
            float_shares = None
            float_source = None
        rows.append({
            "symbol": str(symbol).strip().upper(),
            "company": reference.get("description"),
            "price": last,
            "previous_close": previous_close,
            "change": _number(quote.get("netChange")),
            "change_pct": _number(quote.get("netPercentChange")),
            "gap_pct": gap_pct,
            "volume": volume,
            "float_shares": float_shares,
            "rvol": None,
            "sector": fundamental.get("sector"),
            "exchange": reference.get("exchangeName"),
            "quote_time_ms": quote_time_ms,
            "quote_age_seconds": round(quote_age_seconds, 3) if quote_age_seconds is not None else None,
            "quote_is_fresh": quote_is_fresh,
            "data_source": "schwab",
            "float_source": float_source,
        })
    return rows


def apply_filters(
    rows: list[dict[str, Any]],
    *,
    min_price: float | None = None,
    max_price: float | None = None,
    min_gap_pct: float | None = None,
    max_gap_pct: float | None = None,
    min_rvol: float | None = None,
    min_volume: int | None = None,
) -> list[dict[str, Any]]:
    """Apply only configured filters; a filter excludes rows with missing values."""
    result = []
    for row in rows:
        checks = (
            (min_price, row.get("price"), "min"),
            (max_price, row.get("price"), "max"),
            (min_gap_pct, row.get("gap_pct"), "min"),
            (max_gap_pct, row.get("gap_pct"), "max"),
            (min_rvol, row.get("rvol"), "min"),
            (min_volume, row.get("volume"), "min"),
        )
        accepted = True
        for threshold, actual, mode in checks:
            if threshold is None:
                continue
            if actual is None:
                accepted = False
                break
            if mode == "min" and actual < threshold:
                accepted = False
                break
            if mode == "max" and actual > threshold:
                accepted = False
                break
        if accepted:
            result.append(row)
    return result
