"""Pure, provider-agnostic normalization and filter logic for TradeScanner."""
from __future__ import annotations

from typing import Any


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def normalize_schwab_quotes(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize only fields actually present in Schwab's response.

    Missing values stay None. No price, float, volume baseline, or candle is invented.
    """
    rows: list[dict[str, Any]] = []
    for symbol, record in payload.items():
        if not isinstance(record, dict):
            continue
        quote = record.get("quote")
        reference = record.get("reference")
        quote = quote if isinstance(quote, dict) else {}
        reference = reference if isinstance(reference, dict) else {}

        last = _number(quote.get("lastPrice"))
        previous_close = _number(quote.get("closePrice"))
        gap_pct = None
        if last is not None and previous_close is not None and previous_close > 0:
            gap_pct = round(((last / previous_close) - 1.0) * 100.0, 10)

        rows.append({
            "symbol": str(symbol).strip().upper(),
            "company": reference.get("description"),
            "price": last,
            "previous_close": previous_close,
            "change": _number(quote.get("netChange")),
            "change_pct": _number(quote.get("netPercentChange")),
            "gap_pct": gap_pct,
            "volume": _number(quote.get("totalVolume")),
            "float_shares": None,
            "rvol": None,
            "sector": None,
            "exchange": reference.get("exchangeName"),
            "quote_time_ms": _number(quote.get("quoteTimeInLong")),
            "data_source": "schwab",
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
