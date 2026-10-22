"""Native Momentum rule evaluator; uploaded Python is never executed."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
DEFAULT_CONFIG = {"session_start": "04:00", "session_end": "09:30", "price_min": 1.0, "price_max": 20.0, "gap_min_pct": 5.0, "rvol_min": 5.0, "float_max": 20_000_000, "ema_period": 20, "max_first_candles": 1, "macd_fast": 12, "macd_slow": 26, "macd_signal": 9, "macd_require_above_signal": False, "news_required": True, "news_max_age_hours": 16, "news_require_keyword": False, "min_bars": 35}
CATALYSTS = {"earnings": ("earnings", "resultados", "revenue", "guidance"), "fda": ("fda", "clinical trial", "ensayo clínico", "aprobación"), "contract": ("contract", "award", "agreement", "contrato"), "merger": ("merger", "acquisition", "fusión", "adquisición"), "partnership": ("partnership", "collaboration", "alianza", "colaboración")}


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value == value and abs(value) != float("inf") else None


def _time(value: Any) -> datetime | None:
    try:
        if isinstance(value, datetime):
            result = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            result = datetime.fromtimestamp(value / (1000 if value > 10**11 else 1), timezone.utc)
        elif isinstance(value, str):
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            return None
        return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def _ema(values: list[float], period: int) -> list[float]:
    alpha = 2 / (period + 1)
    result: list[float] = []
    for value in values:
        result.append(value if not result else alpha * value + (1 - alpha) * result[-1])
    return result


def _macd(values: list[float], fast: int, slow: int, signal: int) -> tuple[list[float], list[float]]:
    line = [a - b for a, b in zip(_ema(values, fast), _ema(values, slow))]
    return line, _ema(line, signal)


def evaluate_momentum(candles: list[dict[str, Any]], context: dict[str, Any], config: dict[str, Any] | None = None, *, now: datetime | None = None) -> dict[str, Any]:
    """Evaluate the provided Momentum conditions against candles and explicit context.

    Context keys: symbol, prev_close, avg_volume_30d, avg_premarket_volume_30d,
    float_shares, news. Candles use datetime/timestamp/candle_time_ms, close, volume.
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    bars = []
    for item in candles if isinstance(candles, list) else []:
        if not isinstance(item, dict):
            continue
        stamp = _time(item.get("datetime", item.get("timestamp", item.get("candle_time_ms"))))
        close, volume = _num(item.get("close")), _num(item.get("volume"))
        if stamp and close is not None and close > 0 and volume is not None and volume >= 0:
            bars.append((stamp, close, volume))
    bars.sort(key=lambda x: x[0])
    closes = [b[1] for b in bars]
    checks: dict[str, bool] = {}
    reasons: list[str] = []
    metrics: dict[str, Any] = {}

    def check(key: str, ok: bool, message: str) -> None:
        checks[key] = bool(ok)
        if not ok:
            reasons.append(message)

    check("history", len(bars) >= int(cfg["min_bars"]), f"Se requieren {cfg['min_bars']} velas; disponibles {len(bars)}.")
    price = closes[-1] if closes else _num(context.get("price"))
    previous = _num(context.get("prev_close", context.get("previous_close")))
    gap = (price - previous) / previous * 100 if price is not None and previous is not None and previous > 0 else None
    metrics.update(price=price, previous_close=previous, gap_pct=round(gap, 4) if gap is not None else None)
    check("price", price is not None and cfg["price_min"] <= price <= cfg["price_max"], "Precio fuera del rango configurado o no disponible.")
    check("gap", gap is not None and gap >= cfg["gap_min_pct"], f"GAP inferior a {cfg['gap_min_pct']}% o cierre previo desconocido.")

    if bars:
        local = bars[-1][0].astimezone(NY)
        sh, sm = map(int, cfg["session_start"].split(":")); eh, em = map(int, cfg["session_end"].split(":"))
        minute = local.hour * 60 + local.minute
        session_ok = sh * 60 + sm <= minute < eh * 60 + em
    else:
        local, session_ok = None, False
    metrics["last_candle_time"] = local.isoformat() if local else None
    check("session", session_ok, "La última vela está fuera de la sesión configurada.")

    day_volume = None
    if bars:
        local_day = bars[-1][0].astimezone(NY).date()
        day_volume = sum(v for t, _, v in bars if t.astimezone(NY).date() == local_day)
    baseline = _num(context.get("avg_premarket_volume_30d")) or _num(context.get("avg_volume_30d"))
    rvol = day_volume / baseline if day_volume is not None and baseline is not None and baseline > 0 else None
    metrics.update(today_volume=day_volume, volume_baseline=baseline, rvol=round(rvol, 4) if rvol is not None else None)
    check("rvol", rvol is not None and rvol >= cfg["rvol_min"], f"RVOL inferior a {cfg['rvol_min']} o no hay volumen base.")

    float_shares = _num(context.get("float_shares")); metrics["float_shares"] = float_shares
    check("float", float_shares is not None and 0 < float_shares < cfg["float_max"], f"FLOAT desconocido o no inferior a {cfg['float_max']}.")

    period = max(1, int(cfg["ema_period"])); ema = _ema(closes, period) if closes else []
    streak = 0
    for close, avg in zip(reversed(closes), reversed(ema)):
        if close > avg: streak += 1
        else: break
    prior_below = any(c <= e for c, e in zip(closes[:-1], ema[:-1]))
    first = bool(closes and len(closes) >= period and closes[-1] > ema[-1] and streak == int(cfg["max_first_candles"]) and prior_below)
    metrics.update(ema=round(ema[-1], 6) if ema else None, consecutive_closes_above_ema=streak)
    check("first_candle_above_ema", first, "No es la primera vela consecutiva cerrando por encima de la EMA.")

    fast, slow, signal = int(cfg["macd_fast"]), int(cfg["macd_slow"]), int(cfg["macd_signal"])
    if closes and min(fast, slow, signal) > 0:
        line, sig = _macd(closes, fast, slow, signal); macd, macd_signal = line[-1], sig[-1]
    else:
        macd = macd_signal = None
    macd_ok = macd is not None and macd > 0 and (not cfg["macd_require_above_signal"] or (macd_signal is not None and macd > macd_signal))
    metrics.update(macd=round(macd, 6) if macd is not None else None, macd_signal=round(macd_signal, 6) if macd_signal is not None else None)
    check("macd", macd_ok, "MACD no está por encima de cero o no cumple su señal.")

    cutoff = current - timedelta(hours=float(cfg["news_max_age_hours"]))
    fresh = []
    news = context.get("news")
    if isinstance(news, list):
        for item in news:
            if not isinstance(item, dict): continue
            published = _time(item.get("published_at", item.get("datetime", item.get("timestamp"))))
            headline = item.get("headline", item.get("title", ""))
            if published and isinstance(headline, str) and cutoff <= published <= current:
                text = headline.casefold()
                category = next((name for name, words in CATALYSTS.items() if any(word in text for word in words)), "otro")
                fresh.append((published, category))
    news_ok = (bool(fresh) if cfg["news_required"] else True) and (not cfg["news_require_keyword"] or any(cat != "otro" for _, cat in fresh))
    metrics.update(news_count=len(fresh), news_catalysts=[cat for _, cat in fresh])
    check("news", news_ok, "No hay noticias recientes que cumplan los requisitos configurados.")
    return {"symbol": str(context.get("symbol", "")).strip().upper() or None, "passed": not reasons, "checks": checks, "reasons": reasons, "metrics": metrics, "config": cfg, "evaluated_at": current.isoformat()}
