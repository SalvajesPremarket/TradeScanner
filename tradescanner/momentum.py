"""Safe, provider-agnostic implementation of the user's Momentum premarket scan.

This module never invents missing market context. It returns explicit rejection reasons
when average daily volume, float, or fresh headlines are unavailable.
"""
from __future__ import annotations

from datetime import datetime, timedelta, time as dtime
from math import isfinite
from zoneinfo import ZoneInfo
from typing import Any

STRATEGY_NAME = "Momentum"
ET = ZoneInfo("America/New_York")
CONFIG: dict[str, Any] = {
    "timezone": "America/New_York", "session_start": dtime(4, 0), "session_end": dtime(16, 0),
    "price_min": 1.0, "price_max": 20.0, "gap_min_pct": 2.0,
    "rvol_min": 2.0, "float_max": 20_000_000, "volume_min": 0, "ema_period": 20, "max_first_candles": 1,
    "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
    "macd_require_above_signal": False, "news_required": True, "news_max_age_hours": 16,
    "news_require_keyword": False, "min_bars": 35,
}
CATALYSTS = {
    "earnings": ("earnings", "quarter", "q1", "q2", "q3", "q4", "revenue", "eps", "guidance", "resultados", "ganancias", "trimestre"),
    "fda": ("fda", "phase", "trial", "approval", "approves", "clearance", "nda", "bla", "aprobacion", "aprobación", "ensayo"),
    "contrato": ("contract", "award", "government", "department of", "pentagon", "dod", "contrato", "gobierno"),
    "fusion": ("merger", "acquisition", "acquire", "acquires", "buyout", "takeover", "fusion", "fusión", "adquisicion", "adquisición"),
    "alianza": ("partnership", "partners", "collaboration", "agreement", "alliance", "alianza", "acuerdo", "asociacion", "asociación"),
}


def _ema(values: list[float], period: int) -> list[float]:
    alpha = 2.0 / (period + 1)
    result: list[float] = []
    for value in values:
        result.append(value if not result else alpha * value + (1 - alpha) * result[-1])
    return result


def _classify_news(headline: str) -> str | None:
    lowered = headline.lower()
    return next((category for category, words in CATALYSTS.items() if any(word in lowered for word in words)), None)


def scan(candles: list[dict[str, Any]] | None, ctx: dict[str, Any], cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    config = {**CONFIG, **(cfg or {})}
    out: dict[str, Any] = {"passed": False, "strategy": STRATEGY_NAME, "symbol": ctx.get("symbol"), "metrics": {}, "reasons": []}
    try:
        if not isinstance(candles, list) or len(candles) < int(config["min_bars"]):
            out["reasons"].append("velas: pocas velas OHLCV para EMA20/MACD")
            return out
        valid = []
        for candle in candles:
            try:
                stamp = candle.get("candle_time_ms")
                close = float(candle["close"])
                volume = float(candle["volume"])
                if stamp is None or not isfinite(close) or close <= 0 or not isfinite(volume) or volume < 0:
                    continue
                ts = datetime.fromtimestamp(float(stamp) / 1000, ET)
                valid.append((ts, close, volume))
            except (KeyError, TypeError, ValueError, OverflowError, OSError):
                continue
        valid.sort(key=lambda item: item[0])
        if len(valid) < int(config["min_bars"]):
            out["reasons"].append("velas: historial válido insuficiente")
            return out
        last_ts, candle_price, _ = valid[-1]
        # Current quote fields are authoritative for live price/session checks.
        # Historical candles can be days old and must never override a fresh quote.
        quote_time_ms = ctx.get("quote_time_ms")
        quote_time = last_ts
        if isinstance(quote_time_ms, (int, float)) and not isinstance(quote_time_ms, bool) and quote_time_ms > 0:
            quote_time = datetime.fromtimestamp(float(quote_time_ms) / 1000, ET)
        quote_price = ctx.get("quote_price")
        price = float(quote_price) if isinstance(quote_price, (int, float)) and not isinstance(quote_price, bool) and isfinite(float(quote_price)) and float(quote_price) > 0 else candle_price
        local_today = quote_time.date()
        current_volume = ctx.get("current_volume")
        if isinstance(current_volume, (int, float)) and not isinstance(current_volume, bool) and isfinite(float(current_volume)) and float(current_volume) >= 0:
            today_volume = float(current_volume)
        else:
            today_volume = sum(volume for ts, _, volume in valid if ts.date() == local_today)
        closes = [close for _, close, _ in valid]
        ema20 = _ema(closes, int(config["ema_period"]))
        macd_line = [a - b for a, b in zip(_ema(closes, int(config["macd_fast"])), _ema(closes, int(config["macd_slow"])))]
        macd_signal = _ema(macd_line, int(config["macd_signal"]))
        # Session eligibility must use the timestamp of the current observed quote,
        # not the timestamp of the newest historical candle (which may be stale).
        local_time = quote_time.timetz().replace(tzinfo=None)
        checks: dict[str, tuple[bool, Any, str]] = {}
        checks["hora"] = (config["session_start"] <= local_time < config["session_end"], local_time.strftime("%H:%M"), f"fuera de {config['session_start'].strftime('%H:%M')}-{config['session_end'].strftime('%H:%M')} ET")
        checks["precio"] = (config["price_min"] <= price <= config["price_max"], round(price, 4), f"precio fuera de ${config['price_min']}-${config['price_max']}")
        prev_close = ctx.get("prev_close")
        gap = ((price - float(prev_close)) / float(prev_close) * 100) if isinstance(prev_close, (int, float)) and not isinstance(prev_close, bool) and prev_close > 0 else None
        checks["gap_pct"] = (gap is not None and gap >= config["gap_min_pct"], round(gap, 2) if gap is not None else None, "sin cierre anterior" if gap is None else f"gap < {config['gap_min_pct']}%")
        avg_volume = ctx.get("avg_volume_30d")
        rvol = today_volume / float(avg_volume) if isinstance(avg_volume, (int, float)) and not isinstance(avg_volume, bool) and avg_volume > 0 else None
        checks["rvol"] = (rvol is not None and rvol >= config["rvol_min"], round(rvol, 2) if rvol is not None else None, "sin volumen promedio diario 30d" if rvol is None else f"RVol < {config['rvol_min']}x")
        checks["volumen_minimo"] = (today_volume >= config["volume_min"], int(today_volume), f"volumen < {config['volume_min']:,}")
        float_shares = ctx.get("float_shares")
        float_ok = isinstance(float_shares, (int, float)) and not isinstance(float_shares, bool) and 0 < float_shares < config["float_max"]
        checks["float"] = (float_ok, int(float_shares) if float_ok else None, "float desconocido" if not isinstance(float_shares, (int, float)) or not float_shares else f"float >= {config['float_max']:,}")
        above = [close > ema for close, ema in zip(closes, ema20)]
        streak = 0
        for flag in reversed(above):
            if not flag: break
            streak += 1
        checks["velas_sobre_ema20"] = (0 < streak <= config["max_first_candles"] and streak < len(above), streak, "sin cruce reciente sobre EMA20" if streak == 0 or streak >= len(above) else f"ya lleva {streak} velas sobre EMA20 (máx. {config['max_first_candles']})")
        macd = macd_line[-1]
        macd_ok = macd > 0 and (not config["macd_require_above_signal"] or macd > macd_signal[-1])
        checks["macd"] = (macd_ok, round(macd, 5), "MACD no positivo")
        news = ctx.get("news")
        now = quote_time
        fresh_news = []
        if isinstance(news, list):
            for item in news:
                if not isinstance(item, dict) or not item.get("published_at") or not isinstance(item.get("headline"), str):
                    continue
                try:
                    published = datetime.fromisoformat(str(item["published_at"]).replace("Z", "+00:00"))
                    if published.tzinfo is None: published = published.replace(tzinfo=ET)
                    published = published.astimezone(ET)
                    if published >= now - timedelta(hours=float(config["news_max_age_hours"])):
                        category = _classify_news(item["headline"])
                        if not config["news_require_keyword"] or category:
                            fresh_news.append((published, category or "otro", item["headline"]))
                except (TypeError, ValueError, OverflowError):
                    continue
        fresh_news.sort(key=lambda item: item[0], reverse=True)
        news_value = {"categoria": fresh_news[0][1], "titular": fresh_news[0][2][:120], "published_at": fresh_news[0][0].isoformat()} if fresh_news else None
        checks["noticia"] = (bool(fresh_news) or not config["news_required"], news_value, "sin noticia fresca verificable")
        for name, (passed, value, reason) in checks.items():
            out["metrics"][name] = value
            if not passed: out["reasons"].append(f"{name}: {reason}")
        out["metrics"]["ema20"] = round(ema20[-1], 4)
        out["metrics"]["volumen_hoy"] = int(today_volume)
        out["metrics"]["vela_ultimo_timestamp"] = last_ts.isoformat()
        out["passed"] = not out["reasons"]
    except Exception as exc:
        out["reasons"].append("error de evaluación: " + type(exc).__name__)
    return out
