"""Incremental, restart-safe quote scanner for the clean TradeScanner core."""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Callable

from tradescanner.candles import CandleStore, normalize_price_history
from tradescanner.db import DB_ERRORS
from tradescanner.float_provider import apply_cached_float, refresh_float_cache
from tradescanner.infisical_vault import is_configured as infisical_is_configured
from tradescanner.snapshots import BATCH_SIZE, SnapshotStore, collect_snapshots
from tradescanner.schwab import SchwabAPIError, SchwabConfigurationError, get_all_time_daily_history, get_daily_history, get_price_history, get_quotes
from tradescanner.universe import load_symbol_universe
from tradescanner.strategies import active_strategies, is_momentum_trader_program, is_momentum_scan_program, strategy_matches
from tradescanner.momentum_data import recent_stock_news
from tradescanner.opengap_franco import evaluate as evaluate_open_gap_franco

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_multi_session_minute_history(symbol: str) -> dict[str, Any]:
    """Ten calendar days of one-minute extended-hours candles for indicators and RVOL."""
    return get_price_history(symbol, period=10, frequency=1)


_HISTORY_REFRESH_INTERVAL_SECONDS = 60.0
_HISTORY_MAX_QUOTE_LAG_MS = 120_000

UNIVERSE_CACHE_SECONDS = 6 * 60 * 60
_universe_cache: dict[str, Any] | None = None
_universe_cached_at = 0.0
_universe_lock = threading.Lock()
_cycle_lock = threading.Lock()
_HISTORY_MAX_INFLIGHT = 12
_history_fetch_semaphore = threading.BoundedSemaphore(12)
_history_inflight_symbols: set[str] = set()
_history_inflight_lock = threading.Lock()
_history_last_attempt_at: dict[str, float] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _history_lags_quote(row: dict[str, Any], latest_candle_time_ms: int | None) -> bool:
    """True when a fresh quote is more than two minutes ahead of stored OHLCV."""
    quote_time_ms = row.get("quote_time_ms")
    if (
        row.get("quote_is_fresh") is not True
        or isinstance(quote_time_ms, bool)
        or not isinstance(quote_time_ms, (int, float))
        or not math.isfinite(float(quote_time_ms))
        or quote_time_ms <= 0
    ):
        return False
    if latest_candle_time_ms is None:
        return True
    quote_local = datetime.fromtimestamp(float(quote_time_ms) / 1000, ZoneInfo("America/New_York"))
    latest_local = datetime.fromtimestamp(float(latest_candle_time_ms) / 1000, ZoneInfo("America/New_York"))
    # Before 09:30 ET, no same-minute regular-session candle is expected. The
    # last completed prior-session bar remains a valid EMA/MACD reference.
    if quote_local.hour * 60 + quote_local.minute < 9 * 60 + 30:
        if latest_local > quote_local:
            return True
        return (quote_local.date() - latest_local.date()).days > 4
    return float(quote_time_ms) - latest_candle_time_ms > _HISTORY_MAX_QUOTE_LAG_MS


def _ema20_from_candles(candles: list[dict[str, Any]]) -> float | None:
    """Calculate EMA20 from chronologically ordered OHLCV candles."""
    closes: list[float] = []
    for candle in candles:
        try:
            close = float(candle.get("close"))
        except (TypeError, ValueError):
            continue
        if close > 0 and close == close and close != float("inf"):
            closes.append(close)
    if len(closes) < 20:
        return None
    alpha = 2.0 / 21.0
    ema = closes[0]
    for close in closes[1:]:
        ema = alpha * close + (1.0 - alpha) * ema
    return round(ema, 4)

def _fetch_history_in_background(
    symbol: str,
    fetcher: Callable[[str], dict[str, Any]],
    store_path: Any,
    result: dict[str, Any],
) -> None:
    """Fetch and persist history without allowing a slow provider call to block scans."""
    started = time.monotonic()
    try:
        with _history_fetch_semaphore:
            payload = fetcher(symbol)
        candles = normalize_price_history(symbol, payload)
        saved = CandleStore(store_path).save(candles)
        result.update({"candles_count": len(candles), "saved": saved})
        logger.info(
            "Scan cycle history_symbol=%s returned=%s saved=%s duration_seconds=%.3f",
            symbol, len(candles), saved, time.monotonic() - started,
        )
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        logger.warning(
            "Scan cycle history_fetch_failed symbol=%s error_type=%s duration_seconds=%.3f",
            symbol, type(exc).__name__, time.monotonic() - started,
        )
    finally:
        with _history_inflight_lock:
            _history_inflight_symbols.discard(symbol)
        result["done"].set()


def _credentials_configured() -> bool:
    has_schwab_client = all(os.getenv(key, "").strip() for key in (
        "SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET"
    ))
    # The refresh token may live exclusively in Infisical after OAuth bootstrap.
    # Do not block the engine merely because the bootstrap env var is absent.
    has_refresh_token_source = bool(os.getenv("SCHWAB_REFRESH_TOKEN", "").strip()) or infisical_is_configured()
    return has_schwab_client and has_refresh_token_source


def _load_cached_universe(loader: Callable[[], dict[str, Any]] | None = None) -> dict[str, Any]:
    global _universe_cache, _universe_cached_at
    if loader is not None:
        return loader()
    now = time.monotonic()
    with _universe_lock:
        if _universe_cache is None or now - _universe_cached_at >= UNIVERSE_CACHE_SECONDS:
            _universe_cache = load_symbol_universe()
            _universe_cached_at = now
        return _universe_cache


def record_engine_status(store: SnapshotStore, **updates: Any) -> dict[str, Any]:
    previous = store.get_state("engine_status", {})
    if not isinstance(previous, dict):
        previous = {}
    state = {**previous, **updates, "updated_at": _now()}
    store.set_state("engine_status", state)
    return state


def get_engine_status(store: SnapshotStore | None = None) -> dict[str, Any]:
    state = (store or SnapshotStore()).get_state("engine_status", {})
    return state if isinstance(state, dict) else {}




def _evaluate_momentum_strategy(store: SnapshotStore, batch: list[str], strategy: dict[str, Any]) -> dict[str, Any]:
    """Feed the original per-symbol MomentumTrader from real Schwab quote observations."""
    from tradescanner.momentum_runtime import feed_quote_rows

    rows = store.latest_for_symbols(batch)
    apply_cached_float(rows, store)
    runtime = feed_quote_rows(store, rows, strategy)
    actions = runtime.get("actions", [])
    actions_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for action in actions:
        actions_by_symbol.setdefault(str(action.get("symbol")), []).append(action)
    evaluations = []
    for item in runtime.get("evaluations", []):
        symbol = str(item.get("symbol", ""))
        symbol_actions = actions_by_symbol.get(symbol, [])
        entry = next((action for action in symbol_actions if action.get("action") == "ENTER"), None)
        evaluations.append({
            **item,
            "passed": entry is not None,
            "action": entry.get("action") if entry else None,
            "strategy": strategy.get("name", "Momentum"),
            "strategy_id": strategy.get("id"),
            "reasons": item.get("reasons", []),
            "stop": entry.get("stop") if entry else None,
            "data_source": "schwab_observed_quote",
        })
    state = {
        "status": runtime.get("status", "waiting_for_new_quotes"),
        "strategy_id": strategy.get("id"),
        "strategy": strategy.get("name", "Momentum"),
        "evaluated_at": runtime.get("evaluated_at", _now()),
        "candidate_count": runtime.get("processed_quotes", 0),
        "processed_quotes": runtime.get("processed_quotes", 0),
        "evaluations": evaluations,
        "actions": actions,
        "matches": list(dict.fromkeys(
            str(action.get("symbol")) for action in actions if action.get("action") == "ENTER"
        )),
        "skipped": runtime.get("skipped", {}),
        "note": runtime.get("note"),
    }
    store.set_state("strategy_scan_status:" + str(strategy.get('id')), state)
    return state

def _news_session_for_result(article: dict[str, Any]) -> dict[str, str] | None:
    """Keep timestamped headlines published after-hours, overnight, or premarket."""
    published = article.get("published_at")
    headline = article.get("headline")
    if not isinstance(published, str) or not isinstance(headline, str) or not headline.strip():
        return None
    try:
        stamp = datetime.fromisoformat(published.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        local = stamp.astimezone(ZoneInfo("America/New_York"))
    except (ValueError, TypeError):
        return None
    age_hours = (datetime.now(timezone.utc) - stamp.astimezone(timezone.utc)).total_seconds() / 3600
    if age_hours < 0 or age_hours > 24:
        return None
    minute = local.hour * 60 + local.minute
    if 16 * 60 <= minute < 20 * 60:
        session = "POSTMARKET"
    elif minute >= 20 * 60 or minute < 4 * 60:
        session = "OVERNIGHT"
    elif 4 * 60 <= minute <= 9 * 60 + 30:
        session = "PREMARKET"
    else:
        return None
    return {"published_at": published, "headline": headline.strip(), "session": session}

def _evaluate_original_scan_strategy(store: SnapshotStore, batch: list[str], strategy: dict[str, Any]) -> dict[str, Any]:
    """Run the stored scan(df, ctx) Momentum contract through the safe OHLCV adapter."""
    from tradescanner.momentum import quote_only_checks, scan as evaluate_momentum_scan
    from tradescanner.momentum_runtime import _scan_strategy_config

    rows = store.latest_for_symbols(batch)
    apply_cached_float(rows, store)
    candle_store = CandleStore(store.path)
    cfg = _scan_strategy_config(strategy)
    evaluations: list[dict[str, Any]] = []
    skipped: dict[str, str] = {}

    for row in rows:
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        if row.get("quote_is_fresh") is not True:
            skipped[symbol] = "quote_not_fresh"
            continue
        # Quote-only checks run before touching the candle table. Most fresh
        # universe rows fail price/GAP/volume/FLOAT, so loading 6,000 bars and
        # calculating RVOL for every one was needlessly slow.
        news = {"status": "not_requested", "news": []}
        context = {
            "symbol": symbol,
            "quote_time_ms": row.get("quote_time_ms"),
            "quote_price": row.get("price"),
            "current_volume": row.get("volume"),
            "prev_close": row.get("previous_close"),
            "rvol": None,
            "rvol_sessions": 0,
            "float_shares": row.get("float_shares"),
            "news": [],
        }
        quote_checks = quote_only_checks(context, cfg)
        quote_reasons = [
            f"{name}: {reason}"
            for name, (passed, _value, reason) in quote_checks.items()
            if not passed
        ]
        if quote_reasons:
            candles = []
            enough_candles = False
            rvol_data = {"rvol": None, "sessions_used": 0}
            result = {
                "passed": False,
                "metrics": {name: value for name, (_passed, value, _reason) in quote_checks.items()},
                "reasons": quote_reasons,
            }
        else:
            candles = candle_store.latest(symbol, limit=6000)
            # Only quote-qualified symbols need indicator history and time-of-day RVOL.
            enough_candles = len(candles) >= int(cfg.get("min_bars", 35))
            rvol_data = (
                candle_store.rvol(
                    symbol,
                    as_of_ms=row.get("quote_time_ms"),
                    current_volume=row.get("volume"),
                )
                if enough_candles else {"rvol": None, "sessions_used": 0}
            )
            context["rvol"] = rvol_data.get("rvol")
            context["rvol_sessions"] = rvol_data.get("sessions_used", 0)
            result = evaluate_momentum_scan(candles, context, cfg)
            # Headlines are requested only when every price/volume/FLOAT/indicator
            # condition already passes; an unavailable news source cannot stall scans.
            if enough_candles and cfg.get("news_required", True) and result.get("reasons") == ["noticia: sin noticia fresca verificable"]:
                news = recent_stock_news(store, symbol)
                context["news"] = news.get("news", [])
                result = evaluate_momentum_scan(candles, context, cfg)
        news_headlines = [
            item for item in (_news_session_for_result(article) for article in news.get("news", []))
            if item is not None
        ]
        news_headlines.sort(key=lambda item: item["published_at"], reverse=True)
        timeframe = result.get("metrics", {}).get("timeframe") or cfg.get("timeframe") or cfg.get("interval")
        evaluations.append({
            "symbol": symbol,
            "passed": bool(result.get("passed")),
            "action": "ENTER" if result.get("passed") else None,
            "strategy": strategy.get("name", "Momentum"),
            "strategy_id": strategy.get("id"),
            "metrics": result.get("metrics", {}),
            "ema20": result.get("metrics", {}).get("ema20") or _ema20_from_candles(candles),
            "trend_zone": (
                "bearish" if isinstance(row.get("price"), (int, float)) and isinstance(result.get("metrics", {}).get("ema20") or _ema20_from_candles(candles), (int, float)) and row.get("price") < (result.get("metrics", {}).get("ema20") or _ema20_from_candles(candles))
                else "bullish" if isinstance(row.get("price"), (int, float)) and isinstance(result.get("metrics", {}).get("ema20") or _ema20_from_candles(candles), (int, float)) and row.get("price") > (result.get("metrics", {}).get("ema20") or _ema20_from_candles(candles))
                else None
            ),
            "reasons": result.get("reasons", []),
            "news_headlines": news_headlines[:3],
            "timeframe": timeframe,
            "data_source": "schwab_quotes_and_historical_candles",
            "quote_price": row.get("price"),
            "candles_available": len(candles),
            "rvol_sessions_used": rvol_data.get("sessions_used", 0),
            "rvol_as_of": rvol_data.get("as_of"),
            "news_status": news.get("status"),
        })

    state = {
        "status": "evaluated" if evaluations else "no_fresh_quotes",
        "strategy_id": strategy.get("id"),
        "strategy": strategy.get("name", "Momentum"),
        "evaluated_at": _now(),
        "candidate_count": len(rows),
        "processed_quotes": len(evaluations),
        "evaluations": evaluations,
        "matches": [item["symbol"] for item in evaluations if item.get("passed")],
        "skipped": skipped,
        "note": "Programa original scan(df, ctx) evaluado con OHLCV histórico de Schwab y contexto real; no ejecuta el código subido ni coloca órdenes.",
    }
    store.set_state("strategy_scan_status:" + str(strategy.get("id")), state)
    return state




_ATH_CACHE_TTL_SECONDS = 24 * 60 * 60
_ATH_ERROR_CACHE_TTL_SECONDS = 5 * 60
_ATH_MAX_REFRESHES_PER_CYCLE = 8
_ath_inflight_symbols: set[str] = set()
_ath_inflight_lock = threading.Lock()
_ath_fetch_semaphore = threading.BoundedSemaphore(8)


def _fetch_ath_in_background(symbol: str, quote_time_ms: int | float, store_path: Any) -> None:
    """Fetch ATH off the scanner's critical path and persist it for later quote cycles."""
    key = "rompiendo_maximos_ath:" + symbol.strip().upper()
    try:
        with _ath_fetch_semaphore:
            payload = get_all_time_daily_history(symbol)
        quote_day = datetime.fromtimestamp(float(quote_time_ms) / 1000, ZoneInfo("America/New_York")).date()
        valid = []
        for candle in payload.get("candles", []):
            if not isinstance(candle, dict):
                continue
            try:
                stamp = float(candle.get("datetime"))
                high = float(candle.get("high"))
                candle_day = datetime.fromtimestamp(stamp / 1000, ZoneInfo("America/New_York")).date()
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            if stamp > 0 and high > 0 and math.isfinite(high) and candle_day < quote_day:
                valid.append((high, candle_day.isoformat()))
        if valid:
            high, ath_date = max(valid, key=lambda item: item[0])
            result = {"status": "ready", "high": high, "ath_date": ath_date,
                      "history_sessions": len(valid), "history_window_years": 20}
        else:
            result = {"status": "insufficient_history", "high": None, "ath_date": None}
        SnapshotStore(store_path).set_state(key, {**result, "cached_at": time.time()})
    except Exception as exc:
        result = {"status": "unavailable", "high": None, "ath_date": None, "error": type(exc).__name__}
        try:
            SnapshotStore(store_path).set_state(key, {**result, "cached_at": time.time()})
        except Exception:
            pass
        logger.warning("RompiendoMaximos ATH fetch failed symbol=%s error_type=%s", symbol, type(exc).__name__)
    finally:
        with _ath_inflight_lock:
            _ath_inflight_symbols.discard(symbol.strip().upper())


def _all_time_high_for_symbol(store: SnapshotStore, symbol: str, quote_time_ms: int | float,
                              fetch_budget: dict[str, int]) -> dict[str, Any]:
    """Return cached prior-session ATH; missing history refreshes asynchronously."""
    key = "rompiendo_maximos_ath:" + symbol.strip().upper()
    cached = store.get_state(key, {})
    now = time.time()
    if isinstance(cached, dict):
        age = now - float(cached.get("cached_at", 0) or 0)
        ttl = _ATH_ERROR_CACHE_TTL_SECONDS if cached.get("status") == "unavailable" else _ATH_CACHE_TTL_SECONDS
        if age < ttl:
            return dict(cached)
    if fetch_budget["used"] >= _ATH_MAX_REFRESHES_PER_CYCLE:
        return {"status": "pending", "high": None, "ath_date": None}
    normalized = symbol.strip().upper()
    with _ath_inflight_lock:
        if normalized in _ath_inflight_symbols:
            return {"status": "pending", "high": None, "ath_date": None}
        _ath_inflight_symbols.add(normalized)
    fetch_budget["used"] += 1
    thread = threading.Thread(
        target=_fetch_ath_in_background,
        args=(normalized, quote_time_ms, store.path),
        name="rompiendo-maximos-ath-" + normalized,
        daemon=True,
    )
    thread.start()
    return {"status": "pending", "high": None, "ath_date": None}


def _evaluate_rompiendo_maximos(store: SnapshotStore, rows: list[dict[str, Any]],
                               strategy: dict[str, Any]) -> dict[str, Any]:
    """Evaluate live Schwab quotes against the prior completed daily ATH."""
    evaluations = []
    fetch_budget = {"used": 0}
    for row in rows:
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol or row.get("quote_is_fresh") is not True:
            continue
        price, quote_time_ms = row.get("price"), row.get("quote_time_ms")
        if (not isinstance(price, (int, float)) or isinstance(price, bool)
                or not math.isfinite(float(price)) or price <= 0
                or not isinstance(quote_time_ms, (int, float)) or isinstance(quote_time_ms, bool)
                or quote_time_ms <= 0):
            evaluations.append({"symbol": symbol, "passed": False, "strategy": strategy.get("name"),
                                "strategy_id": strategy.get("id"), "reasons": ["cotización inválida o sin timestamp"]})
            continue
        if not 1.0 <= float(price) <= 20.0:
            evaluations.append({
                "symbol": symbol, "passed": False, "action": None,
                "strategy": strategy.get("name"), "strategy_id": strategy.get("id"),
                "quote_price": price, "all_time_high": None, "ath_date": None,
                "metrics": {"price": price, "price_min": 1.0, "price_max": 20.0},
                "reasons": ["precio: fuera del rango permitido de $1.00 a $20.00"],
                "data_source": "Schwab real-time quote",
            })
            continue
        ath = _all_time_high_for_symbol(store, symbol, quote_time_ms, fetch_budget)
        high = ath.get("high")
        passed = (ath.get("status") == "ready" and isinstance(high, (int, float))
                  and float(price) > float(high))
        if ath.get("status") == "pending":
            reasons = ["histórico: pendiente de consulta; se procesa en próximos ciclos"]
        elif ath.get("status") == "unavailable":
            reasons = ["histórico: Schwab no disponible; no se genera señal"]
        elif ath.get("status") != "ready":
            reasons = ["histórico: no hay velas diarias completas disponibles"]
        elif passed:
            reasons = []
        else:
            reasons = ["ruptura: precio actual no supera el máximo histórico previo"]
        evaluations.append({
            "symbol": symbol, "passed": passed, "action": "ENTER" if passed else None,
            "strategy": strategy.get("name"), "strategy_id": strategy.get("id"),
            "quote_price": price, "all_time_high": high, "ath_date": ath.get("ath_date"),
            "percent_above_ath": round((float(price) / float(high) - 1) * 100, 4) if passed and high else None,
            "history_window_years": ath.get("history_window_years", 20),
            "metrics": {"price": price, "all_time_high": high, "ath_date": ath.get("ath_date")},
            "reasons": reasons, "data_source": "Schwab real-time quote + completed daily history",
        })
    state = {
        "status": "evaluated" if evaluations else "no_fresh_quotes",
        "strategy_id": strategy.get("id"), "strategy": strategy.get("name"),
        "evaluated_at": _now(), "candidate_count": len(rows), "processed_quotes": len(evaluations),
        "ath_refreshes_this_cycle": fetch_budget["used"], "evaluations": evaluations,
        "matches": [item["symbol"] for item in evaluations if item.get("passed")],
        "note": "Ruptura en tiempo real: precio entre $1.00 y $20.00 inclusive y cotización Schwab > máximo High de sesiones diarias completas anteriores. Histórico disponible limitado a 20 años; excluye la sesión actual. No coloca órdenes.",
    }
    store.set_state("strategy_scan_status:" + str(strategy.get("id")), state)
    return state



_ATL_CACHE_TTL_SECONDS = 24 * 60 * 60
_ATL_ERROR_CACHE_TTL_SECONDS = 5 * 60
_ATL_MAX_REFRESHES_PER_CYCLE = 8
_atl_inflight_symbols: set[str] = set()
_atl_inflight_lock = threading.Lock()
_atl_fetch_semaphore = threading.BoundedSemaphore(8)


def _fetch_atl_in_background(symbol: str, quote_time_ms: int | float, store_path: Any) -> None:
    """Fetch and persist the prior completed-session ATL without blocking quote scans."""
    key = "rompiendo_minimos_atl:" + symbol.strip().upper()
    try:
        with _atl_fetch_semaphore:
            payload = get_all_time_daily_history(symbol)
        quote_day = datetime.fromtimestamp(float(quote_time_ms) / 1000, ZoneInfo("America/New_York")).date()
        valid = []
        for candle in payload.get("candles", []):
            if not isinstance(candle, dict):
                continue
            try:
                stamp = float(candle.get("datetime"))
                low = float(candle.get("low"))
                candle_day = datetime.fromtimestamp(stamp / 1000, ZoneInfo("America/New_York")).date()
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            if stamp > 0 and low > 0 and math.isfinite(low) and candle_day < quote_day:
                valid.append((low, candle_day.isoformat()))
        if valid:
            low, atl_date = min(valid, key=lambda item: item[0])
            result = {"status": "ready", "low": low, "atl_date": atl_date,
                      "history_sessions": len(valid), "history_window_years": 20}
        else:
            result = {"status": "insufficient_history", "low": None, "atl_date": None}
        SnapshotStore(store_path).set_state(key, {**result, "cached_at": time.time()})
    except Exception as exc:
        result = {"status": "unavailable", "low": None, "atl_date": None, "error": type(exc).__name__}
        try:
            SnapshotStore(store_path).set_state(key, {**result, "cached_at": time.time()})
        except Exception:
            pass
        logger.warning("RompiendoMinimos ATL fetch failed symbol=%s error_type=%s", symbol, type(exc).__name__)
    finally:
        with _atl_inflight_lock:
            _atl_inflight_symbols.discard(symbol.strip().upper())


def _all_time_low_for_symbol(store: SnapshotStore, symbol: str, quote_time_ms: int | float,
                             fetch_budget: dict[str, int]) -> dict[str, Any]:
    """Return cached prior-session ATL; missing history refreshes asynchronously."""
    key = "rompiendo_minimos_atl:" + symbol.strip().upper()
    cached = store.get_state(key, {})
    now = time.time()
    if isinstance(cached, dict):
        age = now - float(cached.get("cached_at", 0) or 0)
        ttl = _ATL_ERROR_CACHE_TTL_SECONDS if cached.get("status") == "unavailable" else _ATL_CACHE_TTL_SECONDS
        if age < ttl:
            return dict(cached)
    if fetch_budget["used"] >= _ATL_MAX_REFRESHES_PER_CYCLE:
        return {"status": "pending", "low": None, "atl_date": None}
    normalized = symbol.strip().upper()
    with _atl_inflight_lock:
        if normalized in _atl_inflight_symbols:
            return {"status": "pending", "low": None, "atl_date": None}
        _atl_inflight_symbols.add(normalized)
    fetch_budget["used"] += 1
    thread = threading.Thread(
        target=_fetch_atl_in_background,
        args=(normalized, quote_time_ms, store.path),
        name="rompiendo-minimos-atl-" + normalized,
        daemon=True,
    )
    thread.start()
    return {"status": "pending", "low": None, "atl_date": None}


def _evaluate_rompiendo_minimos(store: SnapshotStore, rows: list[dict[str, Any]],
                               strategy: dict[str, Any]) -> dict[str, Any]:
    """Evaluate live Schwab quotes against the prior completed daily ATL."""
    evaluations = []
    fetch_budget = {"used": 0}
    for row in rows:
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol or row.get("quote_is_fresh") is not True:
            continue
        price, quote_time_ms = row.get("price"), row.get("quote_time_ms")
        if (not isinstance(price, (int, float)) or isinstance(price, bool)
                or not math.isfinite(float(price)) or price <= 0
                or not isinstance(quote_time_ms, (int, float)) or isinstance(quote_time_ms, bool)
                or quote_time_ms <= 0):
            evaluations.append({"symbol": symbol, "passed": False, "strategy": strategy.get("name"),
                                "strategy_id": strategy.get("id"), "reasons": ["cotización inválida o sin timestamp"]})
            continue
        if not 1.0 <= float(price) <= 20.0:
            evaluations.append({
                "symbol": symbol, "passed": False, "action": None,
                "strategy": strategy.get("name"), "strategy_id": strategy.get("id"),
                "quote_price": price, "all_time_low": None, "atl_date": None,
                "metrics": {"price": price, "price_min": 1.0, "price_max": 20.0},
                "reasons": ["precio: fuera del rango permitido de $1.00 a $20.00"],
                "data_source": "Schwab real-time quote",
            })
            continue
        atl = _all_time_low_for_symbol(store, symbol, quote_time_ms, fetch_budget)
        low = atl.get("low")
        passed = (atl.get("status") == "ready" and isinstance(low, (int, float))
                  and float(price) < float(low))
        if atl.get("status") == "pending":
            reasons = ["histórico: pendiente de consulta; se procesa en próximos ciclos"]
        elif atl.get("status") == "unavailable":
            reasons = ["histórico: Schwab no disponible; no se genera señal"]
        elif atl.get("status") != "ready":
            reasons = ["histórico: no hay velas diarias completas disponibles"]
        elif passed:
            reasons = []
        else:
            reasons = ["ruptura: precio actual no está por debajo del mínimo histórico previo"]
        evaluations.append({
            "symbol": symbol, "passed": passed, "action": "ENTER" if passed else None,
            "strategy": strategy.get("name"), "strategy_id": strategy.get("id"),
            "quote_price": price, "all_time_low": low, "atl_date": atl.get("atl_date"),
            "percent_below_atl": round((float(price) / float(low) - 1) * 100, 4) if passed and low else None,
            "history_window_years": atl.get("history_window_years", 20),
            "metrics": {"price": price, "all_time_low": low, "atl_date": atl.get("atl_date")},
            "reasons": reasons, "data_source": "Schwab real-time quote + completed daily history",
        })
    state = {
        "status": "evaluated" if evaluations else "no_fresh_quotes",
        "strategy_id": strategy.get("id"), "strategy": strategy.get("name"),
        "evaluated_at": _now(), "candidate_count": len(rows), "processed_quotes": len(evaluations),
        "atl_refreshes_this_cycle": fetch_budget["used"], "evaluations": evaluations,
        "matches": [item["symbol"] for item in evaluations if item.get("passed")],
        "note": "Ruptura en tiempo real: precio entre $1.00 y $20.00 inclusive y cotización Schwab < mínimo Low de sesiones diarias completas anteriores. Histórico disponible limitado a 20 años; excluye la sesión actual. No coloca órdenes.",
    }
    store.set_state("strategy_scan_status:" + str(strategy.get("id")), state)
    return state


def _evaluate_active_strategies(store: SnapshotStore, batch: list[str]) -> dict[str, Any]:
    """Evaluate every enabled strategy once per scanner cycle, without placing orders."""
    strategies = active_strategies(store)
    if not strategies:
        inactive = {"status": "inactive", "evaluated_at": _now(), "evaluations": [], "matches": []}
        store.set_state("momentum_scan_status", inactive)
        store.set_state("active_strategies_scan_status", {"status": "inactive", "strategies": [], "evaluated_at": _now()})
        logger.warning("Scan cycle strategy evaluation active=0 status=inactive")
        return {"status": "inactive", "evaluations": [], "matches": [], "strategies": []}

    rows = store.latest_for_symbols(batch)
    apply_cached_float(rows, store)
    states: list[dict[str, Any]] = []
    for strategy in strategies:
        if str(strategy.get("name", "")).casefold() == "opengapfranco":
            evaluations: list[dict[str, Any]] = []
            for row in rows:
                if row.get("quote_is_fresh") is not True:
                    continue
                result = evaluate_open_gap_franco(row)
                evaluations.append({
                    **result,
                    "action": "ENTER" if result.get("passed") else None,
                    "strategy": strategy.get("name"),
                    "strategy_id": strategy.get("id"),
                    "quote_price": row.get("price"),
                })
            state = {
                "status": "evaluated" if evaluations else "no_fresh_quotes",
                "strategy_id": strategy.get("id"),
                "strategy": strategy.get("name"),
                "evaluated_at": _now(),
                "candidate_count": len(rows),
                "processed_quotes": len(evaluations),
                "evaluations": evaluations,
                "matches": [item["symbol"] for item in evaluations if item.get("passed")],
                "note": "OpenGapFranco usa Bollinger y MACD en velas diarias; volumen, precio, float y hora proceden de datos observados. No coloca órdenes.",
            }
            store.set_state("strategy_scan_status:" + str(strategy.get("id")), state)
        elif str(strategy.get("name", "")).casefold() == "rompiendomaximos":
            state = _evaluate_rompiendo_maximos(store, rows, strategy)
        elif str(strategy.get("name", "")).casefold() == "rompiendominimos":
            state = _evaluate_rompiendo_minimos(store, rows, strategy)
        elif is_momentum_scan_program(strategy):
            state = _evaluate_original_scan_strategy(store, batch, strategy)
            store.set_state("momentum_scan_status", state)
        elif ("momentum" in str(strategy.get("name", "")).casefold() and strategy.get("language") == "python") or is_momentum_trader_program(strategy):
            state = _evaluate_momentum_strategy(store, batch, strategy)
            store.set_state("momentum_scan_status", state)
        else:
            evaluations: list[dict[str, Any]] = []
            needs_rvol = "rvol" in str(strategy.get("source", "")).casefold()
            candle_store = CandleStore(store.path)
            for row in rows:
                if row.get("quote_is_fresh") is not True:
                    continue
                evaluated_row = dict(row)
                if needs_rvol:
                    try:
                        evaluated_row["rvol"] = candle_store.rvol(row["symbol"]).get("rvol")
                    except Exception:
                        evaluated_row["rvol"] = None
                try:
                    candles_for_ema = candle_store.latest(row["symbol"], limit=6000)
                    ema20 = _ema20_from_candles(candles_for_ema)
                except Exception:
                    ema20 = None
                current_price = row.get("price")
                trend_zone = (
                    "bearish" if isinstance(current_price, (int, float)) and isinstance(ema20, (int, float)) and current_price < ema20
                    else "bullish" if isinstance(current_price, (int, float)) and isinstance(ema20, (int, float)) and current_price > ema20
                    else None
                )
                passed = strategy_matches(strategy, evaluated_row)
                evaluations.append({
                    "symbol": row.get("symbol"),
                    "passed": passed,
                    "strategy": strategy.get("name"),
                    "strategy_id": strategy.get("id"),
                    "ema20": ema20,
                    "trend_zone": trend_zone,
                    "metrics": {
                        "price": evaluated_row.get("price"),
                        "ema20": ema20,
                        "gap_pct": evaluated_row.get("gap_pct"),
                        "rvol": evaluated_row.get("rvol"),
                        "volume": evaluated_row.get("volume"),
                        "float_shares": evaluated_row.get("float_shares"),
                    },
                    "reasons": [] if passed else ["no cumple la expresión de la estrategia con los datos disponibles"],
                })
            state = {
                "status": "evaluated" if evaluations else "no_fresh_quotes",
                "strategy_id": strategy.get("id"),
                "strategy": strategy.get("name"),
                "evaluated_at": _now(),
                "candidate_count": len(rows),
                "evaluations": evaluations,
                "matches": [item["symbol"] for item in evaluations if item.get("passed")],
            }
            store.set_state("strategy_scan_status:" + str(strategy.get("id")), state)
        states.append(state)

    combined = {
        "status": "evaluated",
        "evaluated_at": _now(),
        "strategies": states,
        "evaluations": [item for state in states for item in state.get("evaluations", [])],
        "matches": list(dict.fromkeys(symbol for state in states for symbol in state.get("matches", []))),
    }
    store.set_state("active_strategies_scan_status", combined)
    rejection_counts: dict[str, int] = {}
    for evaluation in combined["evaluations"]:
        for reason in evaluation.get("reasons", []):
            key = str(reason).split(":", 1)[0].strip() or "unknown"
            rejection_counts[key] = rejection_counts.get(key, 0) + 1
    candles_ready = sum(
        1 for item in combined["evaluations"]
        if int(item.get("candles_available", 0) or 0) >= 35
        and item.get("timeframe") == "1m"
    )
    rvol_ready = sum(
        1 for item in combined["evaluations"]
        if int(item.get("rvol_sessions_used", 0) or 0) >= 2
    )
    macd_checked = sum(
        1 for item in combined["evaluations"]
        if isinstance(item.get("metrics", {}).get("macd_histogram"), (int, float))
    )
    reason_summary = ",".join(
        f"{key}:{value}" for key, value in sorted(rejection_counts.items())
    ) or "none"
    logger.warning(
        "Scan cycle strategy evaluation active=%s evaluations=%s matches=%s statuses=%s "
        "candles_ready=%s rvol_ready=%s macd_checked=%s rejection_counts=%s",
        len(strategies), len(combined["evaluations"]), len(combined["matches"]),
        ",".join(str(state.get("strategy", "unknown")) + ":" + str(state.get("status", "unknown")) for state in states),
        candles_ready, rvol_ready, macd_checked, reason_summary,
    )
    return combined

def run_scan_cycle(
    store: SnapshotStore | None = None,
    *,
    universe_loader: Callable[[], dict[str, Any]] | None = None,
    quote_fetcher: Callable[[list[str]], dict[str, Any]] | None = None,
    candle_fetcher: Callable[[str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Scan one provider-sized batch; cursor advances only after successful persistence."""
    if not _cycle_lock.acquire(blocking=False):
        return {"status": "skipped", "reason": "cycle_already_running"}
    try:
        target_store = store or SnapshotStore()
        if not _credentials_configured() and quote_fetcher is None:
            state = record_engine_status(
                target_store, status="waiting_for_credentials",
                provider="Schwab", last_error=None,
            )
            return {"status": state["status"], "provider": "Schwab"}

        logger.warning("Scan cycle stage=load_universe_started")
        universe = _load_cached_universe(universe_loader)
        logger.warning("Scan cycle stage=load_universe_finished symbols=%s", universe.get("count", "unknown"))
        symbols = universe.get("symbols")
        if not isinstance(symbols, list) or not symbols:
            raise ValueError("The listed-symbol universe is empty.")
        symbols = list(dict.fromkeys(
            item.strip().upper() for item in symbols
            if isinstance(item, str) and item.strip()
        ))
        if not symbols:
            raise ValueError("The listed-symbol universe contains no supported symbols.")

        saved_cursor = target_store.get_state("universe_cursor", 0)
        cursor = int(saved_cursor) % len(symbols)
        # Schwab limits each quote request to 100 symbols. Sweep eight requests
        # per cycle so the complete 7k-symbol universe is refreshed in minutes,
        # not dozens of 30-second cycles. Keep the cap configurable and bounded.
        try:
            batches_per_cycle = max(1, min(8, int(os.getenv("TRADESCANNER_BATCHES_PER_CYCLE", "8"))))
        except ValueError:
            batches_per_cycle = 8
        cycle_batch_size = BATCH_SIZE * batches_per_cycle
        batch = symbols[cursor:cursor + cycle_batch_size]
        if len(batch) < BATCH_SIZE and cursor + len(batch) < len(symbols):
            batch.extend(symbols[:BATCH_SIZE - len(batch)])
        started = time.monotonic()
        cycle_started_at = _now()
        logger.warning("Scan cycle stage=schwab_quotes_started batch_size=%s provider_requests=%s", len(batch), (len(batch) + BATCH_SIZE - 1) // BATCH_SIZE)
        result = collect_snapshots(batch, target_store, quote_fetcher or get_quotes)
        logger.warning("Scan cycle stage=schwab_quotes_finished received=%s saved=%s", result.get("received_count", 0), result.get("snapshots_saved", 0))
        if result["received_count"] <= 0 or result["snapshots_saved"] <= 0:
            raise SchwabAPIError(
                "Schwab returned no persistable quotes; the universe cursor was not advanced."
            )
        # Massive is the sole external FLOAT provider; Schwab remains the quote provider.
        float_provider_state = refresh_float_cache(target_store)
        candle_state: dict[str, Any] = {
            "candle_symbol": None, "candles_saved": 0, "rvol": None,
            "rvol_sessions_used": 0, "candle_error": None, "float_symbol": None,
            "float_shares": None, "float_as_of": None, "float_source": None,
            "float_error": None, "targeted_float_cooldown_reason": None,
            "targeted_float_provider_http_status": None,
            "targeted_float_cooldown_remaining_seconds": None,
        }
        candle_state["float_error"] = float_provider_state.get("error")
        try:
            latest_rows_for_float = target_store.latest_for_symbols(batch)
            apply_cached_float(latest_rows_for_float, target_store)
            known_float = [row for row in latest_rows_for_float
                           if isinstance(row.get("symbol"), str) and row.get("float_shares") is not None]
            if known_float:
                candidate = max(known_float, key=lambda row: row.get("volume") if isinstance(row.get("volume"), (int, float)) else -1)
                targeted_float_state = {
                    "status": "cached", "provider": "Massive",
                    "symbol": str(candidate["symbol"]).strip().upper(),
                    "float_shares": candidate.get("float_shares"),
                    "float_source": candidate.get("float_source"),
                    "float_as_of": candidate.get("float_as_of"),
                }
                candle_state["float_symbol"] = targeted_float_state["symbol"]
                candle_state["float_shares"] = candidate.get("float_shares")
                candle_state["float_as_of"] = candidate.get("float_as_of")
                candle_state["float_source"] = candidate.get("float_source")
            else:
                targeted_float_state = {
                    "status": "not_available", "provider": "Massive",
                    "detail": float_provider_state.get("error") or float_provider_state.get("status"),
                }
        except Exception as exc:
            targeted_float_state = {
                "status": "cache_unavailable", "provider": "local_cache",
                "error": type(exc).__name__,
            }
            logger.warning("Local FLOAT cache unavailable; error_type=%s", type(exc).__name__)
        if float_provider_state.get("error"):
            logger.warning(
                "Scan cycle FLOAT provider=Massive refresh_status=%s rows_saved=%s cached_symbol=%s error_type=%s",
                float_provider_state.get("status"), float_provider_state.get("rows_saved", 0),
                targeted_float_state.get("symbol"), type(float_provider_state.get("error")).__name__,
            )
        else:
            # Successful cache reads are normal operation, not errors. Avoid an
            # error-field in this message so log collectors do not misclassify it.
            logger.info(
                "Scan cycle FLOAT provider=Massive refresh_status=%s rows_saved=%s cached_symbol=%s",
                float_provider_state.get("status"), float_provider_state.get("rows_saved", 0),
                targeted_float_state.get("symbol"),
            )

        # Strategy evaluation covers the whole quote batch, so sampling history for
        # only one ticker leaves almost every candidate unable to calculate EMA/MACD.
        # Backfill a small bounded set of symbols per cycle, prioritizing missing
        # histories and liquid/gapping names. This does not relax strategy filters.
        enabled_strategies = active_strategies(target_store)
        active_scan_strategies = [s for s in enabled_strategies if is_momentum_scan_program(s)]
        jeanlong_minute_scan = any(str(s.get("name", "")).casefold() == "premarketjeanlong" for s in active_scan_strategies)
        needs_multi_session_history = any(
            is_momentum_scan_program(strategy)
            or is_momentum_trader_program(strategy)
            or "rvol" in str(strategy.get("source", "")).casefold()
            for strategy in enabled_strategies
        )
        history_fetch = candle_fetcher or (
            _get_multi_session_minute_history
            if quote_fetcher is None and needs_multi_session_history
            else get_price_history if quote_fetcher is None else None
        )
        candle_state["candle_symbols"] = []
        candle_state["history_requests"] = 0
        candle_state["history_symbols_skipped_with_cache"] = 0
        if history_fetch is not None:
            logger.warning("Scan cycle stage=price_history_started")
            try:
                rows = target_store.latest_for_symbols(batch)
                eligible = [
                    row for row in rows
                    if isinstance(row.get("symbol"), str)
                    and isinstance(row.get("price"), (int, float))
                    and row["price"] > 0
                ]
                candle_store = CandleStore(target_store.path)
                missing_history = []
                history_status = candle_store.history_status_by_symbols([row["symbol"] for row in eligible])
                history_counts = {
                    symbol: int(status["count"])
                    for symbol, status in history_status.items()
                }
                # Resolve active scan settings before selecting history work.
                scan_configs = []
                try:
                    for active in active_strategies(target_store):
                        if is_momentum_scan_program(active):
                            from tradescanner.momentum_runtime import _scan_strategy_config
                            scan_configs.append(_scan_strategy_config(active))
                except (OSError, RuntimeError, ValueError) + DB_ERRORS:
                    scan_configs = []

                if jeanlong_minute_scan:
                    # Do not spend Schwab history calls on symbols already disqualified
                    # by quote-only JeanLong filters. FLOAT is read from Massive cache;
                    # unknown FLOAT remains a rejection and is never guessed.
                    apply_cached_float(eligible, target_store)
                    jeanlong_cfg = next(
                        (cfg for cfg in scan_configs if cfg.get("timeframe") == "1m"),
                        {},
                    )
                    market_tz = ZoneInfo("America/New_York")
                    for row in eligible:
                        if row.get("quote_is_fresh") is not True:
                            continue
                        price = row.get("price")
                        gap = row.get("gap_pct")
                        volume = row.get("volume")
                        float_shares = row.get("float_shares")
                        quote_stamp = row.get("quote_time_ms")
                        if not all(isinstance(value, (int, float)) and not isinstance(value, bool)
                                   for value in (price, gap, float_shares, quote_stamp)):
                            continue
                        if (
                            not jeanlong_cfg.get("price_min", 1.0) <= price <= jeanlong_cfg.get("price_max", 20.0)
                            or gap < jeanlong_cfg.get("gap_min_pct", 2.0)
                            or not 0 < float_shares < jeanlong_cfg.get("float_max", 50_000_000)
                            or (
                                isinstance(volume, (int, float))
                                and not isinstance(volume, bool)
                                and volume < jeanlong_cfg.get("volume_min", 750_000)
                            )
                        ):
                            continue
                        quote_local = datetime.fromtimestamp(quote_stamp / 1000, market_tz)
                        local_clock = quote_local.timetz().replace(tzinfo=None)
                        if not (
                            jeanlong_cfg.get("session_start", datetime.min.time())
                            <= local_clock
                            < jeanlong_cfg.get("session_end", datetime.max.time())
                        ):
                            continue
                        status = history_status.get(row["symbol"], {})
                        count = int(status.get("count", 0))
                        latest_candle_time_ms = status.get("latest_candle_time_ms")
                        # JeanLong needs at least two comparable prior sessions for
                        # time-of-day RVOL. A merely non-empty 1m cache is not enough.
                        rvol_sessions = 0
                        if count >= 35 and not _history_lags_quote(row, latest_candle_time_ms):
                            try:
                                rvol_sessions = int(
                                    candle_store.rvol(
                                        row["symbol"],
                                        as_of_ms=quote_stamp,
                                        current_volume=volume,
                                    ).get("sessions_used", 0) or 0
                                )
                            except Exception:
                                rvol_sessions = 0
                        if (
                            count < 35
                            or _history_lags_quote(row, latest_candle_time_ms)
                            or rvol_sessions < 2
                        ):
                            missing_history.append(row)
                        else:
                            candle_state["history_symbols_skipped_with_cache"] += 1
                else:
                    for row in eligible:
                        status = history_status.get(row["symbol"], {})
                        count = int(status.get("count", 0))
                        latest_candle_time_ms = status.get("latest_candle_time_ms")
                        if count < 35 or _history_lags_quote(row, latest_candle_time_ms):
                            missing_history.append(row)
                        elif count >= 35:
                            candle_state["history_symbols_skipped_with_cache"] += 1

                def history_priority(row: dict[str, Any]) -> tuple[Any, ...]:
                    price = row.get("price")
                    gap = row.get("gap_pct")
                    fresh = row.get("quote_is_fresh") is True
                    strategy_fit = False
                    if scan_configs and isinstance(price, (int, float)) and isinstance(gap, (int, float)):
                        strategy_fit = any(
                            cfg.get("price_min", 1.0) <= price <= cfg.get("price_max", float("inf"))
                            and gap >= cfg.get("gap_min_pct", 0.0)
                            for cfg in scan_configs
                        )
                    volume = row.get("volume") if isinstance(row.get("volume"), (int, float)) else -1
                    # With no strategy active, prioritize low-volume symbols so
                    # the interface's cumulative-volume filter gets current bars.
                    volume_priority = volume if enabled_strategies else -volume
                    abs_gap = abs(gap) if isinstance(gap, (int, float)) else -1
                    return (fresh, strategy_fit, volume_priority, abs_gap)

                # JeanLong evaluates every fresh candidate with 1-minute OHLCV.
                # Backfill up to the in-flight ceiling each cycle; otherwise the
                # 10-symbol cap leaves part of each 21-symbol evaluation batch
                # without candles and EMA/MACD/RVOL cannot be calculated.
                ordered_candidates = sorted(missing_history, key=history_priority, reverse=True)
                if history_fetch in {get_price_history, _get_multi_session_minute_history}:
                    # Avoid repeatedly requesting unchanged history for the same symbols.
                    now_monotonic = time.monotonic()
                    with _history_inflight_lock:
                        candidates = [
                            row for row in ordered_candidates
                            if row["symbol"] not in _history_inflight_symbols
                            and now_monotonic - _history_last_attempt_at.get(row["symbol"], 0.0)
                            >= _HISTORY_REFRESH_INTERVAL_SECONDS
                        ][:12]
                else:
                    candidates = ordered_candidates[:12]
                candle_state["history_candidates_eligible"] = len(missing_history)
                candle_state["history_candidates_deferred"] = max(0, len(missing_history) - len(candidates))
                if history_fetch in {get_price_history, _get_multi_session_minute_history}:
                    # Schwab can occasionally take minutes to return a candle payload.
                    # Run production history calls in bounded daemon threads and wait
                    # at most 15 seconds for this cycle; quotes and strategy checks continue.
                    jobs = []
                    for candidate in candidates:
                        symbol = candidate["symbol"]
                        with _history_inflight_lock:
                            if symbol in _history_inflight_symbols:
                                continue
                            if len(_history_inflight_symbols) >= _HISTORY_MAX_INFLIGHT:
                                break
                            _history_inflight_symbols.add(symbol)
                            _history_last_attempt_at[symbol] = time.monotonic()
                        job = {"done": threading.Event()}
                        thread = threading.Thread(
                            target=_fetch_history_in_background,
                            args=(symbol, history_fetch, target_store.path, job),
                            name="history-" + symbol,
                            daemon=True,
                        )
                        candle_state["history_requests"] += 1
                        jobs.append((symbol, job))
                        thread.start()

                    deadline = time.monotonic() + 15.0
                    pending_count = 0
                    for symbol, job in jobs:
                        remaining = max(0.0, deadline - time.monotonic())
                        if remaining > 0:
                            job["done"].wait(remaining)
                        if not job["done"].is_set():
                            pending_count += 1
                            continue
                        if job.get("error_type"):
                            candle_state["candle_error"] = job["error_type"]
                            continue
                        candle_state["candle_symbols"].append(symbol)
                        candle_state["candles_saved"] += int(job.get("saved", 0))
                        if candle_state["candle_symbol"] is None:
                            candle_state["candle_symbol"] = symbol
                            rvol = candle_store.rvol(symbol, as_of_ms=candidate.get("quote_time_ms"))
                            candle_state["rvol"] = rvol.get("rvol")
                            candle_state["rvol_sessions_used"] = rvol.get("sessions_used", 0)
                    candle_state["history_pending_count"] = pending_count
                    if pending_count:
                        logger.info(
                            "Scan cycle history_pending=%s deadline_seconds=15 inflight=%s",
                            pending_count, len(_history_inflight_symbols),
                        )
                else:
                    for candidate in candidates:
                        symbol = candidate["symbol"]
                        candle_state["history_requests"] += 1
                        try:
                            candle_payload = history_fetch(symbol)
                            candles = normalize_price_history(symbol, candle_payload)
                            saved = candle_store.save(candles)
                            candle_state["candles_saved"] += saved
                            candle_state["candle_symbols"].append(symbol)
                            if candle_state["candle_symbol"] is None:
                                candle_state["candle_symbol"] = symbol
                                rvol = candle_store.rvol(symbol)
                                candle_state["rvol"] = rvol.get("rvol")
                                candle_state["rvol_sessions_used"] = rvol.get("sessions_used", 0)
                            logger.warning(
                                "Scan cycle history_symbol=%s returned=%s saved=%s",
                                symbol, len(candles), saved,
                            )
                        except (SchwabAPIError, SchwabConfigurationError, ValueError, OSError, RuntimeError) + DB_ERRORS as exc:
                            candle_state["candle_error"] = type(exc).__name__
                            logger.warning(
                                "Scan cycle history_fetch_failed symbol=%s error_type=%s",
                                symbol, type(exc).__name__,
                            )
            except (SchwabAPIError, SchwabConfigurationError, ValueError, OSError, RuntimeError) + DB_ERRORS as exc:
                candle_state["candle_error"] = type(exc).__name__
        logger.warning(
            "Scan cycle stage=price_history_finished requests=%s symbols=%s candles_saved=%s",
            candle_state.get("history_requests", 0),
            ",".join(candle_state.get("candle_symbols", [])),
            candle_state.get("candles_saved", 0),
        )
        next_cursor = (cursor + len(batch)) % len(symbols)
        logger.warning("Scan cycle stage=cursor_persist_started cursor=%s", next_cursor)
        target_store.set_state("universe_cursor", next_cursor)
        logger.warning("Scan cycle stage=cursor_persist_finished")
        momentum_state = None
        try:
            momentum_state = _evaluate_active_strategies(target_store, batch)
        except Exception as exc:
            logger.warning("Momentum evaluation failed; error_type=%s", type(exc).__name__)
            target_store.set_state("momentum_scan_status", {
                "status": "error", "evaluated_at": _now(), "evaluations": [], "matches": [],
                "error_type": type(exc).__name__,
            })
        logger.warning("Scan cycle stage=engine_status_persist_started")
        state = record_engine_status(
            target_store,
            status="running",
            provider="Schwab",
            universe_count=len(symbols),
            batch_size=len(batch),
            cursor=next_cursor,
            cycle_started_at=cycle_started_at,
            last_cycle_at=_now(),
            last_error=None,
            last_cycle_duration_seconds=round(time.monotonic() - started, 3),
            requested_count=result["requested_count"],
            received_count=result["received_count"],
            snapshots_saved=result["snapshots_saved"],
            **candle_state,
            float_provider_status=float_provider_state.get("status"),
            float_provider_rows_saved=float_provider_state.get("rows_saved", 0),
            float_provider_configured=bool(os.getenv("MASSIVE_API_KEY", "").strip()),
            targeted_float_status=targeted_float_state.get("status"),
            targeted_float_symbol=targeted_float_state.get("symbol"),
            cycles_completed=int(target_store.get_state("cycles_completed", 0)) + 1,
            momentum_status=(momentum_state or target_store.get_state("momentum_scan_status", {})).get("status"),
            momentum_evaluations=len((momentum_state or target_store.get_state("momentum_scan_status", {})).get("evaluations", [])),
            momentum_matches=len((momentum_state or target_store.get_state("momentum_scan_status", {})).get("matches", [])),
        )
        logger.warning("Scan cycle stage=engine_status_persist_finished cycles_completed=%s", state.get("cycles_completed"))
        # Only mark market-data connectivity from provider timestamps that are
        # within the accepted freshness window; a newly fetched stale quote is not live data.
        logger.warning("Scan cycle stage=freshness_check_started")
        latest_rows = target_store.latest_for_symbols(batch)
        logger.warning("Scan cycle stage=freshness_check_finished rows=%s", len(latest_rows))
        if any(row.get("quote_is_fresh") is True for row in latest_rows):
            state["last_quote_received_at"] = _now()
            target_store.set_state("engine_status", state)
        target_store.set_state("cycles_completed", state["cycles_completed"])
        logger.warning("Scan cycle stage=cycle_returning status=completed cycles_completed=%s snapshots_saved=%s", state.get("cycles_completed"), state.get("snapshots_saved"))
        return {"status": "completed", **state}
    except (SchwabConfigurationError, SchwabAPIError, ValueError, OSError) + DB_ERRORS as exc:
        target_store = store or SnapshotStore()
        # SchwabAPIError messages are sanitized by tradescanner.schwab:
        # they contain stage/status details but never credentials or provider bodies.
        # Preserve the safe detail so read-only diagnostics can identify the cause.
        error_detail = str(exc) if isinstance(exc, SchwabAPIError) else type(exc).__name__
        logger.error("Scan cycle failed; error_type=%s error_detail=%s", type(exc).__name__, error_detail)
        state = record_engine_status(
            target_store,
            status="error",
            provider="Schwab",
            last_error=error_detail,
        )
        raise
    finally:
        _cycle_lock.release()
