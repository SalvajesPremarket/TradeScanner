"""Incremental, restart-safe quote scanner for the clean TradeScanner core."""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from tradescanner.candles import CandleStore, normalize_price_history
from tradescanner.db import DB_ERRORS
from tradescanner.float_provider import apply_cached_float, refresh_float_cache, refresh_symbol_float
from tradescanner.infisical_vault import is_configured as infisical_is_configured
from tradescanner.snapshots import BATCH_SIZE, SnapshotStore, collect_snapshots
from tradescanner.schwab import SchwabAPIError, SchwabConfigurationError, get_price_history, get_quotes
from tradescanner.universe import load_symbol_universe

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

UNIVERSE_CACHE_SECONDS = 6 * 60 * 60
_universe_cache: dict[str, Any] | None = None
_universe_cached_at = 0.0
_universe_lock = threading.Lock()
_cycle_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        batch = symbols[cursor:cursor + BATCH_SIZE]
        if len(batch) < BATCH_SIZE and cursor + len(batch) < len(symbols):
            batch.extend(symbols[:BATCH_SIZE - len(batch)])
        started = time.monotonic()
        cycle_started_at = _now()
        logger.warning("Scan cycle stage=schwab_quotes_started batch_size=%s", len(batch))
        result = collect_snapshots(batch, target_store, quote_fetcher or get_quotes)
        logger.warning("Scan cycle stage=schwab_quotes_finished received=%s saved=%s", result.get("received_count", 0), result.get("snapshots_saved", 0))
        if result["received_count"] <= 0 or result["snapshots_saved"] <= 0:
            raise SchwabAPIError(
                "Schwab returned no persistable quotes; the universe cursor was not advanced."
            )
        # FMP is used only for public float. Its cached bulk dataset refreshes at
        # most daily (six-hour cooldown on failures); it never supplies quotes.
        logger.warning("Scan cycle stage=float_cache_refresh_started")
        float_provider_state = refresh_float_cache(target_store)
        logger.warning("Scan cycle stage=float_cache_refresh_finished status=%s", float_provider_state.get("status", "unknown"))
        candle_state: dict[str, Any] = {
            "candle_symbol": None,
            "candles_saved": 0,
            "rvol": None,
            "rvol_sessions_used": 0,
            "candle_error": None,
            "float_symbol": None,
            "float_shares": None,
            "float_as_of": None,
            "float_source": None,
            "float_error": None,
            "targeted_float_cooldown_reason": None,
            "targeted_float_provider_http_status": None,
            "targeted_float_cooldown_remaining_seconds": None,
        }
        # If Schwab does not provide FLOAT, fill the current scan candidate from
        # FMP's symbol endpoint. This is at most one additional bounded request per cycle.
        try:
            latest_rows_for_float = target_store.latest_for_symbols(batch)
            # Check the freshly refreshed bulk cache before deciding that a symbol
            # needs the paid/plan-restricted per-symbol endpoint. apply_cached_float
            # is strictly local: it never makes a provider request.
            apply_cached_float(latest_rows_for_float, target_store)
            known_float = [
                row for row in latest_rows_for_float
                if isinstance(row.get("symbol"), str) and row.get("float_shares") is not None
            ]
            missing_float = [
                row for row in latest_rows_for_float
                if row.get("float_shares") is None and isinstance(row.get("symbol"), str)
            ]
            if missing_float:
                float_candidate = max(
                    missing_float,
                    key=lambda row: row.get("volume") if isinstance(row.get("volume"), (int, float)) else -1,
                )
                targeted_float_state = refresh_symbol_float(target_store, float_candidate["symbol"])
            elif known_float:
                # Report a real cache hit in engine diagnostics instead of "not_needed"
                # with no symbol/value. Prefer the most actively traded known symbol.
                float_candidate = max(
                    known_float,
                    key=lambda row: row.get("volume") if isinstance(row.get("volume"), (int, float)) else -1,
                )
                targeted_float_state = {
                    "status": "cached_bulk",
                    "provider": "FMP",
                    "symbol": str(float_candidate["symbol"]).strip().upper(),
                    "float_shares": float_candidate.get("float_shares"),
                }
            else:
                targeted_float_state = {"status": "not_found_in_bulk", "provider": "FMP"}
        except Exception as exc:
            targeted_float_state = {"status": "unavailable", "provider": "FMP", "error": type(exc).__name__}
        if targeted_float_state.get("status") in {"unavailable", "not_found", "cooldown", "not_configured"}:
            candle_state["float_error"] = targeted_float_state.get("error") or targeted_float_state["status"]
        candle_state["targeted_float_cooldown_reason"] = targeted_float_state.get("cooldown_reason")
        candle_state["targeted_float_provider_http_status"] = targeted_float_state.get("provider_http_status")
        candle_state["targeted_float_cooldown_remaining_seconds"] = targeted_float_state.get("cooldown_remaining_seconds")
        logger.warning(
            "Scan cycle FLOAT targeted_status=%s provider_http_status=%s cooldown_remaining_seconds=%s",
            targeted_float_state.get("status", "unknown"),
            targeted_float_state.get("provider_http_status"),
            targeted_float_state.get("cooldown_remaining_seconds"),
        )
        # Reflect cached/updated FLOAT in engine diagnostics too. The endpoint
        # decorates displayed snapshots from this cache, but engine status used
        # to misleadingly show null even when a targeted cache hit had succeeded.
        float_status_symbol = targeted_float_state.get("symbol")
        if isinstance(float_status_symbol, str) and float_status_symbol:
            try:
                from tradescanner.float_provider import apply_cached_float
                float_status_row = {"symbol": float_status_symbol, "float_shares": targeted_float_state.get("float_shares"),
                                    "float_source": None, "float_as_of": None}
                apply_cached_float([float_status_row], target_store)
                candle_state["float_symbol"] = float_status_symbol if float_status_row.get("float_shares") is not None else None
                candle_state["float_shares"] = float_status_row.get("float_shares")
                candle_state["float_as_of"] = float_status_row.get("float_as_of")
                candle_state["float_source"] = float_status_row.get("float_source")
            except (OSError, RuntimeError, ValueError) + DB_ERRORS:
                pass

        # One bounded historical request per successful quote cycle. Prefer the
        # strongest observed GAP from this batch; never block quote persistence
        # or cursor progress if the separate candle endpoint is unavailable.
        history_fetch = candle_fetcher or (get_price_history if quote_fetcher is None else None)
        if history_fetch is not None:
            logger.warning("Scan cycle stage=price_history_started")
            try:
                rows = target_store.latest_for_symbols(batch)
                eligible = [
                    row for row in rows
                    if isinstance(row.get("price"), (int, float)) and row["price"] > 0
                ]
                if eligible:
                    # Prefer the most actively traded symbol so the bounded
                    # history request is more likely to return enough candles for RVOL.
                    # Use absolute GAP only as a tie-breaker; thin/illiquid names often
                    # have sparse histories and cannot establish a reliable baseline.
                    candidate = max(
                        eligible,
                        key=lambda row: (
                            row.get("volume") if isinstance(row.get("volume"), (int, float)) else -1,
                            abs(row.get("gap_pct") or 0.0),
                        ),
                    )
                    candle_payload = history_fetch(candidate["symbol"])
                    candles = normalize_price_history(candidate["symbol"], candle_payload)
                    candle_store = CandleStore(target_store.path)
                    candle_state["candle_symbol"] = candidate["symbol"]
                    candle_state["candles_saved"] = candle_store.save(candles)
                    rvol = candle_store.rvol(candidate["symbol"])
                    candle_state["rvol"] = rvol.get("rvol")
                    candle_state["rvol_sessions_used"] = rvol.get("sessions_used", 0)
                    # Keep FLOAT diagnostics tied to the independently queried FLOAT symbol.
                    # Candle sampling can select a different ticker and must not overwrite it.
            except (SchwabAPIError, SchwabConfigurationError, ValueError, OSError, RuntimeError) + DB_ERRORS as exc:
                candle_state["candle_error"] = type(exc).__name__
        logger.warning("Scan cycle stage=price_history_finished candles_saved=%s", candle_state.get("candles_saved", 0))
        next_cursor = (cursor + len(batch)) % len(symbols)
        logger.warning("Scan cycle stage=cursor_persist_started cursor=%s", next_cursor)
        target_store.set_state("universe_cursor", next_cursor)
        logger.warning("Scan cycle stage=cursor_persist_finished")
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
            targeted_float_status=targeted_float_state.get("status"),
            targeted_float_symbol=targeted_float_state.get("symbol"),
            cycles_completed=int(target_store.get_state("cycles_completed", 0)) + 1,
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
        state = record_engine_status(
            target_store,
            status="error",
            provider="Schwab",
            last_error=type(exc).__name__,
        )
        raise
    finally:
        _cycle_lock.release()
