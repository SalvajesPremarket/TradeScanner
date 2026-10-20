"""Incremental, restart-safe quote scanner for the clean TradeScanner core."""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from tradescanner.candles import CandleStore, normalize_price_history
from tradescanner.float_provider import refresh_float_cache
from tradescanner.infisical_vault import is_configured as infisical_is_configured
from tradescanner.snapshots import BATCH_SIZE, SnapshotStore, collect_snapshots
from tradescanner.schwab import SchwabAPIError, SchwabConfigurationError, get_price_history, get_quotes
from tradescanner.universe import load_symbol_universe

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

        universe = _load_cached_universe(universe_loader)
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
        result = collect_snapshots(batch, target_store, quote_fetcher or get_quotes)
        if result["received_count"] <= 0 or result["snapshots_saved"] <= 0:
            raise SchwabAPIError(
                "Schwab returned no persistable quotes; the universe cursor was not advanced."
            )
        # FMP is used only for public float. Its cached bulk dataset refreshes at
        # most daily (six-hour cooldown on failures); it never supplies quotes.
        float_provider_state = refresh_float_cache(target_store)
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
        }
        # One bounded historical request per successful quote cycle. Prefer the
        # strongest observed GAP from this batch; never block quote persistence
        # or cursor progress if the separate candle endpoint is unavailable.
        history_fetch = candle_fetcher or (get_price_history if quote_fetcher is None else None)
        if history_fetch is not None:
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
                    # Preserve Schwab FLOAT when explicit; the API decorates missing
                    # values from the cached FMP FLOAT dataset without blocking this cycle.
                    candle_state["float_symbol"] = candidate["symbol"] if candidate.get("float_shares") is not None else None
                    candle_state["float_shares"] = candidate.get("float_shares")
                    candle_state["float_source"] = candidate.get("float_source")
            except (SchwabAPIError, SchwabConfigurationError, ValueError, OSError, RuntimeError, sqlite3.Error) as exc:
                candle_state["candle_error"] = type(exc).__name__
        next_cursor = (cursor + len(batch)) % len(symbols)
        target_store.set_state("universe_cursor", next_cursor)
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
            cycles_completed=int(target_store.get_state("cycles_completed", 0)) + 1,
        )
        # Only mark market-data connectivity from provider timestamps that are
        # within the accepted freshness window; a newly fetched stale quote is not live data.
        latest_rows = target_store.latest_for_symbols(batch)
        if any(row.get("quote_is_fresh") is True for row in latest_rows):
            state["last_quote_received_at"] = _now()
            target_store.set_state("engine_status", state)
        target_store.set_state("cycles_completed", state["cycles_completed"])
        return {"status": "completed", **state}
    except (SchwabConfigurationError, SchwabAPIError, ValueError, OSError, sqlite3.Error) as exc:
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
