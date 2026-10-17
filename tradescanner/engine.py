"""Incremental, restart-safe quote scanner for the clean TradeScanner core."""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from tradescanner.snapshots import BATCH_SIZE, SnapshotStore, collect_snapshots
from tradescanner.schwab import SchwabAPIError, SchwabConfigurationError, get_quotes
from tradescanner.universe import load_symbol_universe

UNIVERSE_CACHE_SECONDS = 6 * 60 * 60
_universe_cache: dict[str, Any] | None = None
_universe_cached_at = 0.0
_universe_lock = threading.Lock()
_cycle_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _credentials_configured() -> bool:
    return all(os.getenv(key, "").strip() for key in (
        "SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET", "SCHWAB_REFRESH_TOKEN"
    ))


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
            last_success_at=_now(),
            last_error=None,
            last_cycle_duration_seconds=round(time.monotonic() - started, 3),
            requested_count=result["requested_count"],
            received_count=result["received_count"],
            snapshots_saved=result["snapshots_saved"],
            cycles_completed=int(target_store.get_state("cycles_completed", 0)) + 1,
        )
        target_store.set_state("cycles_completed", state["cycles_completed"])
        return {"status": "completed", **state}
    except (SchwabConfigurationError, SchwabAPIError, ValueError, OSError) as exc:
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
