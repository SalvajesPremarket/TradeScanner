"""Schwab Level One equities websocket feed. Only provider-emitted updates become ticks."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any
from datetime import datetime, timezone

import websocket

from tradescanner.core import _number
from tradescanner.float_provider import apply_cached_float
from tradescanner.momentum_runtime import feed_quote_rows
from tradescanner.schwab import _access_token, get_streamer_info, SchwabAPIError
from tradescanner.snapshots import SnapshotStore
from tradescanner.strategies import active_strategies, is_momentum_trader_program
from tradescanner.universe import load_symbol_universe

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_FIELDS = [0, 1, 2, 3, 4, 5, 8, 10, 11, 12, 13, 15, 28, 36, 47, 48]
_STOP = threading.Event()
_THREAD: threading.Thread | None = None
_ACTIVE_APP: websocket.WebSocketApp | None = None


def _symbols() -> list[str]:
    payload = load_symbol_universe()
    raw = payload.get("symbols", []) if isinstance(payload, dict) else []
    result = list(dict.fromkeys(s.strip().upper() for s in raw if isinstance(s, str) and s.strip()))
    # Schwab Level One subscriptions have provider limits. Do not silently claim
    # symbols outside the subscribed set are being streamed.
    limit = max(1, min(500, int(os.getenv("TRADESCANNER_STREAM_MAX_SYMBOLS", "500"))))
    return result[:limit]


def _normalise_stream_item(item: dict[str, Any]) -> dict[str, Any] | None:
    key = item.get("key") or item.get("0")
    fields = item.get("1") if isinstance(item.get("1"), dict) else item
    if not isinstance(key, str) or not key.strip():
        return None
    symbol = key.strip().upper()
    price = _number(fields.get("3"))
    close = _number(fields.get("15"))
    volume = _number(fields.get("8"))
    stamp = _number(fields.get("47")) or _number(fields.get("10"))
    if price is None or price <= 0 or close is None or close <= 0 or stamp is None or stamp <= 0:
        return None
    age = time.time() - stamp / 1000
    if age < -30 or age > 300:
        return None
    gap = (price / close - 1) * 100
    return {
        "symbol": symbol, "price": price, "previous_close": close,
        "change": _number(fields.get("28")),
        "change_pct": ((price / close) - 1) * 100,
        "gap_pct": gap, "volume": volume, "float_shares": None, "rvol": None,
        "quote_time_ms": int(stamp), "quote_age_seconds": round(age, 3),
        "quote_is_fresh": True, "data_source": "schwab_stream",
        "company": None, "sector": None, "float_source": None,
    }


def _run_stream() -> None:
    delay = 2.0
    while not _STOP.is_set():
        try:
            symbols = _symbols()
            if not symbols:
                raise RuntimeError("Schwab stream has no symbols to subscribe")
            info = get_streamer_info()
            url = info.get("streamerSocketUrl")
            required = ("schwabClientCustomerId", "schwabClientCorrelId", "schwabClientChannel", "schwabClientFunctionId")
            if not isinstance(url, str) or not url.startswith("wss://") or not all(info.get(k) for k in required):
                raise SchwabAPIError("Schwab streamer session metadata is incomplete")
            subscribed = False
            request_id = 0

            def send_login(ws):
                nonlocal request_id
                request_id += 1
                ws.send(json.dumps({"requests": [{
                    "service": "ADMIN", "command": "LOGIN", "requestid": str(request_id),
                    "SchwabClientCustomerId": info["schwabClientCustomerId"],
                    "SchwabClientCorrelId": info["schwabClientCorrelId"],
                    "SchwabClientChannel": info["schwabClientChannel"],
                    "SchwabClientFunctionId": info["schwabClientFunctionId"],
                    "parameters": {"Authorization": _access_token(), "SchwabClientCustomerId": info["schwabClientCustomerId"],
                                   "SchwabClientCorrelId": info["schwabClientCorrelId"],
                                   "SchwabClientChannel": info["schwabClientChannel"],
                                   "SchwabClientFunctionId": info["schwabClientFunctionId"]},
                }]}))

            def on_open(ws):
                send_login(ws)

            def on_message(ws, message):
                nonlocal subscribed, request_id
                try:
                    payload = json.loads(message)
                except (TypeError, ValueError):
                    return
                for response in payload.get("response", []) if isinstance(payload, dict) else []:
                    if response.get("service") == "ADMIN" and response.get("command") == "LOGIN":
                        if str(response.get("content", {}).get("code")) != "0":
                            logger.error("Schwab streamer login rejected")
                            ws.close()
                            return
                        if not subscribed:
                            request_id += 1
                            ws.send(json.dumps({"requests": [{
                                "service": "LEVELONE_EQUITIES", "command": "SUBS", "requestid": str(request_id),
                                "SchwabClientCustomerId": info["schwabClientCustomerId"],
                                "SchwabClientCorrelId": info["schwabClientCorrelId"],
                                "SchwabClientChannel": info["schwabClientChannel"],
                                "SchwabClientFunctionId": info["schwabClientFunctionId"],
                                "parameters": {"keys": ",".join(symbols), "fields": ",".join(str(n) for n in _FIELDS)},
                            }]}))
                            subscribed = True
                            logger.warning("Schwab Level One stream subscription requested; symbols=%s", len(symbols))
                for block in payload.get("data", []) if isinstance(payload, dict) else []:
                    if block.get("service") != "LEVELONE_EQUITIES":
                        continue
                    rows = []
                    for item in block.get("content", []):
                        row = _normalise_stream_item(item)
                        if row:
                            rows.append(row)
                    if not rows:
                        continue
                    store = SnapshotStore()
                    apply_cached_float(rows, store)
                    store.save(rows)
                    enabled = [
                        item for item in active_strategies(store)
                        if ("momentum" in str(item.get("name", "")).casefold() and item.get("language") == "python")
                        or is_momentum_trader_program(item)
                    ]
                    runtime = feed_quote_rows(store, rows) if enabled else {"status": "no_active_momentum_strategy"}
                    if enabled:
                        runtime_evaluations = runtime.get("evaluations", [])
                        runtime_actions = runtime.get("actions", [])
                        actions_by_symbol = {}
                        for action in runtime_actions:
                            actions_by_symbol.setdefault(str(action.get("symbol")), []).append(action)
                        for strategy in enabled:
                            per_symbol = []
                            for evaluation in runtime_evaluations:
                                symbol = str(evaluation.get("symbol", ""))
                                symbol_actions = actions_by_symbol.get(symbol, [])
                                entry = next((action for action in symbol_actions if action.get("action") == "ENTER"), None)
                                per_symbol.append({
                                    **evaluation, "passed": entry is not None,
                                    "action": entry.get("action") if entry else None,
                                    "strategy_id": strategy.get("id"), "strategy": strategy.get("name"),
                                    "stop": entry.get("stop") if entry else None,
                                })
                            store.set_state("strategy_scan_status:" + str(strategy.get("id")), {
                                "status": runtime.get("status", "waiting_for_new_quotes"),
                                "strategy_id": strategy.get("id"), "strategy": strategy.get("name"),
                                "evaluated_at": time.time(), "processed_quotes": runtime.get("processed_quotes", 0),
                                "evaluations": per_symbol, "actions": runtime_actions,
                                "matches": list(dict.fromkeys(str(a.get("symbol")) for a in runtime_actions if a.get("action") == "ENTER")),
                                "note": runtime.get("note"),
                            })
                    store.set_state("schwab_stream_status", {
                        "status": "connected", "provider": "Schwab Level One websocket",
                        "subscribed_symbols": len(symbols), "last_message_at": time.time(),
                        "received_quotes": len(rows), "runtime_status": runtime.get("status"),
                        "active_momentum_strategies": len(enabled),
                        "note": "Solo ticks emitidos por Schwab; no se interpolan ni fabrican precios.",
                    })
                    store.set_state("engine_status", {
                        **(store.get_state("engine_status", {}) if isinstance(store.get_state("engine_status", {}), dict) else {}),
                        "status": "streaming", "last_quote_received_at": datetime.now(timezone.utc).isoformat(),
                        "received_count": len(rows), "provider": "schwab_stream",
                    })

            def on_error(ws, error):
                logger.warning("Schwab streamer socket error type=%s", type(error).__name__)

            def on_close(ws, status_code, message):
                logger.warning("Schwab streamer socket closed code=%s", status_code)

            app = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            global _ACTIVE_APP
            _ACTIVE_APP = app
            app.run_forever(ping_interval=20, ping_timeout=10)
            _ACTIVE_APP = None
            delay = 2.0
        except Exception as exc:
            try:
                SnapshotStore().set_state("schwab_stream_status", {
                    "status": "reconnecting", "error_type": type(exc).__name__,
                    "retry_seconds": delay, "provider": "Schwab Level One websocket",
                })
            except Exception:
                pass
            logger.warning("Schwab streamer unavailable; retrying in %s seconds; error_type=%s", delay, type(exc).__name__)
            _STOP.wait(delay)
            delay = min(delay * 2, 60.0)


def start_schwab_stream() -> None:
    global _THREAD
    enabled = os.getenv("TRADESCANNER_SCHWAB_STREAM_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled or (_THREAD is not None and _THREAD.is_alive()):
        return
    _STOP.clear()
    _THREAD = threading.Thread(target=_run_stream, name="schwab-levelone-stream", daemon=True)
    _THREAD.start()


def stop_schwab_stream() -> None:
    _STOP.set()
    app = _ACTIVE_APP
    if app is not None:
        try:
            app.close()
        except Exception:
            pass
    thread = _THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=3)
