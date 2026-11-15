"""Schwab Level One equities websocket feed with bounded rotating subscriptions."""
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
from tradescanner.notifications import enqueue_confirmed_entry
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


def _symbol_batches() -> list[list[str]]:
    """Split the full universe into Schwab-compliant Level One subscription batches."""
    payload = load_symbol_universe()
    raw = payload.get("symbols", []) if isinstance(payload, dict) else []
    symbols = list(dict.fromkeys(s.strip().upper() for s in raw if isinstance(s, str) and s.strip()))
    try:
        batch_size = max(1, min(500, int(os.getenv("TRADESCANNER_STREAM_MAX_SYMBOLS", "500"))))
    except (TypeError, ValueError):
        batch_size = 500
    return [symbols[index:index + batch_size] for index in range(0, len(symbols), batch_size)]


def _symbols() -> list[str]:
    """Compatibility helper: first active-sized batch only."""
    batches = _symbol_batches()
    return batches[0] if batches else []


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
        rotation_stop = threading.Event()
        rotation_thread: threading.Thread | None = None
        try:
            batches = _symbol_batches()
            if not batches:
                raise RuntimeError("Schwab stream has no symbols to subscribe")
            try:
                rotation_seconds = max(15, min(900, int(os.getenv("TRADESCANNER_STREAM_ROTATION_SECONDS", "60"))))
            except (TypeError, ValueError):
                rotation_seconds = 60
            current_batch_index = 0
            pending_batch_index: int | None = None
            symbols = batches[current_batch_index]
            request_id = 0
            request_lock = threading.RLock()
            state_lock = threading.RLock()
            state = {
                "connected": False, "authenticated": False, "subscribed": False,
                "subscription_batch": 1, "last_message_at": None,
                "last_quote_at": None, "received_quotes": 0,
            }

            def persist_status(status: str | None = None, **extra: Any) -> None:
                with state_lock:
                    snapshot = dict(state)
                snapshot.update(extra)
                snapshot.update({
                    "status": status or ("connected" if snapshot.get("connected") else "disconnected"),
                    "provider": "Schwab Level One websocket",
                    "universe_symbols": sum(len(batch) for batch in batches),
                    "subscription_batches": len(batches),
                    "rotation_seconds": rotation_seconds,
                    "updated_at": time.time(),
                })
                try:
                    SnapshotStore().set_state("schwab_stream_status", snapshot)
                except Exception as exc:
                    logger.warning("Schwab stream status persistence failed; error_type=%s", type(exc).__name__)

            info = get_streamer_info()
            url = info.get("streamerSocketUrl")
            required = ("schwabClientCustomerId", "schwabClientCorrelId", "schwabClientChannel", "schwabClientFunctionId")
            if not isinstance(url, str) or not url.startswith("wss://") or not all(info.get(k) for k in required):
                raise SchwabAPIError("Schwab streamer session metadata is incomplete")

            def next_request_id() -> str:
                nonlocal request_id
                with request_lock:
                    request_id += 1
                    return str(request_id)

            def send_request(ws, service: str, command: str, selected_symbols: list[str] | None = None) -> str:
                rid = next_request_id()
                parameters: dict[str, Any] = {}
                if selected_symbols is not None:
                    parameters["keys"] = ",".join(selected_symbols)
                    parameters["fields"] = ",".join(str(n) for n in _FIELDS)
                request = {
                    "service": service, "command": command, "requestid": rid,
                    "SchwabClientCustomerId": info["schwabClientCustomerId"],
                    "SchwabClientCorrelId": info["schwabClientCorrelId"],
                    "SchwabClientChannel": info["schwabClientChannel"],
                    "SchwabClientFunctionId": info["schwabClientFunctionId"],
                    "parameters": parameters,
                }
                ws.send(json.dumps({"requests": [request]}))
                logger.info("Schwab stream request sent; service=%s command=%s request_id=%s symbols=%s",
                            service, command, rid, len(selected_symbols or []))
                return rid

            def send_login(ws) -> None:
                rid = next_request_id()
                ws.send(json.dumps({"requests": [{
                    "service": "ADMIN", "command": "LOGIN", "requestid": rid,
                    "SchwabClientCustomerId": info["schwabClientCustomerId"],
                    "SchwabClientCorrelId": info["schwabClientCorrelId"],
                    "SchwabClientChannel": info["schwabClientChannel"],
                    "SchwabClientFunctionId": info["schwabClientFunctionId"],
                    "parameters": {
                        "Authorization": _access_token(),
                        "SchwabClientCustomerId": info["schwabClientCustomerId"],
                        "SchwabClientCorrelId": info["schwabClientCorrelId"],
                        "SchwabClientChannel": info["schwabClientChannel"],
                        "SchwabClientFunctionId": info["schwabClientFunctionId"],
                    },
                }]}))
                logger.info("Schwab stream LOGIN request sent; request_id=%s", rid)

            def rotate_subscriptions(ws) -> None:
                nonlocal current_batch_index, pending_batch_index, symbols
                while not _STOP.is_set() and not rotation_stop.wait(rotation_seconds):
                    with state_lock:
                        authenticated = state["authenticated"]
                        subscribed = state["subscribed"]
                    if not authenticated or not subscribed or len(batches) < 2:
                        continue
                    old_index = current_batch_index
                    next_index = (old_index + 1) % len(batches)
                    old_symbols = batches[old_index]
                    new_symbols = batches[next_index]
                    try:
                        send_request(ws, "LEVELONE_EQUITIES", "UNSUBS", old_symbols)
                        send_request(ws, "LEVELONE_EQUITIES", "SUBS", new_symbols)
                    except Exception as exc:
                        logger.warning("Schwab stream rotation send failed; error_type=%s", type(exc).__name__)
                        return
                    with state_lock:
                        pending_batch_index = next_index
                        state["subscribed"] = False
                    logger.info("Schwab stream rotation requested; from_batch=%s to_batch=%s symbols=%s",
                                old_index + 1, next_index + 1, len(new_symbols))

            def on_open(ws):
                with state_lock:
                    state["connected"] = True
                    state["last_open_at"] = time.time()
                logger.info("Schwab stream socket opened")
                persist_status("connected", connected=True, authenticated=False, subscribed=False)
                try:
                    send_login(ws)
                except Exception as exc:
                    logger.error("Schwab stream LOGIN send failed; error_type=%s", type(exc).__name__)
                    ws.close()

            def on_message(ws, message):
                nonlocal current_batch_index, pending_batch_index, symbols, rotation_thread
                try:
                    payload = json.loads(message)
                except (TypeError, ValueError):
                    logger.warning("Schwab stream received non-JSON message")
                    return
                if not isinstance(payload, dict):
                    logger.warning("Schwab stream received unexpected message type")
                    return
                with state_lock:
                    state["last_message_at"] = time.time()

                responses = payload.get("response", [])
                if isinstance(responses, list):
                    for response in responses:
                        if not isinstance(response, dict):
                            continue
                        service = response.get("service")
                        command = response.get("command")
                        content = response.get("content") if isinstance(response.get("content"), dict) else {}
                        code = content.get("code")
                        request_id_value = response.get("requestid")
                        ok = str(code) == "0"
                        logger.info("Schwab stream response; service=%s command=%s code=%s request_id=%s",
                                    service, command, code, request_id_value)
                        if service == "ADMIN" and command == "LOGIN":
                            if not ok:
                                logger.error("Schwab stream LOGIN rejected; code=%s", code)
                                persist_status("login_rejected", connected=True, authenticated=False, subscribed=False,
                                               login_code=str(code) if code is not None else None)
                                ws.close()
                                return
                            with state_lock:
                                state["authenticated"] = True
                            logger.info("Schwab stream LOGIN accepted")
                            try:
                                send_request(ws, "LEVELONE_EQUITIES", "SUBS", symbols)
                            except Exception as exc:
                                logger.error("Schwab stream initial SUBS send failed; error_type=%s", type(exc).__name__)
                                ws.close()
                                return
                            persist_status("authenticated", connected=True, authenticated=True, subscribed=False,
                                           subscription_pending=True, subscription_batch=1)
                            if rotation_thread is None and len(batches) > 1:
                                rotation_thread = threading.Thread(
                                    target=rotate_subscriptions, args=(ws,),
                                    name="schwab-levelone-rotation", daemon=True,
                                )
                                rotation_thread.start()
                        elif service == "LEVELONE_EQUITIES" and command in {"SUBS", "UNSUBS"}:
                            if not ok:
                                logger.error("Schwab stream subscription rejected; command=%s code=%s request_id=%s",
                                             command, code, request_id_value)
                                persist_status("subscription_error", connected=True,
                                               authenticated=state["authenticated"], subscribed=False,
                                               subscription_error_code=str(code) if code is not None else None)
                                continue
                            if command == "SUBS":
                                with state_lock:
                                    if pending_batch_index is not None:
                                        current_batch_index = pending_batch_index
                                        pending_batch_index = None
                                    symbols = batches[current_batch_index]
                                    state["subscribed"] = True
                                    state["subscription_batch"] = current_batch_index + 1
                                logger.info("Schwab stream SUBS acknowledged; batch=%s/%s symbols=%s",
                                            current_batch_index + 1, len(batches), len(symbols))
                                persist_status("subscribed", connected=True, authenticated=True, subscribed=True,
                                               subscription_pending=False, subscribed_symbols=len(symbols),
                                               subscription_batch=current_batch_index + 1)
                            else:
                                logger.info("Schwab stream UNSUBS acknowledged; request_id=%s", request_id_value)

                data_blocks = payload.get("data", [])
                if not isinstance(data_blocks, list):
                    return
                for block in data_blocks:
                    if not isinstance(block, dict) or block.get("service") != "LEVELONE_EQUITIES":
                        continue
                    items = block.get("content", [])
                    if not isinstance(items, list):
                        continue
                    rows = []
                    for item in items:
                        if isinstance(item, dict):
                            row = _normalise_stream_item(item)
                            if row:
                                rows.append(row)
                    if not rows:
                        logger.debug("Schwab stream Level One block contained no valid quote rows")
                        continue
                    store = SnapshotStore()
                    apply_cached_float(rows, store)
                    store.save(rows)
                    enabled = [
                        item for item in active_strategies(store)
                        if ("momentum" in str(item.get("name", "")).casefold() and item.get("language") == "python")
                        or is_momentum_trader_program(item)
                    ]
                    runtime_status = "no_active_momentum_strategy"
                    for strategy in enabled:
                        runtime = feed_quote_rows(store, rows, strategy)
                        runtime_status = runtime.get("status", "waiting_for_new_quotes")
                        runtime_evaluations = runtime.get("evaluations", [])
                        runtime_actions = runtime.get("actions", [])
                        actions_by_symbol = {}
                        for action in runtime_actions:
                            actions_by_symbol.setdefault(str(action.get("symbol")), []).append(action)
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
                            "status": runtime_status,
                            "strategy_id": strategy.get("id"), "strategy": strategy.get("name"),
                            "evaluated_at": time.time(), "processed_quotes": runtime.get("processed_quotes", 0),
                            "evaluations": per_symbol, "actions": runtime_actions,
                            "matches": list(dict.fromkeys(str(a.get("symbol")) for a in runtime_actions if a.get("action") == "ENTER")),
                            "note": runtime.get("note"),
                        })
                        for action in runtime_actions:
                            if action.get("action") == "ENTER":
                                enqueue_confirmed_entry(action, strategy)
                    quote_at = time.time()
                    with state_lock:
                        state["last_quote_at"] = quote_at
                        state["received_quotes"] += len(rows)
                        quote_total = state["received_quotes"]
                        batch_index = current_batch_index
                    persist_status("receiving_quotes", connected=True, authenticated=True,
                                   subscribed=state["subscribed"], subscribed_symbols=len(symbols),
                                   subscription_batch=batch_index + 1, last_quote_at=quote_at,
                                   last_message_at=state["last_message_at"], received_quotes=quote_total,
                                   last_quote_count=len(rows), runtime_status=runtime_status,
                                   active_momentum_strategies=len(enabled))
                    store.set_state("engine_status", {
                        **(store.get_state("engine_status", {}) if isinstance(store.get_state("engine_status", {}), dict) else {}),
                        "status": "streaming", "last_quote_received_at": datetime.now(timezone.utc).isoformat(),
                        "received_count": len(rows), "provider": "schwab_stream",
                    })

            def on_error(ws, error):
                logger.warning("Schwab stream socket error; error_type=%s", type(error).__name__)
                persist_status("socket_error", connected=False, authenticated=False, subscribed=False,
                               error_type=type(error).__name__)

            def on_close(ws, status_code, message):
                rotation_stop.set()
                logger.warning("Schwab stream socket closed; code=%s", status_code)
                with state_lock:
                    state["connected"] = False
                    state["authenticated"] = False
                    state["subscribed"] = False
                persist_status("disconnected", connected=False, authenticated=False, subscribed=False,
                               close_code=status_code)

            app = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message,
                                         on_error=on_error, on_close=on_close)
            global _ACTIVE_APP
            _ACTIVE_APP = app
            logger.info("Schwab stream connecting; symbol_batches=%s rotation_seconds=%s",
                        len(batches), rotation_seconds)
            app.run_forever(ping_interval=20, ping_timeout=10)
            rotation_stop.set()
            if rotation_thread is not None and rotation_thread.is_alive():
                rotation_thread.join(timeout=2)
            _ACTIVE_APP = None
            delay = 2.0
            if not _STOP.is_set():
                logger.warning("Schwab stream loop ended; reconnecting after %s seconds", delay)
                _STOP.wait(delay)
        except Exception as exc:
            rotation_stop.set()
            if rotation_thread is not None and rotation_thread.is_alive():
                rotation_thread.join(timeout=2)
            try:
                SnapshotStore().set_state("schwab_stream_status", {
                    "status": "reconnecting", "connected": False, "authenticated": False,
                    "subscribed": False, "error_type": type(exc).__name__,
                    "retry_seconds": delay, "provider": "Schwab Level One websocket",
                })
            except Exception:
                pass
            logger.warning("Schwab streamer unavailable; retrying in %s seconds; error_type=%s",
                           delay, type(exc).__name__)
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
