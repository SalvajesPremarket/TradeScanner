"""Non-blocking, idempotent signal notifications for Discord and Telegram."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from tradescanner.snapshots import SnapshotStore

logger = logging.getLogger(__name__)
_QUEUE: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=256)
_LOCK = threading.Lock()
_PENDING: set[str] = set()
_WORKER: threading.Thread | None = None


def _destinations() -> list[tuple[str, str]]:
    destinations: list[tuple[str, str]] = []
    discord = os.getenv("TRADESCANNER_DISCORD_WEBHOOK_URL", "").strip()
    if discord.startswith("https://discord.com/api/webhooks/") or discord.startswith("https://discordapp.com/api/webhooks/"):
        destinations.append(("discord", discord))
    token = os.getenv("TRADESCANNER_TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TRADESCANNER_TELEGRAM_CHAT_ID", "").strip()
    if token and chat_id and all(ch.isalnum() or ch in "_:-" for ch in token):
        destinations.append(("telegram", f"{token}|{chat_id}"))
    return destinations


def _signal_key(action: dict[str, Any], strategy: dict[str, Any]) -> str:
    raw = "|".join((
        str(strategy.get("id", "unknown")),
        str(action.get("symbol", "")).upper(),
        str(action.get("quote_time_ms", "")),
        str(action.get("action", "")).upper(),
    ))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _send(destination: tuple[str, str], message: str) -> None:
    kind, target = destination
    if kind == "discord":
        body = json.dumps({"content": message[:1900]}).encode("utf-8")
        request = urllib.request.Request(target, data=body, headers={"Content-Type": "application/json"}, method="POST")
    else:
        token, chat_id = target.split("|", 1)
        body = urllib.parse.urlencode({"chat_id": chat_id, "text": message[:3900], "disable_web_page_preview": "true"}).encode("utf-8")
        request = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=body, headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
    with urllib.request.urlopen(request, timeout=8) as response:
        if not 200 <= response.status < 300:
            raise RuntimeError(f"{kind} returned HTTP {response.status}")


def _format_message(action: dict[str, Any], strategy: dict[str, Any]) -> str:
    symbol = str(action.get("symbol", "UNKNOWN")).upper()
    name = str(strategy.get("name", "TradeScanner"))
    price = action.get("price", action.get("entry_price"))
    stamp = action.get("quote_time_ms")
    when = datetime.fromtimestamp(float(stamp) / 1000, timezone.utc).isoformat() if isinstance(stamp, (int, float)) else "timestamp unavailable"
    price_text = f"${float(price):.4f}" if isinstance(price, (int, float)) else "not provided"
    return f"TRADE SCANNER · CONFIRMED ENTRY\nStrategy: {name}\nSymbol: {symbol}\nPrice: {price_text}\nObserved: {when} UTC\nMode: alert only; no order was sent."


def _worker() -> None:
    while True:
        item = _QUEUE.get()
        try:
            key = item["key"]
            store = SnapshotStore()
            if store.get_state("signal_alert_sent:" + key, False):
                continue
            message = item["message"]
            for destination in item["destinations"]:
                sent_key = "signal_alert_sent:" + key + ":" + destination[0]
                if store.get_state(sent_key, False):
                    continue
                for attempt in range(3):
                    try:
                        _send(destination, message)
                        store.set_state(sent_key, True)
                        break
                    except Exception as exc:
                        if attempt == 2:
                            logger.warning("Signal notification failed; provider=%s error_type=%s", destination[0], type(exc).__name__)
                        else:
                            time.sleep(2 ** attempt)
            if any(store.get_state("signal_alert_sent:" + key + ":" + d[0], False) for d in item["destinations"]):
                store.set_state("signal_alert_sent:" + key, True)
        except Exception as exc:
            logger.warning("Signal notification worker error; error_type=%s", type(exc).__name__)
        finally:
            with _LOCK:
                _PENDING.discard(item.get("key", ""))
            _QUEUE.task_done()


def _ensure_worker() -> None:
    global _WORKER
    with _LOCK:
        if _WORKER is None or not _WORKER.is_alive():
            _WORKER = threading.Thread(target=_worker, name="signal-notifications", daemon=True)
            _WORKER.start()


def enqueue_confirmed_entry(action: dict[str, Any], strategy: dict[str, Any]) -> bool:
    """Queue an ENTER action once; network delivery never runs in the market-data callback."""
    if not isinstance(action, dict) or str(action.get("action", "")).upper() != "ENTER":
        return False
    symbol = action.get("symbol")
    stamp = action.get("quote_time_ms")
    if not isinstance(symbol, str) or not symbol.strip() or not isinstance(stamp, (int, float)) or stamp <= 0:
        return False
    destinations = _destinations()
    if not destinations:
        return False
    key = _signal_key(action, strategy if isinstance(strategy, dict) else {})
    with _LOCK:
        if key in _PENDING:
            return False
        _PENDING.add(key)
    try:
        if SnapshotStore().get_state("signal_alert_sent:" + key, False):
            with _LOCK:
                _PENDING.discard(key)
            return False
        _QUEUE.put_nowait({"key": key, "message": _format_message(action, strategy), "destinations": destinations})
    except queue.Full:
        with _LOCK:
            _PENDING.discard(key)
        logger.warning("Signal notification queue full; symbol=%s", symbol)
        return False
    except Exception as exc:
        with _LOCK:
            _PENDING.discard(key)
        logger.warning("Could not queue signal notification; error_type=%s", type(exc).__name__)
        return False
    _ensure_worker()
    return True
