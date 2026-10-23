"""Feed the original MomentumTrader with observed Schwab quote updates only.

This adapter intentionally does not synthesize per-second ticks from REST polling.
A symbol is advanced only when Schwab provides a new, timestamped quote.
"""
from __future__ import annotations

import ast
import threading
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from tradescanner.candles import CandleStore
from tradescanner.momentum_trader import CONFIG as DEFAULT_CONFIG, MomentumTrader
from tradescanner.momentum_data import average_daily_volume_30d, recent_stock_news

ET = ZoneInfo("America/New_York")
_LOCK = threading.RLock()
_TRADERS: dict[tuple[str, str], MomentumTrader] = {}
_LAST_QUOTE: dict[tuple[str, str], tuple[int, float]] = {}
_SEEDED: set[tuple[str, str]] = set()
_TRADER_CONFIGS: dict[tuple[str, str], tuple[str, tuple[tuple[str, Any], ...]]] = {}


def _strategy_side(strategy: dict[str, Any] | None) -> str:
    """Read a literal SIDE constant only; never execute uploaded Python source."""
    source = strategy.get("source", "") if isinstance(strategy, dict) else ""
    try:
        tree = ast.parse(source, mode="exec")
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "SIDE"
                for target in node.targets
            ):
                value = ast.literal_eval(node.value)
                if value in {"LONG", "SHORT"}:
                    return value
    except (SyntaxError, ValueError, TypeError):
        pass
    return "LONG"


def _strategy_config(strategy: dict[str, Any] | None) -> dict[str, Any]:
    """Read literal CONFIG values from the uploaded original; never execute its source."""
    source = strategy.get("source", "") if isinstance(strategy, dict) else ""
    try:
        tree = ast.parse(source, mode="exec")
    except (SyntaxError, ValueError):
        return {}
    config_node = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "CONFIG" for target in node.targets
        ):
            config_node = node.value
            break
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "CONFIG":
            config_node = node.value
            break
    if not isinstance(config_node, ast.Dict):
        return {}
    parsed: dict[str, Any] = {}
    for key_node, value_node in zip(config_node.keys, config_node.values):
        try:
            key = ast.literal_eval(key_node)
            value = ast.literal_eval(value_node)
        except (ValueError, TypeError, SyntaxError):
            continue
        if not isinstance(key, str) or key not in DEFAULT_CONFIG:
            continue
        default = DEFAULT_CONFIG[key]
        # Accept literal primitive values only; never execute uploaded expressions.
        if default is None:
            if value is None:
                parsed[key] = value
        elif isinstance(default, bool):
            if isinstance(value, bool):
                parsed[key] = value
        elif isinstance(default, int) and not isinstance(default, bool):
            if isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 1000:
                parsed[key] = value
        elif isinstance(default, float):
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                number = float(value)
                if number == number and abs(number) != float("inf") and abs(number) <= 1_000_000:
                    parsed[key] = number
        elif isinstance(default, str) and isinstance(value, str) and len(value) <= 256:
            parsed[key] = value
    return parsed


def _parse_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed = []
    for item in items:
        headline = item.get("headline")
        published = item.get("published_at")
        if not isinstance(headline, str) or not isinstance(published, str):
            continue
        try:
            stamp = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except ValueError:
            continue
        parsed.append({"headline": headline, "published_at": stamp})
    return parsed


def feed_quote_rows(
    store: Any,
    rows: list[dict[str, Any]],
    strategy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Advance an isolated original MomentumTrader per strategy and symbol."""
    strategy_id = str(strategy.get("id", "default")) if isinstance(strategy, dict) else "default"
    side = _strategy_side(strategy)
    candle_store = CandleStore(store.path)
    actions: list[dict[str, Any]] = []
    processed = 0
    skipped: dict[str, str] = {}
    evaluations: list[dict[str, Any]] = []
    with _LOCK:
        for row in rows:
            symbol = row.get("symbol")
            stamp = row.get("quote_time_ms")
            price = row.get("price")
            volume = row.get("volume")
            if not isinstance(symbol, str) or not symbol:
                continue
            if row.get("quote_is_fresh") is not True:
                skipped[symbol] = "quote_not_fresh"
                continue
            if not isinstance(stamp, (int, float)) or stamp <= 0 or not isinstance(price, (int, float)) or price <= 0:
                skipped[symbol] = "missing_real_quote_timestamp_or_price"
                continue
            stamp = int(stamp)
            runtime_key = (strategy_id, symbol)
            previous = _LAST_QUOTE.get(runtime_key)
            if previous is not None and (stamp, float(price)) == previous:
                skipped[symbol] = "duplicate_quote"
                continue
            # Ignore out-of-order provider observations. Never invent an intermediate tick.
            if previous is not None and stamp < previous[0]:
                skipped[symbol] = "out_of_order_quote"
                continue
            trader = _TRADERS.get(runtime_key)
            if trader is None:
                cfg = _strategy_config(strategy)
                trader = MomentumTrader(symbol, side=side, cfg=cfg)
                _TRADERS[runtime_key] = trader
                _TRADER_CONFIGS[runtime_key] = (side, tuple(sorted(cfg.items())))
            if runtime_key not in _SEEDED:
                candles = candle_store.latest(symbol, limit=6000)
                # Seed only closed candles strictly older than this quote's minute.
                quote_minute = stamp // 60000
                closed = [c for c in candles if c.get("candle_time_ms", 0) // 60000 < quote_minute]
                if closed:
                    trader.seed([
                        {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"]}
                        for c in closed
                    ])
                _SEEDED.add(runtime_key)
            # Mandatory context comes only from provider/cached market data.
            daily = average_daily_volume_30d(store, symbol)
            news = recent_stock_news(store, symbol)
            trader.set_context({
                "prev_close": row.get("previous_close"),
                "avg_volume_30d": daily.get("average"),
                "float_shares": row.get("float_shares"),
                "news": _parse_news(news.get("news", [])),
            })
            ts = datetime.fromtimestamp(stamp / 1000, ET)
            emitted = trader.on_tick(
                float(price), ts,
                float(volume) if isinstance(volume, (int, float)) else None,
            )
            _LAST_QUOTE[runtime_key] = (stamp, float(price))
            processed += 1
            evaluations.append({
                "symbol": symbol,
                "quote_time_ms": stamp,
                "price": float(price),
                "position_open_in_strategy_state": trader.pos is not None,
                "metrics": dict(trader.last_metrics),
                "reasons": list(trader.last_reject),
                "actions": [action.get("action") for action in emitted],
            })
            for action in emitted:
                actions.append({**action, "quote_time_ms": stamp, "data_source": "schwab_observed_quote"})
        state = {
            "status": "fed_quotes" if processed else "waiting_for_new_quotes",
            "strategy_id": strategy_id,
            "side": side,
            "evaluated_at": datetime.now(ET).isoformat(),
            "processed_quotes": processed,
            "actions": actions,
            "evaluations": evaluations,
            "skipped": skipped,
            "note": "MomentumTrader original alimentado solo con actualizaciones observadas de Schwab; no se fabrican ticks ni se ejecutan órdenes.",
        }
        store.set_state("momentum_trader_runtime_status:" + strategy_id, state)
        if strategy_id == "default":
            store.set_state("momentum_trader_runtime_status", state)
        return state
