"""Safe native evaluator for the saved PullBackArteaga strategy contract.

The uploaded Python source is never executed. Its literal CONFIG and TF_CONFIG
are parsed from a restricted AST, while the calculation pipeline below is
implemented with bounded, deterministic Python operations.
"""
from __future__ import annotations

import ast
import math
from datetime import date, datetime, time as dtime
from typing import Any
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
DEFAULT_CONFIG = {
    "price_min": 2.0, "price_max": 30.0, "min_avg_daily_volume": 300_000,
    "float_max": 50_000_000, "ema_periods": (50, 200), "bb_period": 20,
    "bb_std": 2.0, "atr_period": 14, "rsi_period": 14,
    "min_dollar_volume": 2_000_000, "rsi_long_max": 45,
    "rsi_short_min": 55, "require_confirmation": True,
    "trend_guard_pct": 5.0, "rr_min": 1.5, "stop_atr_buffer": 0.25,
    "earnings_min_days": 3, "session_start": dtime(4, 0),
    "session_end": dtime(16, 0),
}
DEFAULT_TF = {
    "D": {"min_bars": 60, "band_tol_atr": .6, "ema_tol_atr": 1.0,
          "swing_lookback": 30, "min_bars_since_touch": 3,
          "min_swing_atr": 3.0, "atr_pct_min": 3.0, "rvol_min": 1.0,
          "vol_divisor": 1},
    "W": {"min_bars": 55, "band_tol_atr": .6, "ema_tol_atr": 1.0,
          "swing_lookback": 20, "min_bars_since_touch": 2,
          "min_swing_atr": 3.0, "atr_pct_min": 6.0, "rvol_min": 0.0,
          "vol_divisor": 5},
}


def _literal(node: ast.AST) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.Tuple, ast.List)):
        values = [_literal(item) for item in node.elts]
        return tuple(values) if isinstance(node, ast.Tuple) else values
    if isinstance(node, ast.Dict):
        return {_literal(k): _literal(v) for k, v in zip(node.keys, node.values)}
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        value = _literal(node.operand)
        return -value if isinstance(node.op, ast.USub) else value
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dtime":
        args = [_literal(arg) for arg in node.args]
        if len(args) in (2, 3) and all(isinstance(v, int) for v in args):
            return dtime(*args)
    raise ValueError("CONFIG contains a non-literal value; native Pullback will not execute uploaded code.")


def strategy_config(source: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = dict(DEFAULT_CONFIG)
    tf_config = {key: dict(value) for key, value in DEFAULT_TF.items()}
    try:
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                name, value_node = node.target.id, node.value
            elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                name, value_node = node.targets[0].id, node.value
            else:
                continue
            if name == "CONFIG" and value_node is not None:
                supplied = _literal(value_node)
                if isinstance(supplied, dict):
                    config.update(supplied)
            elif name == "TF_CONFIG" and value_node is not None:
                supplied = _literal(value_node)
                if isinstance(supplied, dict):
                    for key, value in supplied.items():
                        if key in tf_config and isinstance(value, dict):
                            tf_config[key].update(value)
    except (SyntaxError, ValueError, TypeError) as exc:
        raise ValueError("La configuración de Pullback no es literal o contiene valores no admitidos.") from exc
    return config, tf_config


def to_weekly(daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate completed daily bars into ISO weeks ending Friday."""
    groups: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in daily:
        stamp = datetime.fromtimestamp(row["candle_time_ms"] / 1000, ET).date()
        iso = stamp.isocalendar()
        groups.setdefault((iso.year, iso.week), []).append(row)
    weekly = []
    for key in sorted(groups):
        rows = sorted(groups[key], key=lambda x: x["candle_time_ms"])
        weekly.append({
            "candle_time_ms": rows[-1]["candle_time_ms"],
            "open": float(rows[0]["open"]),
            "high": max(float(x["high"]) for x in rows),
            "low": min(float(x["low"]) for x in rows),
            "close": float(rows[-1]["close"]),
            "volume": sum(int(x["volume"]) for x in rows),
        })
    return weekly


def _ema(values: list[float], period: int) -> list[float]:
    alpha = 2.0 / (period + 1)
    out = []
    for value in values:
        out.append(value if not out else alpha * value + (1 - alpha) * out[-1])
    return out


def _indicators(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any] | None:
    closes = [float(x["close"]) for x in rows]
    highs = [float(x["high"]) for x in rows]
    lows = [float(x["low"]) for x in rows]
    if len(closes) < max(int(cfg["bb_period"]), int(cfg["atr_period"]), int(cfg["rsi_period"])) + 1:
        return None
    n = int(cfg["bb_period"])
    window = closes[-n:]
    mid = sum(window) / n
    variance = sum((x - mid) ** 2 for x in window) / n
    sd = math.sqrt(variance)
    upper, lower = mid + float(cfg["bb_std"]) * sd, mid - float(cfg["bb_std"]) * sd

    tr = []
    for i in range(len(rows)):
        prev = closes[i - 1] if i else closes[i]
        tr.append(max(highs[i] - lows[i], abs(highs[i] - prev), abs(lows[i] - prev)))
    atr_values = _ema_wilder(tr, int(cfg["atr_period"]))
    atr = atr_values[-1]

    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    ups = [max(x, 0.0) for x in changes]
    downs = [max(-x, 0.0) for x in changes]
    avg_up = _ema_wilder(ups, int(cfg["rsi_period"]))[-1]
    avg_down = _ema_wilder(downs, int(cfg["rsi_period"]))[-1]
    rsi = None if avg_down == 0 else 100 - 100 / (1 + avg_up / avg_down)
    emas = {period: _ema(closes, int(period))[-1] for period in cfg["ema_periods"] if len(closes) >= int(period)}
    return {"mid": mid, "upper": upper, "lower": lower, "atr": atr, "rsi": rsi, "emas": emas}


def _ema_wilder(values: list[float], period: int) -> list[float]:
    alpha = 1.0 / period
    out = []
    for value in values:
        out.append(value if not out else alpha * value + (1 - alpha) * out[-1])
    return out


def _scan_timeframe(rows: list[dict[str, Any]], ctx: dict[str, Any], timeframe: str,
                    cfg: dict[str, Any], tf: dict[str, Any]) -> dict[str, Any]:
    out = {"passed": False, "timeframe": timeframe, "side": None, "metrics": {}, "reasons": []}
    if len(rows) < int(tf["min_bars"]):
        out["reasons"].append(f"historial {timeframe}: {len(rows)} velas; requiere {tf['min_bars']}")
        return out
    rows = sorted(rows, key=lambda x: x["candle_time_ms"])
    last = rows[-1]
    price = float(last["close"])
    ind = _indicators(rows, cfg)
    if not ind or ind["atr"] <= 0 or not math.isfinite(ind["atr"]):
        out["reasons"].append("indicadores no calculables")
        return out
    atr = ind["atr"]
    side = "long" if abs(price - ind["lower"]) <= tf["band_tol_atr"] * atr and abs(price - ind["upper"]) > tf["band_tol_atr"] * atr else (
        "short" if abs(price - ind["upper"]) <= tf["band_tol_atr"] * atr and abs(price - ind["lower"]) > tf["band_tol_atr"] * atr else None
    )
    out["side"] = side
    avg_vol = float(ctx.get("avg_volume_30d") or (sum(float(x["volume"]) for x in rows[-20:]) / min(20, len(rows))) / float(tf["vol_divisor"]))
    float_shares = ctx.get("float_shares")
    checks = [
        ("precio", cfg["price_min"] <= price <= cfg["price_max"], round(price, 4), f"precio fuera de ${cfg['price_min']}-${cfg['price_max']}"),
        ("volumen_prom", avg_vol >= cfg["min_avg_daily_volume"], int(avg_vol), f"volumen < {cfg['min_avg_daily_volume']:,}"),
        ("float", isinstance(float_shares, (int, float)) and not isinstance(float_shares, bool) and 0 < float_shares <= cfg["float_max"], float_shares, "float desconocido o superior al máximo"),
        ("volumen_usd", price * avg_vol >= cfg["min_dollar_volume"], int(price * avg_vol), f"volumen en $ < {cfg['min_dollar_volume']:,}"),
        ("atr_pct", atr / price * 100 >= tf["atr_pct_min"], round(atr / price * 100, 2), f"ATR% < {tf['atr_pct_min']}"),
        ("rvol", float(ctx.get("rvol", 0) or 0) >= tf["rvol_min"], ctx.get("rvol"), f"RVol < {tf['rvol_min']}"),
    ]
    best = min(((period, abs(price - value) / atr, value) for period, value in ind["emas"].items()), key=lambda x: x[1], default=None)
    checks.append(("ema_cercana", best is not None and best[1] <= tf["ema_tol_atr"],
                   {"ema": best[0], "dist_atr": round(best[1], 2), "valor": round(best[2], 4)} if best else None,
                   "sin EMA cercana"))
    checks.append(("banda_bollinger", side is not None, side, "no está cerca de una sola banda de Bollinger"))
    if side:
        lo_i = max(0, len(rows) - 1 - int(tf["swing_lookback"]))
        hi_i = len(rows) - 1 - int(tf["min_bars_since_touch"])
        found = None
        for i in range(hi_i, lo_i - 1, -1):
            if side == "long" and float(rows[i]["high"]) >= _bands_at(rows, i, cfg)[0]:
                found = (i, (float(rows[i]["high"]) - price) / atr)
                break
            if side == "short" and float(rows[i]["low"]) <= _bands_at(rows, i, cfg)[1]:
                found = (i, (price - float(rows[i]["low"])) / atr)
                break
        checks.append(("swing_desde_banda_contraria", found is not None and found[1] >= tf["min_swing_atr"],
                       {"velas_atras": len(rows) - 1 - found[0], "swing_atr": round(found[1], 2)} if found else None,
                       "no vino de banda contraria o swing insuficiente"))
        if cfg["require_confirmation"]:
            rng = max(float(last["high"]) - float(last["low"]), 1e-9)
            wick = ((min(float(last["open"]), price) - float(last["low"])) / rng) if side == "long" else ((float(last["high"]) - max(float(last["open"]), price)) / rng)
            confirmed = price > float(last["open"]) or wick >= .4 if side == "long" else price < float(last["open"]) or wick >= .4
        else:
            wick, confirmed = None, True
        checks.append(("confirmacion", confirmed, round(wick, 2) if wick is not None else "off", "sin vela de confirmación"))
        rsi_ok = ind["rsi"] is not None and (ind["rsi"] <= cfg["rsi_long_max"] if side == "long" else ind["rsi"] >= cfg["rsi_short_min"])
        checks.append(("rsi", rsi_ok, round(ind["rsi"], 1) if ind["rsi"] is not None else None, "RSI sin agotamiento a favor"))
        ema200 = ind["emas"].get(200)
        trend_ok = True if ema200 is None else (price >= ema200 * (1 - cfg["trend_guard_pct"] / 100) if side == "long" else price <= ema200 * (1 + cfg["trend_guard_pct"] / 100))
        checks.append(("tendencia_ema200", trend_ok, round((price / ema200 - 1) * 100, 2) if ema200 else "sin EMA200", "contra tendencia mayor (EMA200)"))
        buffer = cfg["stop_atr_buffer"] * atr
        stop = min(float(x["low"]) for x in rows[-3:]) - buffer if side == "long" else max(float(x["high"]) for x in rows[-3:]) + buffer
        risk, reward = (price - stop, ind["mid"] - price) if side == "long" else (stop - price, price - ind["mid"])
        rr = reward / risk if risk > 0 else -math.inf
        checks.append(("riesgo_beneficio", rr >= cfg["rr_min"], {"rr_a_media": round(rr, 2), "stop": round(stop, 4), "objetivo_1": round(ind["mid"], 4)}, f"R:R {rr:.2f} < {cfg['rr_min']}"))
    days_to_earnings = ctx.get("days_to_earnings")
    if days_to_earnings is not None:
        checks.append(("earnings", days_to_earnings >= cfg["earnings_min_days"], days_to_earnings, f"earnings en {days_to_earnings} días"))
    for name, ok, value, reason in checks:
        out["metrics"][name] = value
        if not ok:
            out["reasons"].append(f"{name}: {reason}")
    out["metrics"].update({"bb_superior": round(ind["upper"], 4), "bb_inferior": round(ind["lower"], 4),
                           "atr": round(atr, 4), "rsi": round(ind["rsi"], 1) if ind["rsi"] is not None else None,
                           "emas": {str(k): round(v, 4) for k, v in ind["emas"].items()},
                           "avg_volume": int(avg_vol)})
    out["passed"] = not out["reasons"]
    return out


def _bands_at(rows: list[dict[str, Any]], index: int, cfg: dict[str, Any]) -> tuple[float, float]:
    period = int(cfg["bb_period"])
    window = [float(x["close"]) for x in rows[max(0, index - period + 1):index + 1]]
    mean = sum(window) / len(window)
    variance = sum((x - mean) ** 2 for x in window) / len(window)
    spread = float(cfg["bb_std"]) * math.sqrt(variance)
    return mean + spread, mean - spread


def evaluate(daily: list[dict[str, Any]], ctx: dict[str, Any], source: str) -> dict[str, Any]:
    try:
        cfg, tf_cfg = strategy_config(source)
    except ValueError as exc:
        return {"passed": False, "strategy": "PullBackArteaga", "symbol": ctx.get("symbol"),
                "side": None, "metrics": {}, "reasons": [str(exc)], "timeframe": "D/W"}
    now_ms = ctx.get("quote_time_ms")
    quote_time = datetime.fromtimestamp(now_ms / 1000, ET) if isinstance(now_ms, (int, float)) and now_ms > 0 else None
    out = {"passed": False, "strategy": "PullBackArteaga", "symbol": ctx.get("symbol"),
           "side": None, "metrics": {}, "reasons": [], "timeframe": "D/W"}
    if quote_time is None:
        out["reasons"].append("cotización sin hora válida")
        return out
    clock = quote_time.timetz().replace(tzinfo=None)
    if not cfg["session_start"] <= clock < cfg["session_end"]:
        out["reasons"].append("fuera de horario configurado")
        return out
    if not daily:
        out["reasons"].append("historial diario: pendiente o no disponible")
        return out
    avg_volume = sum(float(x["volume"]) for x in daily[-30:]) / min(30, len(daily))
    context = {**ctx, "avg_volume_30d": ctx.get("avg_volume_30d") or avg_volume}
    results = []
    for timeframe, rows in (("D", daily), ("W", to_weekly(daily))):
        tf = tf_cfg.get(timeframe, DEFAULT_TF[timeframe])
        # The source strategy defines RVol as the latest bar volume divided by
        # the mean of the previous 20 bars on the same timeframe.
        previous_volumes = [float(x["volume"]) for x in rows[-21:-1]]
        rvol = None
        if len(previous_volumes) == 20:
            baseline = sum(previous_volumes) / 20
            rvol = float(rows[-1]["volume"]) / baseline if baseline > 0 else None
        timeframe_context = {**context, "rvol": rvol}
        result = _scan_timeframe(rows, timeframe_context, timeframe, cfg, tf)
        result["metrics"]["rvol"] = round(rvol, 4) if rvol is not None else None
        results.append(result)
    passed = [item for item in results if item["passed"]]
    out["passed"] = bool(passed)
    out["side"] = passed[0]["side"] if passed else next((x["side"] for x in results if x["side"]), None)
    out["metrics"] = {"daily": results[0]["metrics"], "weekly": results[1]["metrics"],
                      "daily_bars": len(daily), "weekly_bars": len(to_weekly(daily)),
                      "avg_volume_30d": round(avg_volume, 2)}
    out["reasons"] = [] if passed else [f"{x['timeframe']}: {reason}" for x in results for reason in x["reasons"]]
    out["timeframe_results"] = results
    return out
