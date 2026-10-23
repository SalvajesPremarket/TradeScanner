"""
TRADEBOOT - MOMENTUM POR TRAMOS (velas de 1 minuto)  --  version LARGO
=======================================================================
El mismo archivo sirve para largo y corto: solo cambia SIDE ("LONG" / "SHORT").
tradeboot_largo.py = SIDE "LONG"   |   tradeboot_corto.py = SIDE "SHORT"

REGLAS (largo; el corto es el espejo exacto)
  ENTRADA  (al volver a la apertura en los primeros 20 s de la vela)
    - la vela nace por encima de la EMA20 (como mucho 1 vela previa ya por encima)
    - MACD positivo
    - sin EMA50 / EMA200 por encima en el camino (a menos de CONFIG["clearance_pct"] %)
    - tramo 1 (seg 0-20): abre, baja, regresa a su apertura (libelula) -> ENTRA
      si primero sube, regresa a la apertura y se va negativa -> NO entra / cierra
  VELA 1 (la de entrada)
    - tramo 2 (21-40) y tramo 3 (41-60) positivas -> sigue abierta
    - regresa a la apertura en tramo 3 -> cierra de inmediato
  VELA 2
    - abre positiva y su minimo > minimo de la vela 1 -> sigue abierta
    - tras 20 s positiva -> stop al cierre de la vela 1
    - regresa a su apertura -> cierra
    - si no cumple -> espera a la vela 3 (sin mover el stop)
  VELA 3 EN ADELANTE
    - positiva con minimo superior -> sigue abierta y stop al cierre de la vela anterior
    - tocar Bandas de Bollinger y seguir positiva -> sigue abierta
  SALIDA
    - vela que toca la banda de Bollinger y forma lapida (gravestone) -> cierra
  CORTO = todo invertido: nace bajo la EMA20, MACD negativo, sin EMA50/200 debajo,
    entrada si sube y regresa a la apertura, maximo inferior, stop baja,
    banda inferior + libelula (dragonfly) -> cierra.

FILTROS DE SCANNER (se evaluan en el momento de la entrada; TODOS son obligatorios)
    - Top gainer: sube >= 5% vs el cierre anterior (ideal >= 10%)      [gap_min_pct / gap_ideal_pct]
    - RVol >= 5x: volumen de hoy / volumen promedio diario de 30 dias    [rvol_min]
    - Precio entre $1 y $20                                              [price_min / price_max]
    - Float < 20,000,000 acciones                                        [float_max]
    - Noticia fresca obligatoria (earnings, FDA, contratos, fusiones, alianzas)  [news_*]
    En el corto son los mismos filtros (acciones que ya subieron fuerte y estan agotandose).
    Se apagan todos juntos con CONFIG["use_scanner_filters"] = False.

COMO LO USA EL TRADEBOOT
    bot   = MomentumTrader("ABCD")                      # un objeto por simbolo
    bot.set_context({"prev_close": 3.10,                # cierre regular del dia anterior
                     "avg_volume_30d": 850_000,         # volumen diario promedio 30 dias
                     "float_shares": 12_500_000,
                     "news": [{"published_at": datetime, "headline": "texto"}]})
                    # opcional: "avg_premarket_volume_30d" (si existe, se usa para el RVol)
    bot.seed(velas_cerradas_1m)                         # opcional pero recomendado (200+ velas)
    acciones = bot.on_tick(precio, datetime_con_segundos, volumen_acumulado_del_dia)   # en CADA tick/segundo
    # volumen_acumulado_del_dia = volumen total de hoy hasta ahora (premarket incluido)

    Cada accion es un dict:
      {"action": "ENTER" | "EXIT" | "MOVE_STOP", "symbol", "side": "LONG"|"SHORT",
       "order": "BUY"|"SELL"|None, "price", "stop", "reason"}
    ENTER     -> abrir a mercado (la cantidad de acciones la define tu interfaz) y poner stop en `stop`
    MOVE_STOP -> mover el stop existente a `stop` (solo se mueve a favor, nunca atras)
    EXIT      -> cerrar toda la posicion a mercado

    El tamano de posicion (dinero, % diario, cantidad de acciones) NO esta aqui: lo maneja la interfaz.
    Si la interfaz no puede ejecutar un ENTER (sin fondos, limite diario...), llamar bot.cancel_entry()
    para que el motor no crea que hay una posicion abierta.

LO QUE NO ESTABA EN TUS REGLAS (lo defini yo, todo editable en CONFIG)
    - stop inicial: extremo de la vela de entrada (la mecha de la libelula) + colchon,
      con tope maximo de perdida `hard_stop_pct`
    - tramo 2 de la vela 1: si pierde la apertura tambien cierra (t2_close_if_back_to_open)
    - tolerancias de "regresa a la apertura" y tamano minimo de la mecha de entrada
    - forma exacta de la lapida (cuerpo pequeno, mecha larga hacia la banda)
"""

from __future__ import annotations

import math
from collections import deque
from datetime import datetime, time as dtime
from typing import Any

SIDE = "LONG"   # "LONG" o "SHORT"
STRATEGY_NAME = "Momentum Tramos"

# ----------------------------------------------------------------------------
# CONFIG: cambia numeros aqui, no toques la logica de abajo.
# ----------------------------------------------------------------------------
CONFIG: dict[str, Any] = {
    # tramos de la vela (segundos)
    "t1_end": 20,
    "t2_end": 40,

    # entrada
    "ema_fast": 20,
    "max_prior_streak": 1,        # velas previas cerradas del lado correcto de la EMA20 (1 = "recien naciendo")
    "macd_fast": 12, "macd_slow": 26,
    "clearance_pct": 2.0,         # EMA50/EMA200 "en el camino" si estan a menos de este % por delante
    "open_tol_pct": 0.02,         # tolerancia para decir "se alejo de la apertura"
    "min_dip_pct": 0.05,          # mecha minima (en %) de la libelula antes de regresar a la apertura
    "trading_window": None,       # ej. (dtime(4, 0), dtime(9, 30)) en la hora de tus ticks; None = siempre

    # gestion
    "t2_close_if_back_to_open": True,
    "close_on_return_from_candle3": False,
    "candle2_stop_delay_sec": 20,
    "stop_buffer_pct": 0.05,      # colchon bajo/sobre el extremo de la vela de entrada
    "hard_stop_pct": 3.0,         # perdida maxima desde la entrada (stop de emergencia)

    # Bollinger y lapida / libelula
    "bb_period": 20, "bb_std": 2.0,
    "gs_body_max": 0.20,          # cuerpo <= 20% del rango de la vela
    "gs_reject_wick_min": 0.60,   # mecha hacia la banda >= 60% del rango
    "gs_small_wick_max": 0.20,    # mecha contraria <= 20%
    "gs_live_from_sec": 45,       # evaluar la lapida en vivo desde este segundo (ademas del cierre)

    # filtros de scanner (los de tu Estrategia 1)
    "use_scanner_filters": True,
    "gap_min_pct": 5.0,           # top gainer minimo (tu rango: 4-5%)
    "gap_ideal_pct": 10.0,        # a partir de aqui se marca "gap_ideal" en las metricas
    "rvol_min": 5.0,
    "price_min": 1.0, "price_max": 20.0,
    "float_max": 20_000_000,
    "news_required": True,
    "news_max_age_hours": 16,     # "fresca": publicada en las ultimas N horas
    "news_require_keyword": False,  # True = solo cuenta si coincide con un catalizador conocido
}

CATALYSTS: dict[str, tuple[str, ...]] = {
    "earnings": ("earnings", "quarter", "q1", "q2", "q3", "q4", "revenue", "eps",
                 "guidance", "resultados", "ganancias", "trimestre"),
    "fda": ("fda", "phase", "trial", "approval", "approves", "clearance", "nda", "bla",
            "aprobacion", "aprobación", "ensayo"),
    "contrato": ("contract", "award", "government", "department of", "pentagon", "dod",
                 "contrato", "gobierno"),
    "fusion": ("merger", "acquisition", "acquire", "acquires", "buyout", "takeover",
               "fusion", "fusión", "adquisicion", "adquisición"),
    "alianza": ("partnership", "partners", "collaboration", "agreement", "alliance",
                "alianza", "acuerdo", "asociacion", "asociación"),
}

# ----------------------------------------------------------------------------
# Indicadores incrementales
# ----------------------------------------------------------------------------
class _EMA:
    def __init__(self, n: int):
        self.n, self.a, self.v, self.count = n, 2 / (n + 1), None, 0

    def update(self, x: float) -> float:
        self.v = x if self.v is None else self.a * x + (1 - self.a) * self.v
        self.count += 1
        return self.v

    def peek(self, x: float) -> float:
        return x if self.v is None else self.a * x + (1 - self.a) * self.v

    @property
    def ready(self) -> bool:
        return self.count >= self.n


# ----------------------------------------------------------------------------
# Motor de la estrategia (un objeto por simbolo)
# ----------------------------------------------------------------------------
class MomentumTrader:
    def __init__(self, symbol: str, side: str = SIDE, cfg: dict | None = None):
        self.symbol = symbol
        self.side = side.upper()
        self.d = 1 if self.side == "LONG" else -1
        self.cfg = {**CONFIG, **(cfg or {})}
        self.ema = {n: _EMA(n) for n in (self.cfg["ema_fast"], 50, 200)}
        self.m_fast, self.m_slow = _EMA(self.cfg["macd_fast"]), _EMA(self.cfg["macd_slow"])
        self.closes: deque[float] = deque(maxlen=self.cfg["bb_period"])
        self.streak = 0
        self.prev: dict | None = None
        self.c: dict | None = None
        self.pos: dict | None = None
        self.last_reject: list[str] = []
        self.ctx: dict = {}
        self.day_volume: float | None = None
        self.last_metrics: dict = {}

    # ---------------- datos historicos ----------------
    def seed(self, candles: list[dict]) -> None:
        """Velas CERRADAS de 1 min en orden: {"open","high","low","close"}."""
        for cd in candles:
            self._update_indicators(cd["close"])
            self.prev = self._snap(cd)

    def set_context(self, ctx: dict) -> None:
        """Datos del simbolo que no salen de los ticks: prev_close, avg_volume_30d, float_shares, news."""
        self.ctx = ctx or {}

    # ---------------- entrada principal ----------------
    def on_tick(self, price: float, ts: datetime, day_volume: float | None = None) -> list[dict]:
        actions: list[dict] = []
        if day_volume is not None:
            self.day_volume = day_volume
        start = ts.replace(second=0, microsecond=0)
        if self.c is None:
            self._new_candle(start, price)
        elif start > self.c["start"]:
            actions += self._close_candle()
            self._new_candle(start, price)
        elif start < self.c["start"]:
            return actions  # tick atrasado

        c = self.c
        c["high"], c["low"], c["close"] = max(c["high"], price), min(c["low"], price), price
        sec = ts.second + ts.microsecond / 1e6
        actions += self._manage(price, sec, ts)
        return actions

    # ---------------- logica por tick ----------------
    def _manage(self, price: float, sec: float, ts: datetime) -> list[dict]:
        cfg, d, c = self.cfg, self.d, self.c
        o = c["open"]
        dev = d * (price - o) / o * 100        # % a favor (+) o en contra (-) respecto a la apertura
        tol = cfg["open_tol_pct"]

        if c["first_move"] is None and abs(dev) >= tol:
            c["first_move"] = "fav" if dev > 0 else "adv"
        c["adv_dip"] = max(c["adv_dip"], -dev)
        if sec <= cfg["t1_end"]:
            if dev >= tol:
                c["went_fav_t1"] = True
            elif c["went_fav_t1"] and dev <= -tol:
                c["pattern_inverted"] = True   # subio, regreso a la apertura y se fue en contra

        if self.pos is None:
            return self._try_enter(price, sec, ts, dev)
        return self._manage_position(price, sec, dev)

    # ---------------- entrada ----------------
    def _try_enter(self, price, sec, ts, dev) -> list[dict]:
        cfg, c = self.cfg, self.c
        if (sec > cfg["t1_end"] or c["entered_here"] or c["pattern_inverted"]
                or c["first_move"] != "adv" or c["adv_dip"] < cfg["min_dip_pct"] or dev < 0):
            return []
        reasons = self._entry_filters(price, ts)
        self.last_reject = reasons
        if reasons:
            return []

        d = self.d
        extreme = c["low"] if d == 1 else c["high"]
        stop = extreme - d * price * cfg["stop_buffer_pct"] / 100
        hard = price * (1 - d * cfg["hard_stop_pct"] / 100)
        stop = max(stop, hard) if d == 1 else min(stop, hard)
        if d * (price - stop) <= 0:
            stop = hard
        self.pos = {"entry": price, "stop": stop, "k": 1}
        c["entered_here"] = True
        return [self._act("ENTER", price, stop=stop, reason="libelula en tramo 1 + filtros OK",
                          metrics=dict(self.last_metrics))]

    def _entry_filters(self, price: float, ts: datetime) -> list[str]:
        cfg, d, c = self.cfg, self.d, self.c
        r: list[str] = []
        w = cfg["trading_window"]
        if w and not (w[0] <= ts.time() < w[1]):
            r.append("fuera de horario")
        r += self._scanner_filters(price, ts)
        e20 = self.ema[cfg["ema_fast"]]
        macd = self._macd_live(price)
        if not e20.ready or macd is None:
            r.append("sin historial suficiente")
            return r
        if d * (c["open"] - e20.v) <= 0 or d * (price - e20.peek(price)) <= 0:
            r.append("la vela no nace del lado correcto de la EMA20")
        if self.streak > cfg["max_prior_streak"]:
            r.append(f"ya lleva {self.streak} velas del lado de la EMA20")
        if d * macd <= 0:
            r.append("MACD en contra")
        for n in (50, 200):
            e = self.ema[n]
            if e.ready:
                ahead = d * (e.v - price)
                if 0 < ahead / price * 100 <= cfg["clearance_pct"]:
                    r.append(f"EMA{n} en el camino")
        return r

    # ---------------- gestion de la posicion ----------------
    def _manage_position(self, price, sec, dev) -> list[dict]:
        cfg, d, c, pos = self.cfg, self.d, self.c, self.pos
        if d * (price - pos["stop"]) <= 0:
            return self._exit(price, "stop")

        k = pos["k"]
        if k == 1:
            if sec <= cfg["t1_end"]:
                if c["pattern_inverted"]:
                    return self._exit(price, "subio, regreso a la apertura y se fue en contra (tramo 1)")
            elif sec <= cfg["t2_end"]:
                if cfg["t2_close_if_back_to_open"] and dev <= 0:
                    return self._exit(price, "perdio la apertura en tramo 2")
            elif dev <= 0:
                return self._exit(price, "regreso a la apertura en tramo 3")
        else:
            prev = self.prev
            adv_ext = c["low"] if d == 1 else c["high"]
            higher = prev is not None and d * (adv_ext - prev["adv_ext"]) > 0   # minimo superior / maximo inferior
            positive_open = c["first_move"] == "fav"
            if k == 2 and positive_open and dev <= 0:
                return self._exit(price, "vela 2 regreso a su apertura")
            if k >= 3 and cfg["close_on_return_from_candle3"] and positive_open and dev <= 0:
                return self._exit(price, "regreso a su apertura")
            if higher and dev > 0 and (k >= 3 or (positive_open and sec >= cfg["candle2_stop_delay_sec"])):
                a = self._raise_stop(prev["close"], price)
                if a:
                    return [a]

        if sec >= cfg["gs_live_from_sec"] and self._is_reversal_candle(c, price):
            return self._exit(price, "lapida/libelula tocando Bollinger")
        return []

    def _raise_stop(self, new_stop: float, price: float) -> dict | None:
        d, pos = self.d, self.pos
        if d * (new_stop - pos["stop"]) > 0 and d * (price - new_stop) > 0:
            pos["stop"] = new_stop
            return self._act("MOVE_STOP", price, stop=new_stop, reason="stop al cierre de la vela anterior")
        return None

    def _exit(self, price: float, reason: str) -> list[dict]:
        pos = self.pos
        pnl = self.d * (price - pos["entry"])
        a = self._act("EXIT", price, stop=pos["stop"], reason=reason, pnl_por_accion=round(pnl, 4))
        self.pos = None
        return [a]

    def cancel_entry(self) -> None:
        """Llamar si la interfaz NO pudo ejecutar el ENTER: borra la posicion que el motor creyo abierta."""
        self.pos = None

    # ---------------- cierre de vela ----------------
    def _close_candle(self) -> list[dict]:
        c, d, actions = self.c, self.d, []
        if self.pos:
            if self._is_reversal_candle(c, c["close"]):
                actions += self._exit(c["close"], "lapida/libelula al cierre de la vela")
            elif self.pos["k"] == 1 and d * (c["close"] - c["open"]) <= 0:
                actions += self._exit(c["close"], "la vela de entrada cerro sin ganancia")
        self._update_indicators(c["close"])
        self.prev = self._snap(c)
        if self.pos:
            self.pos["k"] += 1
        return actions

    def _new_candle(self, start: datetime, price: float) -> None:
        self.c = {"start": start, "open": price, "high": price, "low": price, "close": price,
                  "first_move": None, "adv_dip": 0.0, "went_fav_t1": False,
                  "pattern_inverted": False, "entered_here": False}

    def _snap(self, c: dict) -> dict:
        return {"close": c["close"], "open": c["open"], "high": c["high"], "low": c["low"],
                "adv_ext": c["low"] if self.d == 1 else c["high"]}

    # ---------------- indicadores ----------------
    def _update_indicators(self, close: float) -> None:
        for e in self.ema.values():
            e.update(close)
        self.m_fast.update(close)
        self.m_slow.update(close)
        self.closes.append(close)
        e20 = self.ema[self.cfg["ema_fast"]]
        self.streak = self.streak + 1 if self.d * (close - e20.v) > 0 else 0

    def _macd_live(self, price: float) -> float | None:
        if not self.m_slow.ready:
            return None
        return self.m_fast.peek(price) - self.m_slow.peek(price)

    def _bands(self, price: float) -> tuple[float, float] | None:
        n = self.cfg["bb_period"]
        hist = list(self.closes)[-(n - 1):]
        if len(hist) < n - 1:
            return None
        vals = hist + [price]
        m = sum(vals) / n
        sd = math.sqrt(sum((v - m) ** 2 for v in vals) / n)
        return m + self.cfg["bb_std"] * sd, m - self.cfg["bb_std"] * sd

    def _is_reversal_candle(self, c: dict, close_price: float) -> bool:
        """Largo: lapida en banda superior. Corto: libelula en banda inferior."""
        bands = self._bands(close_price)
        rng = c["high"] - c["low"]
        if bands is None or rng <= 0:
            return False
        cfg, o, h, l = self.cfg, c["open"], c["high"], c["low"]
        body = abs(close_price - o) / rng
        top_wick = (h - max(o, close_price)) / rng
        bot_wick = (min(o, close_price) - l) / rng
        if self.d == 1:
            touched, reject, small = h >= bands[0], top_wick, bot_wick
        else:
            touched, reject, small = l <= bands[1], bot_wick, top_wick
        return (touched and body <= cfg["gs_body_max"]
                and reject >= cfg["gs_reject_wick_min"] and small <= cfg["gs_small_wick_max"])

    # ---------------- filtros de scanner ----------------
    def _scanner_filters(self, price: float, ts: datetime) -> list[str]:
        cfg, ctx = self.cfg, self.ctx
        r: list[str] = []
        m: dict[str, Any] = {}
        self.last_metrics = m
        if not cfg["use_scanner_filters"]:
            return r

        m["precio"] = round(price, 4)
        if not (cfg["price_min"] <= price <= cfg["price_max"]):
            r.append(f"precio fuera de ${cfg['price_min']}-${cfg['price_max']}")

        pc = ctx.get("prev_close")
        if not pc or pc <= 0:
            r.append("sin cierre anterior (prev_close)")
        else:
            gap = (price - pc) / pc * 100
            m["gap_pct"], m["gap_ideal"] = round(gap, 2), gap >= cfg["gap_ideal_pct"]
            if gap < cfg["gap_min_pct"]:
                r.append(f"gap {gap:.1f}% < {cfg['gap_min_pct']}%")

        base = ctx.get("avg_premarket_volume_30d") or ctx.get("avg_volume_30d")
        vol = self.day_volume if self.day_volume is not None else ctx.get("volume_today")
        if not base or base <= 0 or vol is None:
            r.append("sin volumen promedio 30d o volumen de hoy")
        else:
            rvol = vol / base
            m["rvol"] = round(rvol, 2)
            if rvol < cfg["rvol_min"]:
                r.append(f"RVol {rvol:.1f}x < {cfg['rvol_min']}x")

        fl = ctx.get("float_shares")
        if not fl or fl <= 0:
            r.append("float desconocido")
        else:
            m["float"] = int(fl)
            if fl >= cfg["float_max"]:
                r.append(f"float >= {cfg['float_max']:,}")

        if cfg["news_required"]:
            news = self._fresh_news(ts)
            if news is None:
                r.append("sin noticia fresca")
            else:
                m["noticia"] = news
        return r

    def _fresh_news(self, now: datetime) -> dict | None:
        cfg, best = self.cfg, None
        for n in self.ctx.get("news") or []:
            pub = n["published_at"]
            if pub.tzinfo is None and now.tzinfo is not None:
                pub = pub.replace(tzinfo=now.tzinfo)
            elif pub.tzinfo is not None and now.tzinfo is None:
                now = now.replace(tzinfo=pub.tzinfo)
            age_h = (now - pub).total_seconds() / 3600
            if age_h > cfg["news_max_age_hours"]:
                continue
            head = n.get("headline", "")
            cat = next((k for k, words in CATALYSTS.items() if any(w in head.lower() for w in words)), None)
            if cfg["news_require_keyword"] and cat is None:
                continue
            if best is None or age_h < best[0]:
                best = (age_h, cat or "otro", head[:120])
        return None if best is None else {"categoria": best[1], "titular": best[2], "edad_horas": round(best[0], 1)}

    # ---------------- utilidades ----------------
    def _act(self, kind: str, price: float, **kw) -> dict:
        long_ = self.d == 1
        order = {"ENTER": "BUY" if long_ else "SELL", "EXIT": "SELL" if long_ else "BUY"}.get(kind)
        return {"action": kind, "symbol": self.symbol, "side": self.side, "order": order,
                "price": round(price, 4), **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in kw.items()}}