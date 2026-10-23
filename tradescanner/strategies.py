"""Persist independent user strategies and evaluate safe formula expressions."""
from __future__ import annotations

import ast
import math
import operator
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from tradescanner.snapshots import SnapshotStore

STATE_KEY = "user_strategies_v1"
MAX_NAME_LENGTH = 80
MAX_SOURCE_LENGTH = 50_000
MAX_AST_NODES = 300
MAX_AST_DEPTH = 40
MAX_STRING_LENGTH = 256
MAX_RESULT_STRING_LENGTH = 1024
MAX_INTEGER_BITS = 256
LANGUAGES = {"formula", "python", "thinkscript"}

_BINARY = {
    ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
    ast.Div: operator.truediv, ast.Mod: operator.mod,
}
_COMPARE = {
    ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.Lt: operator.lt,
    ast.LtE: operator.le, ast.Gt: operator.gt, ast.GtE: operator.ge,
}
_ALLOWED_NAMES = {
    "price", "previous_close", "change", "change_pct", "gap_pct",
    "float_shares", "rvol", "volume", "sector", "symbol",
}
_ALLOWED_CALLS = {"abs": abs, "min": min, "max": max}


def _store(store: SnapshotStore | None = None) -> SnapshotStore:
    return store or SnapshotStore()


def list_strategies(store: SnapshotStore | None = None) -> list[dict[str, Any]]:
    value = _store(store).get_state(STATE_KEY, [])
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _validate_formula(source: str) -> ast.Expression:
    if not source or len(source) > MAX_SOURCE_LENGTH:
        raise ValueError("La fórmula está vacía o supera el límite permitido.")
    try:
        tree = ast.parse(source, mode="eval")
    except (SyntaxError, ValueError):
        raise ValueError("La fórmula no tiene una sintaxis válida. Usa expresiones como: gap_pct >= 3 and volume >= 500000") from None
    nodes = list(ast.walk(tree))
    if len(nodes) > MAX_AST_NODES:
        raise ValueError("La fórmula es demasiado compleja.")
    def depth(node: ast.AST, level: int = 1) -> int:
        if level > MAX_AST_DEPTH:
            raise ValueError("La fórmula supera la profundidad permitida.")
        children = list(ast.iter_child_nodes(node))
        return max((depth(child, level + 1) for child in children), default=level)
    depth(tree)
    for node in nodes:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str) and len(node.value) > MAX_STRING_LENGTH:
                raise ValueError("Las cadenas de texto son demasiado largas.")
            if isinstance(node.value, int) and not isinstance(node.value, bool) and node.value.bit_length() > MAX_INTEGER_BITS:
                raise ValueError("El número entero supera el límite permitido.")
            if isinstance(node.value, float) and (not math.isfinite(node.value) or abs(node.value) > 1e100):
                raise ValueError("El número está fuera del rango permitido.")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"min", "max"} and len(node.args) > 20:
            raise ValueError("min() y max() admiten como máximo 20 argumentos.")
        if isinstance(node, ast.Name) and node.id not in _ALLOWED_NAMES | set(_ALLOWED_CALLS):
            raise ValueError(f"Campo o función no permitido: {node.id}")
        if isinstance(node, ast.Call) and (not isinstance(node.func, ast.Name) or node.func.id not in _ALLOWED_CALLS):
            raise ValueError("Solo se permiten las funciones abs(), min() y max().")
        if isinstance(node, ast.BinOp) and type(node.op) not in _BINARY:
            raise ValueError("Operador matemático no permitido.")
        if isinstance(node, ast.Compare) and any(type(op) not in _COMPARE for op in node.ops):
            raise ValueError("Comparación no permitida.")
        if isinstance(node, (ast.Attribute, ast.Subscript, ast.Lambda, ast.Dict, ast.ListComp, ast.GeneratorExp, ast.Await, ast.NamedExpr)):
            raise ValueError("La fórmula contiene una construcción no permitida.")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float, bool, str, type(None))):
            raise ValueError("Constante no permitida.")
        if not isinstance(node, (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.USub, ast.UAdd, ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Name, ast.Call, ast.Constant, ast.Load)):
            raise ValueError("La fórmula contiene una construcción no permitida.")
    return tree


def _eval(node: ast.AST, row: dict[str, Any]) -> Any:
    if isinstance(node, ast.Expression):
        return _eval(node.body, row)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in _ALLOWED_NAMES:
            return row.get(node.id)
        return _ALLOWED_CALLS[node.id]
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            for value in node.values:
                if not _eval(value, row):
                    return False
            return True
        for value in node.values:
            if _eval(value, row):
                return True
        return False
    if isinstance(node, ast.UnaryOp):
        value = _eval(node.operand, row)
        if value is None:
            return False
        if isinstance(node.op, ast.Not): return not value
        if isinstance(node.op, ast.USub): return -value
        return +value
    if isinstance(node, ast.BinOp):
        left, right = _eval(node.left, row), _eval(node.right, row)
        if left is None or right is None:
            return None
        # Permit bounded string concatenation/repetition only; never allocate
        # an unbounded result from a user-supplied strategy.
        if isinstance(left, str) or isinstance(right, str):
            if isinstance(node.op, ast.Add) and isinstance(left, str) and isinstance(right, str):
                if len(left) + len(right) > MAX_RESULT_STRING_LENGTH:
                    return None
                return left + right
            if isinstance(node.op, ast.Mult):
                string_value = left if isinstance(left, str) else right
                count = right if isinstance(left, str) else left
                if isinstance(string_value, str) and isinstance(count, int) and not isinstance(count, bool):
                    if count < 0 or len(string_value) * count > MAX_RESULT_STRING_LENGTH:
                        return None
                    return string_value * count
            return None
        if not isinstance(left, (int, float, bool)) or not isinstance(right, (int, float, bool)):
            return None
        if isinstance(left, int) and not isinstance(left, bool) and left.bit_length() > MAX_INTEGER_BITS:
            return None
        if isinstance(right, int) and not isinstance(right, bool) and right.bit_length() > MAX_INTEGER_BITS:
            return None
        result = _BINARY[type(node.op)](left, right)
        if isinstance(result, int) and not isinstance(result, bool) and result.bit_length() > MAX_INTEGER_BITS:
            return None
        if isinstance(result, float) and (not math.isfinite(result) or abs(result) > 1e100):
            return None
        return result
    if isinstance(node, ast.Compare):
        left = _eval(node.left, row)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval(comparator, row)
            if left is None or right is None: return None
            try:
                accepted = _COMPARE[type(op)](left, right)
            except (TypeError, ValueError):
                return False
            if not accepted: return False
            left = right
        return True
    if isinstance(node, ast.Call):
        function = _eval(node.func, row)
        args = [_eval(arg, row) for arg in node.args]
        if any(arg is None for arg in args): return False
        return function(*args)
    raise ValueError("Construcción no permitida.")



class _PythonStockFields(ast.NodeTransformer):
    """Convert only stock['known_field'] references into safe formula names."""
    def visit_Subscript(self, node: ast.Subscript):
        if not (isinstance(node.value, ast.Name) and node.value.id == "stock"):
            raise ValueError("Python solo permite acceder a campos mediante stock['campo'].")
        key = node.slice
        if not isinstance(key, ast.Constant) or not isinstance(key.value, str) or key.value not in _ALLOWED_NAMES:
            raise ValueError("Campo stock no permitido.")
        return ast.copy_location(ast.Name(id=key.value, ctx=ast.Load()), node)


def _python_expression(source: str) -> str:
    if not source or len(source) > MAX_SOURCE_LENGTH:
        raise ValueError("El script está vacío o supera el límite permitido.")
    try:
        tree = ast.parse(source, mode="exec")
    except (SyntaxError, ValueError):
        try:
            expression = ast.parse(source, mode="eval")
            _validate_formula(ast.unparse(expression))
            return ast.unparse(expression)
        except (SyntaxError, ValueError):
            raise ValueError("Python seguro acepta una expresión o una función evaluate(stock) con un único return.") from None
    if len(tree.body) == 1 and isinstance(tree.body[0], ast.Expr):
        result = ast.unparse(tree.body[0].value)
        _validate_formula(result)
        return result
    if len(tree.body) != 1 or not isinstance(tree.body[0], (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise ValueError("Python seguro acepta una expresión o def evaluate(stock): return expresión.")
    function = tree.body[0]
    if isinstance(function, ast.AsyncFunctionDef) or function.name != "evaluate" or len(function.args.args) != 1 or function.args.args[0].arg != "stock":
        raise ValueError("La función debe llamarse evaluate(stock).")
    if function.decorator_list or function.args.vararg or function.args.kwarg or function.args.kwonlyargs or len(function.body) != 1 or not isinstance(function.body[0], ast.Return) or function.body[0].value is None:
        raise ValueError("La función debe contener solamente una instrucción return.")
    expression = _PythonStockFields().visit(function.body[0].value)
    ast.fix_missing_locations(expression)
    result = ast.unparse(expression)
    _validate_formula(result)
    return result


def _thinkscript_expression(source: str) -> str:
    """Translate a deliberately small, stateless ThinkScript-like scan subset."""
    if not source or len(source) > MAX_SOURCE_LENGTH:
        raise ValueError("El script está vacío o supera el límite permitido.")
    cleaned = source.strip()
    # Permit a single plot assignment, optionally ending with a semicolon.
    match = re.fullmatch(r"(?is)plot\s+scan\s*=\s*(.*?)\s*;?", cleaned)
    if match:
        cleaned = match.group(1)
    if ";" in cleaned or re.search(r"(?i)\b(def|input|script|rec|fold|reference|addlabel|assignpricecolor)\b", cleaned):
        raise ValueError("ThinkScript: este adaptador admite una expresión de escaneo simple, no bloques ni indicadores con historial.")
    aliases = {
        "close": "price", "last": "price", "volume": "volume",
        "gap_pct": "gap_pct", "change_pct": "change_pct", "rvol": "rvol",
        "float_shares": "float_shares", "open": "price",
    }
    for name, replacement in aliases.items():
        cleaned = re.sub(r"(?i)\b" + re.escape(name) + r"\b", replacement, cleaned)
    cleaned = re.sub(r"(?i)\band\b", "and", cleaned)
    cleaned = re.sub(r"(?i)\bor\b", "or", cleaned)
    cleaned = re.sub(r"(?i)\bnot\b", "not", cleaned)
    _validate_formula(cleaned)
    return cleaned


def is_momentum_trader_program(strategy: dict[str, Any]) -> bool:
    """Recognize the original stateful MomentumTrader contract without executing uploaded code."""
    if strategy.get("language") != "python":
        return False
    source = strategy.get("source", "")
    return (
        isinstance(source, str)
        and "class MomentumTrader" in source
        and "def on_tick(" in source
        and "def _entry_filters(" in source
        and "def _manage_position(" in source
    )


def is_momentum_scan_program(strategy: dict[str, Any]) -> bool:
    """Recognize the original scan(df, ctx) Momentum contract without executing it."""
    if strategy.get("language") != "python":
        return False
    source = strategy.get("source", "")
    return (
        isinstance(source, str)
        and "STRATEGY_NAME" in source
        and re.search(r"(?m)^\\s*def\\s+scan\\s*\\(", source) is not None
        and re.search(r"(?m)^\\s*CONFIG(?:\\s*:[^=]+)?\\s*=", source) is not None
    )


def strategy_matches(strategy: dict[str, Any], row: dict[str, Any]) -> bool:
    language = strategy.get("language")
    source = strategy.get("source", "")
    try:
        if ("momentum" in strategy.get("name", "").casefold() and language == "python") or is_momentum_trader_program(strategy):
            return False  # Dedicated OHLCV/context pipeline evaluates this strategy.
        if language == "formula":
            expression = source
        elif language == "python":
            expression = _python_expression(source)
        elif language == "thinkscript":
            expression = _thinkscript_expression(source)
        else:
            return False
        return formula_matches(expression, row)
    except (SyntaxError, TypeError, ValueError):
        return False


def formula_matches(source: str, row: dict[str, Any]) -> bool:
    try:
        return bool(_eval(_validate_formula(source), row))
    except (ArithmeticError, TypeError, ValueError, KeyError):
        return False


def save_strategy(name: str, language: str, source: str, strategy_id: str | None = None,
                  store: SnapshotStore | None = None) -> dict[str, Any]:
    clean_name = name.strip() if isinstance(name, str) else ""
    if not clean_name or len(clean_name) > MAX_NAME_LENGTH:
        raise ValueError("El nombre es obligatorio y debe tener 80 caracteres o menos.")
    if language not in LANGUAGES:
        raise ValueError("Lenguaje no reconocido.")
    if not isinstance(source, str) or not source.strip() or len(source) > MAX_SOURCE_LENGTH:
        raise ValueError("El código está vacío o supera el límite de 50.000 caracteres.")
    # Saving is separate from execution support: preserve the user's full source
    # even when it uses constructs outside this scanner's safe execution subset.
    # Such strategies remain stored but cannot be activated until adapted.
    execution_error = None
    # Momentum has a dedicated, bounded evaluator in tradescanner.momentum.
    is_momentum = (("momentum" in clean_name.casefold() and language == "python") or is_momentum_trader_program({"language": language, "source": source}) or is_momentum_scan_program({"language": language, "source": source}))
    try:
        if is_momentum:
            if not (is_momentum_scan_program({"language": language, "source": source}) or is_momentum_trader_program({"language": language, "source": source})):
                raise ValueError("La estrategia debe coincidir con el contrato Momentum registrado (scan o MomentumTrader completo).")
        elif language == "formula":
            _validate_formula(source)
        elif language == "python":
            _python_expression(source)
        elif language == "thinkscript":
            _thinkscript_expression(source)
    except (SyntaxError, TypeError, ValueError) as exc:
        execution_error = str(exc) or "El código no pertenece al subconjunto ejecutable del motor."
    target = _store(store)
    items = list_strategies(target)
    existing = next((item for item in items if item.get("id") == strategy_id), None) if strategy_id else None
    if strategy_id and existing is None:
        raise KeyError("Estrategia no encontrada.")
    item = {
        "id": strategy_id or str(uuid.uuid4()), "name": clean_name,
        "language": language, "source": source, "enabled": bool(existing and existing.get("enabled")),
        "execution_ready": execution_error is None,
        "execution_error": execution_error,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if item["enabled"] and language == "formula":
        _validate_formula(source)
    if existing:
        items = [item if entry.get("id") == strategy_id else entry for entry in items]
    else:
        items.append(item)
    target.set_state(STATE_KEY, items)
    return item


def set_strategy_enabled(strategy_id: str, enabled: bool,
                         store: SnapshotStore | None = None) -> dict[str, Any]:
    target = _store(store)
    items = list_strategies(target)
    selected = next((item for item in items if item.get("id") == strategy_id), None)
    if selected is None:
        raise KeyError("Estrategia no encontrada.")
    if enabled:
        if not selected.get("execution_ready", False):
            detail = selected.get("execution_error") or "El código no pertenece al subconjunto ejecutable del motor."
            raise ValueError("La estrategia se guardó, pero no se puede activar todavía: " + detail)
        if ("momentum" in selected.get("name", "").casefold() and selected.get("language") == "python") or is_momentum_trader_program(selected) or is_momentum_scan_program(selected):
            source = selected.get("source", "")
            if not (is_momentum_scan_program(selected) or is_momentum_trader_program(selected)):
                raise ValueError("La estrategia debe coincidir con el contrato Momentum registrado (scan o MomentumTrader completo).")
        elif selected.get("language") == "formula": _validate_formula(selected.get("source", ""))
        elif selected.get("language") == "python": _python_expression(selected.get("source", ""))
        elif selected.get("language") == "thinkscript": _thinkscript_expression(selected.get("source", ""))
        else: raise ValueError("Lenguaje no compatible.")
    for item in items:
        if item.get("id") == strategy_id:
            item["enabled"] = bool(enabled)
    target.set_state(STATE_KEY, items)
    return next(item for item in items if item.get("id") == strategy_id)


def delete_strategy(strategy_id: str, store: SnapshotStore | None = None) -> bool:
    target = _store(store)
    items = list_strategies(target)
    updated = [item for item in items if item.get("id") != strategy_id]
    if len(updated) == len(items):
        raise KeyError("Estrategia no encontrada.")
    target.set_state(STATE_KEY, updated)
    return True


def active_strategies(store: SnapshotStore | None = None) -> list[dict[str, Any]]:
    """Return every enabled strategy; activation is independent per strategy."""
    return [item for item in list_strategies(store) if item.get("enabled")]


def active_strategy(store: SnapshotStore | None = None) -> dict[str, Any] | None:
    """Backward-compatible accessor for callers that need one active strategy."""
    return next(iter(active_strategies(store)), None)
