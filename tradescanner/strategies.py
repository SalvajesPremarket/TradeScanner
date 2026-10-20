"""Persist independent user strategies and evaluate safe formula expressions."""
from __future__ import annotations

import ast
import operator
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from tradescanner.snapshots import SnapshotStore

STATE_KEY = "user_strategies_v1"
MAX_NAME_LENGTH = 80
MAX_SOURCE_LENGTH = 50_000
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
    for node in ast.walk(tree):
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
        values = [_eval(value, row) for value in node.values]
        return all(values) if isinstance(node.op, ast.And) else any(values)
    if isinstance(node, ast.UnaryOp):
        value = _eval(node.operand, row)
        if value is None:
            return False
        if isinstance(node.op, ast.Not): return not value
        if isinstance(node.op, ast.USub): return -value
        return +value
    if isinstance(node, ast.BinOp):
        left, right = _eval(node.left, row), _eval(node.right, row)
        if left is None or right is None: return None
        return _BINARY[type(node.op)](left, right)
    if isinstance(node, ast.Compare):
        left = _eval(node.left, row)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval(comparator, row)
            if left is None or right is None: return False
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
    if len(tree.body) != 1 or not isinstance(tree.body[0], (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise ValueError("Python seguro acepta únicamente def evaluate(stock): return expresión.")
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
    match = re.fullmatch(r"(?is)plot\\s+scan\\s*=\\s*(.*?)\\s*;?", cleaned)
    if match:
        cleaned = match.group(1)
    if ";" in cleaned or re.search(r"(?i)\\b(def|input|script|rec|fold|reference|addlabel|assignpricecolor)\\b", cleaned):
        raise ValueError("ThinkScript: este adaptador admite una expresión de escaneo simple, no bloques ni indicadores con historial.")
    aliases = {
        "close": "price", "last": "price", "volume": "volume",
        "gap_pct": "gap_pct", "change_pct": "change_pct", "rvol": "rvol",
        "float_shares": "float_shares", "open": "price",
    }
    for name, replacement in aliases.items():
        cleaned = re.sub(r"(?i)\\b" + re.escape(name) + r"\\b", replacement, cleaned)
    cleaned = re.sub(r"(?i)\\band\\b", "and", cleaned)
    cleaned = re.sub(r"(?i)\\bor\\b", "or", cleaned)
    cleaned = re.sub(r"(?i)\\bnot\\b", "not", cleaned)
    _validate_formula(cleaned)
    return cleaned


def strategy_matches(strategy: dict[str, Any], row: dict[str, Any]) -> bool:
    language = strategy.get("language")
    source = strategy.get("source", "")
    try:
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
    if language == "formula":
        _validate_formula(source)
    elif language == "python":
        _python_expression(source)
    elif language == "thinkscript":
        _thinkscript_expression(source)
    target = _store(store)
    items = list_strategies(target)
    existing = next((item for item in items if item.get("id") == strategy_id), None) if strategy_id else None
    if strategy_id and existing is None:
        raise KeyError("Estrategia no encontrada.")
    if existing and existing.get("enabled") and language != "formula":
        raise ValueError("Desactiva la estrategia antes de cambiarla a un lenguaje aún no ejecutable.")
    item = {
        "id": strategy_id or str(uuid.uuid4()), "name": clean_name,
        "language": language, "source": source, "enabled": bool(existing and existing.get("enabled")),
        "execution_ready": language in LANGUAGES, "updated_at": datetime.now(timezone.utc).isoformat(),
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
        strategy_matches(selected, {})
        if selected.get("language") == "formula": _validate_formula(selected.get("source", ""))
        elif selected.get("language") == "python": _python_expression(selected.get("source", ""))
        elif selected.get("language") == "thinkscript": _thinkscript_expression(selected.get("source", ""))
        else: raise ValueError("Lenguaje no compatible.")
    for item in items:
        item["enabled"] = bool(enabled and item.get("id") == strategy_id)
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


def active_strategy(store: SnapshotStore | None = None) -> dict[str, Any] | None:
    return next((item for item in list_strategies(store) if item.get("enabled")), None)
