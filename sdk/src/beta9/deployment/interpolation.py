"""Compose and Dockerfile variable interpolation without environment access."""

import re
from typing import Any, Dict, List, Set

VARIABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def interpolate(text: str, variables: Dict[str, str], missing: Set[str]) -> str:
    """Compose interpolation: $VAR, ${VAR}, ${VAR:-default}, ${VAR-default},
    ${VAR:?error}, ${VAR:+alternative}, nested defaults, and $$ for a literal $."""
    out: List[str] = []
    i = 0
    while i < len(text):
        if text[i] != "$":
            out.append(text[i])
            i += 1
        elif text.startswith("$$", i):
            out.append("$")
            i += 2
        elif text.startswith("${", i):
            end = _closing_brace(text, i + 1)
            if end < 0:
                out.append(text[i:])
                break
            out.append(_expand(text[i + 2 : end], variables, missing))
            i = end + 1
        else:
            match = VARIABLE.match(text, i + 1)
            if match:
                out.append(_lookup(match.group(), variables, missing))
                i = match.end()
            else:
                out.append("$")
                i += 1
    return "".join(out)


def _closing_brace(text: str, start: int) -> int:
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    return -1


def _lookup(name: str, variables: Dict[str, str], missing: Set[str]) -> str:
    if name not in variables:
        missing.add(name)
    return variables.get(name, "")


def _expand(expression: str, variables: Dict[str, str], missing: Set[str]) -> str:
    match = VARIABLE.match(expression)
    if not match:
        return ""
    name, rest = match.group(), expression[match.end() :]
    value = variables.get(name)
    for operator in (":-", ":?", ":+", "-", "?", "+"):
        if rest.startswith(operator):
            empty = value is None or (operator.startswith(":") and value == "")
            if operator.endswith("?"):
                if empty:
                    missing.add(name)
                return value or ""
            if empty == operator.endswith("-"):
                return interpolate(rest[len(operator) :], variables, missing)
            return value or ""
    return _lookup(name, variables, missing)


def _interpolate_tree(node: Any, variables: Dict[str, str], missing: Set[str]) -> Any:
    if isinstance(node, str):
        return interpolate(node, variables, missing)
    if isinstance(node, list):
        return [_interpolate_tree(item, variables, missing) for item in node]
    if isinstance(node, dict):
        return {key: _interpolate_tree(value, variables, missing) for key, value in node.items()}
    return node
