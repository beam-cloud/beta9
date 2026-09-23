"""
`${{...}}` reference grammar: validation. The gateway resolves
(pkg/gateway/services/references.go).
"""

import re
from typing import Dict, Iterable, List, Sequence, Union

REFERENCE_RE = re.compile(r"\$\{\{\s*([^}]*?)\s*\}\}")

DB_FIELDS = ("DATABASE_URL", "REDIS_URL", "URL", "USERNAME", "PASSWORD", "DATABASE", "HOST", "PORT")

_SECRET_FN = re.compile(r"^secret\(\s*(\d+)?\s*(?:,\s*(\"[^\"]*\"|'[^']*'))?\s*\)$")
_RANDOM_FN = re.compile(r"^randomInt\(\s*(-?\d+)?\s*(?:,\s*(-?\d+))?\s*\)$")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")


def find_references(value: str) -> List[str]:
    return [m.group(1).strip() for m in REFERENCE_RE.finditer(value or "")]


def validate_expression(expr: str) -> List[str]:
    """Return problems with one reference expression (empty when valid)."""
    if expr.startswith("secret."):
        name = expr[len("secret.") :]
        return [] if _NAME.match(name) else [f"secret reference {expr!r} needs a secret name"]
    if expr.startswith("db."):
        parts = expr[len("db.") :].split(".")
        if len(parts) != 2 or not parts[0]:
            return [f"database reference {expr!r} must be db.<name>.<field>"]
        if parts[1].upper() not in DB_FIELDS:
            return [f"database field {parts[1]!r} is not one of {', '.join(DB_FIELDS)}"]
        return []
    if expr.startswith("app."):
        parts = expr[len("app.") :].split(".")
        if len(parts) != 2 or parts[1].upper() != "URL":
            return [f"app reference {expr!r} must be app.<name>.URL"]
        return []
    if expr.startswith("secret("):
        m = _SECRET_FN.match(expr)
        if not m:
            return [
                f'{expr!r} must be secret(), secret(<length>) or secret(<length>, "<alphabet>")'
            ]
        if m.group(1) and not 1 <= int(m.group(1)) <= 512:
            return ["secret() length must be between 1 and 512"]
        return []
    if expr.startswith("randomInt("):
        m = _RANDOM_FN.match(expr)
        if not m:
            return [f"{expr!r} must be randomInt(), randomInt(<max>) or randomInt(<min>, <max>)"]
        if m.group(1) is not None and m.group(2) is not None and int(m.group(2)) <= int(m.group(1)):
            return ["randomInt() max must be greater than min"]
        return []
    return [
        f"unknown reference {expr!r}; use secret.NAME, db.NAME.FIELD, app.NAME.URL, secret() or randomInt()"
    ]


def validate_env(env: Union[Dict[str, str], Sequence[str]]) -> List[str]:
    """Problems across an env mapping or KEY=VALUE list."""
    entries: Iterable[str] = [f"{k}={v}" for k, v in env.items()] if isinstance(env, dict) else env
    problems: List[str] = []
    for entry in entries:
        key, _, value = str(entry).partition("=")
        refs = find_references(value)
        if not refs:
            continue
        for expr in refs:
            problems.extend(f"{key}: {p}" for p in validate_expression(expr))
        whole = len(refs) == 1 and REFERENCE_RE.fullmatch(value.strip()) is not None
        for expr in refs:
            if (expr.startswith("secret") or expr.startswith("db.")) and not whole:
                problems.append(
                    f"{key}: {expr!r} must be the entire value; secrets cannot be embedded in a string"
                )
    return problems
