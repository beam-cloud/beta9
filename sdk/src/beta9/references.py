"""
`${{...}}` reference grammar: validation and autocomplete. The gateway resolves
(pkg/gateway/services/references.go).
"""

import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

REFERENCE_RE = re.compile(r"\$\{\{\s*([^}]*?)\s*\}\}")

DB_FIELDS = ("DATABASE_URL", "REDIS_URL", "URL", "USERNAME", "PASSWORD", "DATABASE", "HOST", "PORT")
FUNCTIONS = ("secret(", "randomInt(")
KINDS = ("secret.", "db.", "app.", "secret(", "randomInt(")

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


def complete(
    prefix: str,
    secrets: Sequence[str] = (),
    databases: Sequence[str] = (),
    apps: Sequence[str] = (),
) -> List[str]:
    """Autocomplete candidates for a partial expression (text after `${{`)."""
    p = prefix.strip()
    if p.startswith("secret."):
        return [f"secret.{s}" for s in secrets if s.startswith(p[len("secret.") :])]
    if p.startswith("db."):
        rest = p[len("db.") :]
        name, dot, field = rest.partition(".")
        if dot:
            return [f"db.{name}.{f}" for f in DB_FIELDS if f.startswith(field.upper())]
        return [f"db.{d}." for d in databases if d.startswith(name)]
    if p.startswith("app."):
        rest = p[len("app.") :]
        name, dot, _ = rest.partition(".")
        if dot:
            return [f"app.{name}.URL"]
        return [f"app.{a}.URL" for a in apps if a.startswith(name)]
    return [k for k in KINDS if k.startswith(p)]


DB_ENV_PREFIX = {"postgres": "PG", "redis": "REDIS", "mysql": "MYSQL", "mongo": "MONGO"}
DB_URL_NAME = {
    "postgres": "DATABASE_URL",
    "redis": "REDIS_URL",
    "mysql": "MYSQL_URL",
    "mongo": "MONGO_URL",
}


def connection_references(
    source: str, database_kind: Optional[str], env_name: str = ""
) -> List[Tuple[str, str]]:
    """References wiring an app to `source`: a database's URL and parts, or an app's URL. Mirrors utils/connections.ts."""
    if not database_kind:
        key = re.sub(r"[^A-Z0-9]+", "_", source.upper()).strip("_")
        return [(env_name or f"{key}_URL", f"${{{{app.{source}.URL}}}}")]

    def ref(field: str) -> str:
        return f"${{{{db.{source}.{field}}}}}"

    url_name = DB_URL_NAME.get(database_kind, "DATABASE_URL")
    if env_name:
        return [(env_name, ref(url_name))]
    prefix = DB_ENV_PREFIX.get(database_kind, database_kind.upper())
    refs = [
        (url_name, ref(url_name)),
        (f"{prefix}HOST", ref("HOST")),
        (f"{prefix}PORT", ref("PORT")),
        (f"{prefix}USER", ref("USERNAME")),
        (f"{prefix}PASSWORD", ref("PASSWORD")),
    ]
    if database_kind != "redis":
        refs.append((f"{prefix}DATABASE", ref("DATABASE")))
    return refs
