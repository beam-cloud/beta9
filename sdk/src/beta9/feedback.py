"""
Feedback about the platform itself, from users and the coding agents working for them: the
opt-out, and the last error the CLI reported, which `feedback --last-error` attaches.
"""

import json
import os
import platform
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
from click.core import ParameterSource

DISABLE_VARIABLE = "BETA9_NO_FEEDBACK"
DISABLED_MESSAGE = f"Feedback is turned off: {DISABLE_VARIABLE} is set."
KINDS = ("bug", "error_message", "missing_capability", "docs", "performance", "praise", "other")
LAST_ERROR_MAX_AGE = 3600

_recording = False
_OPTION = re.compile(r"--?[A-Za-z][A-Za-z0-9-]*")
_WORD = re.compile(r"[a-z][a-z0-9-]*")


def feedback_disabled() -> bool:
    return os.getenv(DISABLE_VARIABLE, "").lower() in ("1", "true", "yes")


def record_errors(on: bool) -> None:
    """Keep the error that ends a CLI run for `feedback --last-error`. Off outside the CLI."""
    global _recording
    _recording = on


def last_error_path() -> Path:
    from .config import get_settings

    return Path(get_settings().config_path).expanduser().parent / "last_error.json"


def command_line(argv: List[str]) -> str:
    """The command and the names of the options it was given. Values can hold secrets, so
    none are kept. `argv` is read only once Click has finished with the command."""
    context = click.get_current_context(silent=True)
    if context is not None:
        chain = []
        while context is not None:
            chain.insert(0, context)
            context = context.parent
        options = [
            max(param.opts, key=len)
            for ctx in chain
            for param in ctx.command.params
            if isinstance(param, click.Option)
            and ctx.get_parameter_source(param.name) == ParameterSource.COMMANDLINE
        ]
        return " ".join([chain[-1].command_path, *options])

    from .terminal import cli_name

    words = [cli_name()]
    for arg in argv[1:]:
        if not _WORD.fullmatch(arg):
            break
        words.append(arg)
    options = [arg.split("=", 1)[0] for arg in argv[1:] if arg.startswith("-")]
    words += [option for option in dict.fromkeys(options) if _OPTION.fullmatch(option)]
    return " ".join(words)


def record_error(title: str, details: str, code: str) -> None:
    if not _recording:
        return
    record = {
        "command": command_line(sys.argv),
        "code": code,
        "error": title,
        "details": details[:2000],
        "at": time.time(),
    }
    try:
        path = last_error_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as file:
            json.dump(record, file)
    except Exception:
        # Failing to remember an error must not hide the error itself.
        pass


def last_error(max_age: float = LAST_ERROR_MAX_AGE) -> Optional[Dict[str, Any]]:
    try:
        record = json.loads(last_error_path().read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(record, dict) or time.time() - float(record.get("at") or 0) > max_age:
        return None
    return record


def nudge(code: str) -> Optional[str]:
    """For an INTERNAL error, a failure on the platform's side: how an agent reports it."""
    if code != "INTERNAL" or not _recording or feedback_disabled():
        return None
    from .config import get_settings
    from .terminal import cli_name

    return (
        f"This failed inside {get_settings().name}. Report it once, then continue: "
        f'{cli_name()} feedback --last-error --kind bug -m "<one sentence>"'
    )


def environment() -> str:
    return f"{platform.system()} {platform.release()} {platform.machine()}; python {platform.python_version()}"
