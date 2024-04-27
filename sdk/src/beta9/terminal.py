import datetime
import sys
from contextlib import contextmanager
from typing import Any, Sequence, Tuple

from rich.console import Console
from rich.markup import escape
from rich.progress import open as _progress_open
from rich.text import Text

_console = Console()


def header(text: str, subtext: str = "") -> None:
    header_text = f"[bold white]=> {text}[/bold white]"
    _console.print(header_text, subtext)


def print(*objects: Any, **kwargs: Any) -> None:
    _console.print(*objects, **kwargs)


def print_json(data: Any, **kwargs: Any) -> None:
    _console.print_json(data=data, indent=2, default=lambda o: str(o), **kwargs)


def prompt(*, text: str, default: Any) -> Any:
    prompt_text = f"=> {text} [{default}]: "
    _console.print(Text(prompt_text, style="bold blue"), end="")
    user_input = input().strip()
    return user_input if user_input else default


def detail(text: str, dim: bool = True, **kwargs) -> None:
    style = "dim" if dim else ""
    _console.print(Text(text, style=style), **kwargs)


def success(text: str) -> None:
    _console.print(Text(text, style="bold green"))


def warn(text: str) -> None:
    _console.print(Text(text, style="bold yellow"))


def error(text: str) -> None:
    _console.print(Text(text, style="bold red"))
    sys.exit(1)


def url(text: str) -> None:
    _console.print(Text(text, style="underline blue"))


@contextmanager
def progress(task_name: str):
    with _console.status(task_name, spinner="dots", spinner_style="white"):
        yield


def progress_open(file, mode, **kwargs):
    options = dict(
        complete_style="green",
        finished_style="slate_blue1",
        refresh_per_second=60,
        **kwargs,
    )

    if "description" in options:
        options["description"] = escape(f"[{options['description']}]")

    return _progress_open(file, mode, **options)


def humanize_date(d: datetime.datetime) -> str:
    # Check if datetime is "zero" time
    if d == datetime.datetime(1, 1, 1, tzinfo=datetime.timezone.utc):
        return ""

    # Generate relative datetime
    diff = datetime.datetime.now(datetime.timezone.utc) - d
    s = diff.seconds
    if diff.days > 7 or diff.days < 0:
        return d.strftime("%b %d %Y")
    elif diff.days == 1:
        return "1 day ago"
    elif diff.days > 1:
        return f"{diff.days} days ago"
    elif s <= 1:
        return "just now"
    elif s < 60:
        return f"{s} seconds ago"
    elif s < 120:
        return "1 minute ago"
    elif s < 3600:
        return f"{s // 60} minutes ago"
    elif s < 7200:
        return "1 hour ago"
    else:
        return f"{s // 3600} hours ago"


def humanize_memory(m: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    index = 0
    while m >= 1024 and index < len(units) - 1:
        m /= 1024
        index += 1
    return f"{m:.2f} {units[index]}"


def pluralize(seq: Sequence, suffix: str = "s") -> Tuple[int, str]:
    n = len(seq)
    return n, "s" if n != 1 else ""
