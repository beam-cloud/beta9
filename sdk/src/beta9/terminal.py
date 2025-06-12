import datetime
import sys
import threading
from contextlib import contextmanager
from io import BytesIO
from os import PathLike
from typing import Any, Generator, Literal, Optional, Sequence, Tuple, Union

import rich
import rich.columns
import rich.control
import rich.status
import rich.traceback
from rich.console import Console
from rich.control import STRIP_CONTROL_CODES as _STRIP_CONTROL_CODES
from rich.markup import escape
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    ProgressColumn,
    Task,
    TextColumn,
    TimeRemainingColumn,
)
from rich.progress import open as _progress_open
from rich.text import Text

from . import env

# Fixes printing carriage returns and backspaces
# https://github.com/Textualize/rich/issues/3260
for i in (8, 13):
    if i in _STRIP_CONTROL_CODES:
        _STRIP_CONTROL_CODES.remove(i)
        rich.control.strip_control_codes.__defaults__ = ({c: None for c in _STRIP_CONTROL_CODES},)


if env.is_local():
    rich.traceback.install()

_console = Console()
_current_status = None
_status_lock = threading.Lock()
_status_count = 0


def header(text: str, subtext: str = "") -> None:
    header_text = f"[bold #4CCACC]=> {text}[/bold #4CCACC]"
    _console.print(header_text, subtext)


def print(*objects: Any, **kwargs: Any) -> None:
    _console.print(*objects, **kwargs)


def print_json(data: Any, **kwargs: Any) -> None:
    _console.print_json(data=data, indent=2, default=lambda o: str(o), **kwargs)


def prompt(
    *, text: str, default: Optional[Any] = None, markup: bool = False, password: bool = False
) -> Any:
    prompt_text = f"{text} [{default}]: " if default is not None else f"{text}: "
    user_input = _console.input(prompt_text, markup=markup, password=password).strip()
    return user_input if user_input else default


def detail(text: str, dim: bool = True, **kwargs) -> None:
    style = "dim" if dim else ""
    _console.print(Text(text, style=style), **kwargs)


def success(text: str) -> None:
    _console.print(Text(text, style="bold green"))


def warn(text: str) -> None:
    _console.print(Text(text, style="bold yellow"))


def error(text: str, exit: bool = True) -> None:
    _console.print(Text(text, style="bold red"))

    if exit:
        reset_terminal()
        sys.exit(1)


def url(text: str) -> None:
    _console.print(Text(text, style="underline blue"))


@contextmanager
def progress(task_name: str) -> Generator[rich.status.Status, None, None]:
    global _current_status, _status_count

    with _status_lock:
        if _current_status is None:
            _current_status = _console.status(task_name, spinner="dots", spinner_style="white")
            _current_status.start()
        _status_count += 1

    try:
        yield _current_status
    finally:
        with _status_lock:
            _status_count -= 1
            if _status_count == 0:
                _current_status.stop()
                _current_status = None


def progress_open(file: Union[str, PathLike, bytes], mode: str, **kwargs: Any) -> BytesIO:
    options = dict(
        complete_style="green",
        finished_style="slate_blue1",
        refresh_per_second=60,
        **kwargs,
    )

    if "description" in options and options["description"]:
        options["description"] = escape(f"[{options['description']}]")

    return _progress_open(file, mode, **options)  # type:ignore


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


def humanize_duration(delta: datetime.timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds < 60:
        return f"{total_seconds} seconds"
    elif total_seconds < 3600:
        minutes = total_seconds // 60
        return f"{minutes} minutes"
    elif total_seconds < 86400:
        hours = total_seconds // 3600
        return f"{hours} hours"
    else:
        days = total_seconds // 86400
        return f"{days} days"


def humanize_memory(m: float, base: Literal[2, 10] = 2) -> str:
    if base not in [2, 10]:
        raise ValueError("Base must be 2 (binary) or 10 (decimal)")

    factor = 1024 if base == 2 else 1000
    units = (
        ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
        if base == 2
        else ["B", "KB", "MB", "GB", "TB", "PB"]
    )
    index = 0
    while m >= factor and index < len(units) - 1:
        m /= factor
        index += 1
    return f"{m:.2f} {units[index]}"


def pluralize(seq: Sequence, suffix: str = "s") -> Tuple[int, str]:
    n = len(seq)
    return n, "s" if n != 1 else ""


def reset_terminal() -> None:
    _console.show_cursor()


def progress_description(name: str, max_width: Optional[int] = None):
    max_desc_width = max_width or len(name)
    if len(name) > max_desc_width:
        text = f"...{name[-(max_desc_width - 3) :]}"
    else:
        text = name.ljust(max_desc_width)

    return escape(f"[{text}]")


class CustomProgress(Progress):
    def add_task(self, description: Any, *args, **kwargs):
        return super().add_task(progress_description(str(description)), *args, **kwargs)


class AverageTransferSpeedColumn(ProgressColumn):
    """
    Renders the average data transfer speed over the entire lifetime of the transfer.
    """

    def render(self, task: Task) -> Text:
        task.fields.setdefault("average_mib_s", 0.0)

        # If the task hasn't started or there's no elapsed time yet, we can't compute an average
        if not task.started or task.elapsed == 0:
            return Text("?", style="progress.data.speed")

        if task.completed == task.total:
            return Text(f"{task.fields['average_mib_s']:.2f} MiB/s", style="progress.data.speed")

        # Calculate average speed in bytes per second
        average_bps = task.completed / (task.elapsed or 1)

        # Convert bytes per second to MiB/s (1 MiB = 1024 * 1024 bytes)
        task.fields["average_mib_s"] = average_bps / (1024**2)

        # Format to a reasonable precision (e.g., 2 decimal places)
        return Text(f"{task.fields['average_mib_s']:.2f} MiB/s", style="progress.data.speed")


def StyledProgress() -> CustomProgress:
    """
    Return a styled progress bar with custom columns.
    """
    return CustomProgress(
        *[
            TextColumn("[progress.description]{task.description}"),
            BarColumn(
                complete_style="green",
                finished_style="slate_blue1",
            ),
            DownloadColumn(binary_units=True),
            AverageTransferSpeedColumn(),
            TimeRemainingColumn(elapsed_when_finished=True),
        ],
        auto_refresh=True,
        refresh_per_second=60,
        disable=False,
    )


@contextmanager
def redirect_terminal_to_buffer(buffer):
    global _console
    original_console = _console
    _console = Console(file=buffer, force_terminal=False)
    try:
        yield
    finally:
        _console = original_console
