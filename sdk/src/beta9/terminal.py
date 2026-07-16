import datetime
import os
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from os import PathLike
from typing import Any, Generator, List, Literal, Optional, Sequence, Tuple, Union

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
    _console.print(Text("✓ ", style="bold green").append(text, style="bold green"))


def warn(text: str) -> None:
    _console.print(Text("! ", style="bold yellow").append(text, style="bold yellow"))


def error(text: str, exit: bool = True, hint: Optional[str] = None) -> None:
    _console.print(Text("✗ ", style="bold red").append(text, style="bold red"))
    if hint:
        _console.print(Text(f"  hint: {hint}", style="dim"))

    if exit:
        reset_terminal()
        sys.exit(1)


def url(text: str) -> None:
    _console.print(Text(text, style="underline green"))


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


def update_progress(text: str) -> None:
    """Update the text of the currently active progress spinner, if any."""
    with _status_lock:
        if _current_status is not None:
            _current_status.update(text)


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


BRAND_COLOR = "#4CCACC"


def is_interactive() -> bool:
    """True when both stdin and stdout are TTYs and the terminal isn't dumb."""
    try:
        return sys.stdin.isatty() and sys.stdout.isatty() and os.environ.get("TERM", "") != "dumb"
    except Exception:
        return False


@dataclass
class SelectOption:
    label: str
    value: Any = None
    description: str = ""

    def __post_init__(self):
        if self.value is None:
            self.value = self.label


def select(
    title: str,
    options: Sequence[Union[str, SelectOption]],
    default_index: int = 0,
) -> Any:
    """
    Arrow-key single-select prompt. Returns the chosen option's value.

    Falls back to a numbered prompt when the terminal can't do raw-mode
    input (non-TTY, CI, dumb terminals). Raises KeyboardInterrupt on ctrl-c.
    """
    opts: List[SelectOption] = [
        o if isinstance(o, SelectOption) else SelectOption(label=o) for o in options
    ]
    if not opts:
        raise ValueError("select() requires at least one option")

    if is_interactive():
        try:
            return _select_arrow_keys(title, opts, default_index)
        except KeyboardInterrupt:
            raise
        except Exception:
            # Fall back to numbered input if raw mode fails. Collect any
            # coroutine prompt_toolkit abandoned mid-failure while warnings
            # are suppressed, so no RuntimeWarning leaks to the user.
            import gc
            import warnings

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                gc.collect()

    return _select_numbered(title, opts, default_index)


def _select_arrow_keys(title: str, opts: List[SelectOption], default_index: int) -> Any:
    from prompt_toolkit.application import Application
    from prompt_toolkit.formatted_text import FormattedText
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import HSplit, Layout, Window
    from prompt_toolkit.layout.controls import FormattedTextControl

    index = [max(0, min(default_index, len(opts) - 1))]

    def render() -> FormattedText:
        fragments = [("bold", title), ("fg:#808080", "   ↑/↓ move · enter select\n")]
        for i, opt in enumerate(opts):
            selected = i == index[0]
            pointer = "❯ " if selected else "  "
            style = f"bold {BRAND_COLOR}" if selected else ""
            fragments.append((style, f"{pointer}{opt.label}"))
            if opt.description:
                fragments.append(("fg:#808080", f"   {opt.description}"))
            fragments.append(("", "\n"))
        return FormattedText(fragments)

    bindings = KeyBindings()

    @bindings.add("up")
    @bindings.add("k")
    def _up(event):
        index[0] = (index[0] - 1) % len(opts)

    @bindings.add("down")
    @bindings.add("j")
    def _down(event):
        index[0] = (index[0] + 1) % len(opts)

    @bindings.add("enter")
    def _enter(event):
        event.app.exit(result=index[0])

    @bindings.add("c-c")
    @bindings.add("q")
    def _cancel(event):
        event.app.exit(exception=KeyboardInterrupt())

    control = FormattedTextControl(render, focusable=True, show_cursor=False)
    app = Application(
        layout=Layout(HSplit([Window(control, always_hide_cursor=True)])),
        key_bindings=bindings,
        full_screen=False,
        erase_when_done=True,
        mouse_support=False,
    )
    # Run in a dedicated thread with its own event loop: the SDK drives gRPC
    # through the main thread's asyncio loop (see aio.run_sync), which
    # Application.run() would otherwise try to reuse and crash on.
    chosen = app.run(in_thread=True)
    choice = opts[chosen]
    # Collapse any column padding in the label for the single-line echo.
    chosen_label = " ".join(choice.label.split())
    _console.print(
        Text("✓ ", style="bold green")
        .append(title, style="bold")
        .append("  ")
        .append(chosen_label, style=BRAND_COLOR)
    )
    return choice.value


def _select_numbered(title: str, opts: List[SelectOption], default_index: int) -> Any:
    _console.print(Text(title, style="bold"))
    for i, opt in enumerate(opts):
        line = Text(f"  {i + 1}) {opt.label}")
        if opt.description:
            line.append(f"  {opt.description}", style="dim")
        _console.print(line)

    default = default_index + 1
    while True:
        raw = prompt(text=f"Select an option [1-{len(opts)}]", default=default)
        try:
            picked = int(raw)
        except (TypeError, ValueError):
            warn("Enter a number.")
            continue
        if 1 <= picked <= len(opts):
            return opts[picked - 1].value
        warn(f"Enter a number between 1 and {len(opts)}.")


def confirm(text: str, default: bool = True) -> bool:
    """
    Single-keypress y/n confirmation. Falls back to line input when the
    terminal can't do raw-mode reads.
    """
    suffix = "[Y/n]" if default else "[y/N]"
    prompt_text = Text(text, style="bold").append(f" {suffix} ", style="dim")

    if is_interactive():
        try:
            import click

            _console.print(prompt_text, end="")
            char = click.getchar()
            _console.print(Text(char if char in "ynYN" else "", style=BRAND_COLOR))
            if char in ("\r", "\n"):
                return default
            if char in ("y", "Y"):
                return True
            if char in ("n", "N"):
                return False
            if char == "\x03":  # ctrl-c
                raise KeyboardInterrupt
            return default
        except KeyboardInterrupt:
            raise
        except Exception:
            pass

    raw = prompt(text=f"{text} {suffix}", default="y" if default else "n")
    return str(raw).strip().lower() in ("y", "yes", "true", "1")


@dataclass
class Step:
    """Handle yielded by StepTracker.step; set ok=False to mark the step failed."""

    ok: bool = True


class StepTracker:
    """
    Step narration: a ✓/✗ line with the elapsed time when each step
    completes. Steps print no start line of their own — the work inside a
    step announces itself (e.g. `=> Building image`), keeping one consistent
    gutter — and they hold no rich live display, so code inside a step can
    safely start its own spinners and progress bars.
    """

    def __init__(self, title: str = ""):
        self._started_at = time.monotonic()
        if title:
            header(title)

    @contextmanager
    def step(self, name: str, done_name: str = ""):
        start = time.monotonic()
        handle = Step()
        try:
            yield handle
        except BaseException:
            handle.ok = False
            self._finish(name, "", start, ok=False)
            raise
        self._finish(name, done_name, start, ok=handle.ok)

    def _finish(self, name: str, done_name: str, start: float, ok: bool) -> None:
        if ok:
            line = Text("✓ ", style="bold green").append(done_name or name)
        else:
            line = Text("✗ ", style="bold red").append(name)
        line.append(f" ({_format_elapsed(start)})", style="dim")
        _console.print(line)

    def note(self, text: str) -> None:
        _console.print(Text(f"  {text}", style="dim"))

    def elapsed(self) -> float:
        return time.monotonic() - self._started_at


def _format_elapsed(start: float) -> str:
    seconds = time.monotonic() - start
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m{secs:02d}s"


def done(text: str, start_time: Optional[float] = None) -> None:
    """End-of-command summary line, e.g. `✓ Deployed in 4.2s`."""
    line = Text("✓ ", style="bold green").append(text, style="bold")
    if start_time is not None:
        line.append(f" in {_format_elapsed(start_time)}", style="dim")
    _console.print(line)


@contextmanager
def redirect_terminal_to_buffer(buffer):
    global _console
    original_console = _console
    _console = Console(file=buffer, force_terminal=False)
    try:
        yield
    finally:
        _console = original_console
