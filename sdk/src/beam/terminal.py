import sys
from contextlib import contextmanager

from rich.console import Console
from rich.text import Text

_console = Console()


def header(text: str) -> None:
    header_text = f"=> {text}"
    _console.print(Text(header_text, style="bold white"))


def prompt(*, text: str, default: str) -> str:
    prompt_text = f"=> {text} [{default}]: "
    _console.print(Text(prompt_text, style="bold blue"), end="")
    user_input = input().strip()
    return user_input if user_input else default


def detail(text: str, dim: bool = True) -> None:
    style = "dim" if dim else ""
    _console.print(Text(text, style=style))


def success(text: str) -> None:
    _console.print(Text(text, style="bold green"))


def error(text: str) -> None:
    _console.print(Text(text, style="bold red"))
    sys.exit(1)


def url(text: str) -> None:
    _console.print(Text(text, style="underline blue"))


@contextmanager
def progress(task_name: str):
    with _console.status(task_name, spinner="dots", spinner_style="white"):
        yield
