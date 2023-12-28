import sys
from contextlib import contextmanager

from rich.console import Console
from rich.text import Text

_console = Console()


def header(text):
    header_text = f"=> {text}"
    _console.print(Text(header_text, style="bold white"))


def prompt(text):
    prompt_text = f"=> {text}"
    _console.print(Text(prompt_text, style="bold blue"), end="")


def detail(text, dim=True):
    style = "dim" if dim else ""
    _console.print(Text(text, style=style))


def success(text):
    _console.print(Text(text, style="bold green"))


def error(text):
    _console.print(Text(text, style="bold red"))
    sys.exit(1)


def url(text):
    _console.print(Text(text, style="underline blue"))


@contextmanager
def progress(task_name):
    with _console.status(task_name, spinner="dots", spinner_style="white"):
        yield
