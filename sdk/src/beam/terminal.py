from contextlib import contextmanager

from rich.console import Console
from rich.text import Text


class Terminal:
    console = Console()

    @staticmethod
    def header(text):
        header_text = f"=> {text}"
        Terminal.console.print(Text(header_text, style="bold white"))

    @staticmethod
    def detail(text, dim=True):
        style = "dim" if dim else ""
        Terminal.console.print(Text(text, style=style))

    @staticmethod
    def success(text):
        Terminal.console.print(Text(text, style="bold green"))

    @staticmethod
    def error(text):
        Terminal.console.print(Text(text, style="bold red"))

    @staticmethod
    def url(text):
        Terminal.console.print(Text(text, style="underline blue"))

    @staticmethod
    @contextmanager
    def progress(task_name):
        with Terminal.console.status(task_name, spinner="dots", spinner_style="white"):
            yield
