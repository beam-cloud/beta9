import shutil
from types import ModuleType
from typing import Any

import click

from ..cli import task, volume
from ..cli.extraclick import CommandGroupCollection
from . import deployment

click.formatting.FORCED_WIDTH = shutil.get_terminal_size().columns


class CLI:
    """
    The CLI application.

    This is used to dynamically register commands. Commands are of type
    click.Group and are named either "common" or "management".
    """

    def __init__(self) -> None:
        self.group = click.Group()
        self.collection = CommandGroupCollection(sources=[self.group])

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.collection.main(*args, **kwargs)

    def register(self, module: ModuleType) -> None:
        if hasattr(module, "common"):
            self.collection.add_source(module.common)
        if hasattr(module, "management"):
            self.group.add_command(module.management)


cli = CLI()
cli.register(task)
cli.register(deployment)
cli.register(volume)
