import shutil
from types import ModuleType
from typing import Any, Optional

import click

from .. import terminal
from ..channel import get_channel
from ..config import is_config_empty
from . import config, deployment, task, volume
from .extraclick import ClickCommonGroup, CommandGroupCollection

click.formatting.FORCED_WIDTH = shutil.get_terminal_size().columns

# Can be overwritten by doing `load_cli(context_settings={})`
CLICK_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"],
    show_default=True,
)


class CLI:
    """
    The CLI application.

    This is used to dynamically register commands. Commands are of type
    click.Group and are named either "common" or "management".
    """

    def __init__(self, context_settings: Optional[dict] = None) -> None:
        if context_settings is None:
            context_settings = CLICK_CONTEXT_SETTINGS

        self.management_group = ClickCommonGroup()
        self.common_group = CommandGroupCollection(
            sources=[self.management_group],
            context_settings=context_settings,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.common_group.main(*args, **kwargs)

    def register(self, module: ModuleType) -> None:
        if hasattr(module, "common"):
            self.common_group.add_command(module.common)
        if hasattr(module, "management"):
            self.management_group.add_command(module.management)


def load_cli(**kwargs: Any) -> CLI:
    if is_config_empty():
        terminal.header("Welcome to Beta9! Let's get started 📡")
        get_channel().close()

    cli = CLI(**kwargs)
    cli.register(task)
    cli.register(deployment)
    cli.register(volume)
    cli.register(config)

    return cli


if __name__ == "beta9.cli.main":
    cli = load_cli()
