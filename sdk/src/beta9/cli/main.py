import shutil
from types import ModuleType
from typing import Any, Optional

import click

from ..channel import prompt_first_auth
from ..config import SDKSettings, is_config_empty, set_settings
from . import config, deployment, serve, task, volume
from .extraclick import CLICK_CONTEXT_SETTINGS, ClickCommonGroup, CommandGroupCollection

click.formatting.FORCED_WIDTH = shutil.get_terminal_size().columns


class CLI:
    """
    The CLI application.

    This is used to dynamically register commands. Commands are of type
    click.Group and are named either "common" or "management".
    """

    def __init__(
        self,
        settings: Optional[SDKSettings] = None,
        context_settings: Optional[dict] = None,
    ) -> None:
        self.settings = SDKSettings() if settings is None else settings
        set_settings(self.settings)

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

    def check_config(self) -> None:
        if is_config_empty(self.settings.config_path):
            prompt_first_auth(self.settings)


def load_cli(**kwargs: Any) -> CLI:
    cli = CLI(**kwargs)
    cli.register(task)
    cli.register(deployment)
    cli.register(volume)
    cli.register(config)
    cli.register(serve)

    cli.check_config()

    return cli


def start():
    """Used as entrypoint in Poetry"""
    cli = load_cli()
    cli()
