from pathlib import Path
from typing import Any

import click
from rich.style import Style
from rich.table import Column, Table, box

from .. import terminal
from ..config import (
    DEFAULT_CONTEXT_NAME,
    ConfigContext,
    get_settings,
    load_config,
    prompt_for_config_context,
    save_config,
)
from .extraclick import ClickManagementGroup


def get_setting_callback(ctx: click.Context, param: click.Parameter, value: Any):
    return getattr(get_settings(), param.name) if not value else value


@click.group(
    name="config",
    help="Manage configuration contexts.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="Lists available contexts.",
)
@click.option(
    "--show-token",
    is_flag=True,
    required=False,
    default=False,
    help="Display token.",
)
@click.option(
    "--config-path",
    type=click.Path(),
    required=False,
    callback=get_setting_callback,
    help="Path to a config file.",
)
def list_contexts(show_token: bool, config_path: Path):
    contexts = load_config(config_path)

    table = Table(
        Column("Name"),
        Column("Host"),
        Column("Port"),
        Column("Token"),
        box=box.SIMPLE,
    )

    for name, context in contexts.items():
        # Style default context
        style = Style(bold=True) if name == DEFAULT_CONTEXT_NAME else Style()
        table.add_row(
            name,
            context.gateway_host,
            str(context.gateway_port),
            context.token
            if show_token
            else str(context.token)[0:6] + "..."
            if context.token and len(context.token) > 6
            else context.token,
            style=style,
        )

    terminal.print(table)


@management.command(
    name="delete",
    help="Delete a context.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=True,
)
@click.option(
    "--config-path",
    type=click.Path(),
    required=False,
    callback=get_setting_callback,
    help="Path to a config file.",
)
@click.confirmation_option("--force")
def delete_context(name: str, config_path: Path):
    contexts = load_config(config_path)

    if name == DEFAULT_CONTEXT_NAME:
        contexts[name] = ConfigContext()

    elif name in contexts:
        del contexts[name]

    save_config(contexts=contexts, path=config_path)
    terminal.success(f"Deleted context {name}.")


@management.command(
    name="create",
    help="Create a new context.",
)
@click.argument("name", type=click.STRING, required=True)
@click.option("--token", type=click.STRING)
@click.option(
    "--gateway-host",
    type=click.STRING,
    callback=get_setting_callback,
)
@click.option(
    "--gateway-port",
    type=click.INT,
    callback=get_setting_callback,
)
@click.option(
    "--config-path",
    type=click.Path(),
    required=False,
    callback=get_setting_callback,
    help="Path to a config file.",
)
def create_context(config_path: Path, **kwargs):
    contexts = load_config(config_path)

    if name := kwargs.get("name"):
        if name in contexts:
            text = f"Context '{name}' already exists. Overwrite?"
            if terminal.prompt(text=text, default="n").lower() in ["n", "no"]:
                return

    # Prompt user for context settings
    name, context = prompt_for_config_context(**kwargs)

    # Save context to config
    contexts[name] = context
    save_config(contexts=contexts, path=config_path)

    terminal.success("Added new context 🎉!")


@management.command(
    name="select",
    help="Set a default context. This will overwrite the current default context.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=True,
)
@click.option(
    "--config-path",
    type=click.Path(),
    required=False,
    callback=get_setting_callback,
    help="Path to a config file.",
)
def select_context(name: str, config_path: Path):
    contexts = load_config(config_path)

    if name not in contexts:
        terminal.error(f"Context '{name}' does not exist.")

    contexts[DEFAULT_CONTEXT_NAME] = contexts[name]
    save_config(contexts=contexts, path=config_path)

    terminal.success(f"Default context updated with '{name}'.")
