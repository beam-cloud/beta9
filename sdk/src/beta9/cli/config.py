import click
from rich.table import Column, Table, box

from .. import terminal
from ..config import (
    DEFAULT_CONTEXT_NAME,
    ConfigContext,
    load_config,
    prompt_for_config_context,
    save_config,
)
from .extraclick import ClickManagementGroup


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
def list_contexts():
    contexts = load_config()

    table = Table(
        Column("Name"),
        Column("Host"),
        Column("Port"),
        box=box.SIMPLE,
    )

    for name, context in contexts.items():
        table.add_row(
            name,
            context.gateway_host,
            str(context.gateway_port),
        )

    terminal.print(table)


@management.command(
    name="delete",
    help="Delete a context.",
)
@click.option("--name", "-n", type=click.STRING, required=True)
def delete_context(name: str):
    contexts = load_config()

    if name == DEFAULT_CONTEXT_NAME:
        contexts[name] = ConfigContext()

    elif name in contexts:
        del contexts[name]

    save_config(contexts)
    terminal.print("Deleted context")


@management.command(
    name="create",
    help="Create a new context.",
)
@click.option("--name", type=click.STRING, required=True)
@click.option("--token", type=click.STRING)
@click.option("--gateway-host", type=click.STRING)
@click.option("--gateway-port", type=click.INT)
def create_context(**kwargs):
    prompt_for_config_context(**kwargs)


@management.command(
    name="select",
    help="Set a context as the default. This will overwrite the default context.",
)
@click.option("--name", "-n", type=click.STRING, required=True)
def select_context(name: str):
    contexts = load_config()

    if name in contexts:
        contexts[DEFAULT_CONTEXT_NAME] = contexts[name]

    save_config(contexts)
