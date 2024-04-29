import click

from ..config import prompt_for_config_context
from .extraclick import ClickManagementGroup


@click.group(
    name="config",
    help="Manage configuration contexts.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(name="list")
def list_contexts():
    pass


@management.command(name="create")
@click.option("--name", type=click.STRING, required=True)
@click.option("--token", type=click.STRING)
@click.option("--gateway-host", type=click.STRING)
@click.option("--gateway-port", type=click.INT)
@click.option("--gateway-http-port", type=click.INT)
def create_context(**kwargs):
    prompt_for_config_context(**kwargs)
