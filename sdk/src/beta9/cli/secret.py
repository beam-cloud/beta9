import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.secret import (
    CreateSecretRequest,
    CreateSecretResponse,
    DeleteSecretRequest,
    DeleteSecretResponse,
    GetSecretRequest,
    GetSecretResponse,
    ListSecretsRequest,
    ListSecretsResponse,
    UpdateSecretRequest,
    UpdateSecretResponse,
)
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="secret",
    help="Manage secrets",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="List all secrets.",
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@extraclick.pass_service_client
def list_secrets(
    service: ServiceClient,
    format: str,
):
    res: ListSecretsResponse
    res = service.secret.list_secrets(ListSecretsRequest())

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        secrets = [d.to_dict(casing=Casing.SNAKE) for d in res.secrets]  # type:ignore
        terminal.print_json(secrets)
        return

    table = Table(
        Column("Name"),
        Column("Last Updated"),
        Column("Created"),
        box=box.SIMPLE,
    )

    for secret in res.secrets:
        table.add_row(
            secret.name,
            terminal.humanize_date(secret.created_at),
            terminal.humanize_date(secret.updated_at),
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.secrets)} items")
    terminal.print(table)


@management.command(
    name="create",
    help="Create new secret.",
)
@click.argument("name")
@click.argument("value")
@extraclick.pass_service_client
def create_secret(service: ServiceClient, name: str, value: str):
    res: CreateSecretResponse
    res = service.secret.create_secret(CreateSecretRequest(name=name, value=value))
    if res.ok:
        terminal.header(f"Created secret with name: '{res.name}'")
    else:
        terminal.error(f"Error: {res.err_msg}")


@management.command(
    name="modify",
    help="Modify existing secret.",
)
@click.argument("name")
@click.argument("value")
@extraclick.pass_service_client
def modify_secret(service: ServiceClient, name: str, value: str):
    res: UpdateSecretResponse
    res = service.secret.update_secret(UpdateSecretRequest(name=name, value=value))
    if res.ok:
        terminal.header(f"Modified secret '{name}'")
    else:
        terminal.error(f"Error: {res.err_msg}")


@management.command(
    name="delete",
    help="Delete secret.",
)
@click.argument("name")
@extraclick.pass_service_client
def delete_secret(service: ServiceClient, name: str):
    res: DeleteSecretResponse
    res = service.secret.delete_secret(DeleteSecretRequest(name=name))
    if res.ok:
        terminal.header(f"Deleted secret '{name}'")
    else:
        terminal.error(f"Error: {res.err_msg}")


@management.command(
    name="show",
    help="Show secret.",
)
@click.argument("name")
@extraclick.pass_service_client
def show_secret(service: ServiceClient, name: str):
    res: GetSecretResponse

    res = service.secret.get_secret(GetSecretRequest(name=name))

    if res.ok:
        terminal.header(f"Secret '{name}': {res.secret.value}")
    else:
        terminal.error(f"Error: {res.err_msg}")
