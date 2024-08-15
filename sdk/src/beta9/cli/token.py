import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    CreateTokenRequest,
    CreateTokenResponse,
    DeleteTokenRequest,
    DeleteTokenResponse,
    ListTokensRequest,
    ListTokensResponse,
    ToggleTokenRequest,
    ToggleTokenResponse,
)
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="token",
    help="Manage tokens.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="List all tokens.",
    epilog="""
    Examples:

      # List tokens
      {cli_name} token list

      # List tokens and output in JSON format
      {cli_name} token list --format json
      \b
    """,
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@extraclick.pass_service_client
def list_tokens(
    service: ServiceClient,
    format: str,
):
    res: ListTokensResponse
    res = service.gateway.list_tokens(ListTokensRequest())

    if not res.ok:
        terminal.error(res.err_msg)
        return

    if format == "json":
        tokens = [t.to_dict(casing=Casing.SNAKE) for t in res.tokens]  # type:ignore
        terminal.print_json(tokens)
        return

    table = Table(
        Column("ID"),
        Column("Active"),
        Column("Reusable"),
        Column("Token Type"),
        Column("Created At"),
        Column("Updated At"),
        Column("Workspace ID"),
        box=box.SIMPLE,
    )

    for token in res.tokens:
        table.add_row(
            token.token_id,
            "Yes" if token.active else "No",
            "Yes" if token.reusable else "No",
            token.token_type,
            terminal.humanize_date(token.created_at),
            terminal.humanize_date(token.updated_at),
            str(token.workspace_id) if token.workspace_id else "N/A",
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.tokens)} items")
    terminal.print(table)


@management.command(
    name="create",
    help="Create a new token.",
    epilog="""
    Examples:

      # Create a new token. 
      {cli_name} token create

      \b
    """,
)
@extraclick.pass_service_client
def create_token(
    service: ServiceClient,
):
    res: CreateTokenResponse
    res = service.gateway.create_token(CreateTokenRequest())

    if not res.ok:
        terminal.error(res.err_msg)
    else:
        terminal.success("Token created successfully.")
        terminal.print(res.token.to_dict(casing=Casing.SNAKE))


@management.command(
    name="delete",
    help="Delete a token.",
    epilog="""
    Examples:

      # Delete a token. 
      {cli_name} token delete 49554ce9-5861-4834-899b-ec301e5d8534

      \b
    """,
)
@extraclick.pass_service_client
@click.argument("token_id", type=click.STRING)
def delete_token(
    service: ServiceClient,
    token_id: str,
):
    res: DeleteTokenResponse
    res = service.gateway.delete_token(DeleteTokenRequest(token_id=token_id))

    if not res.ok:
        terminal.error(res.err_msg)
    else:
        terminal.success("Deleted token.")


@management.command(
    name="toggle",
    help="Toggle a token's active status.",
    epilog="""
    Examples:

      # Toggle a token
      {cli_name} token toggle 49554ce9-5861-4834-899b-ec301e5d8534

      \b
    """,
)
@click.argument("token_id", type=click.STRING)
@extraclick.pass_service_client
def toggle_token(
    service: ServiceClient,
    token_id: str,
):
    res: ToggleTokenResponse
    res = service.gateway.toggle_token(ToggleTokenRequest(token_id=token_id))

    if not res.ok:
        terminal.error(res.err_msg)
    else:
        token = res.token.to_dict(casing=Casing.SNAKE)
        if "active" not in token:
            token["active"] = False
        terminal.success(
            f"Token {res.token.token_id} {'enabled' if  token['active'] else 'disabled'}."
        )
        terminal.print(token)
