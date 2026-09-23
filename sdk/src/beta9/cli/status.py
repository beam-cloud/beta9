from typing import Any, Dict

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..clients.gateway import ListContainersRequest, ListDeploymentsRequest
from . import extraclick
from .extraclick import ClickCommonGroup, selected_context


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


def _workspace_summary(service: ServiceClient) -> Dict[str, Any]:
    context = service._config
    return {
        "context": selected_context(),
        "workspace_id": service.http.workspace_id,
        "gateway_grpc": f"{context.gateway_host}:{context.gateway_port}",
        "gateway_http": service.http.base_url,
        "token": f"{(context.token or '')[:6]}…" if context.token else "",
    }


@common.command(name="whoami", help="Show the active context, workspace and gateway.")
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
@extraclick.pass_service_client
def whoami(service: ServiceClient, format: str):
    summary = _workspace_summary(service)
    if format == "json" or terminal.json_output():
        terminal.print_json(summary)
        return
    terminal.resource("Workspace", summary)


@common.command(
    name="status",
    help="One-shot snapshot of the workspace: latest deployments and running containers.",
)
@click.option("--limit", type=click.IntRange(min=1), default=10, show_default=True)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
@extraclick.pass_service_client
def status(service: ServiceClient, limit: int, format: str):
    summary = _workspace_summary(service)

    deployments_res = service.gateway.list_deployments(ListDeploymentsRequest(limit=limit))
    if not deployments_res.ok:
        terminal.error(deployments_res.err_msg, code="ERROR")
    deployments = [d.to_dict(casing=Casing.SNAKE) for d in deployments_res.deployments]  # type: ignore

    containers_res = service.gateway.list_containers(ListContainersRequest())
    containers = (
        [c.to_dict(casing=Casing.SNAKE) for c in containers_res.containers]  # type: ignore
        if containers_res.ok
        else []
    )

    if format == "json" or terminal.json_output():
        terminal.print_json({**summary, "deployments": deployments, "containers": containers})
        return

    terminal.resource("Workspace", summary)

    table = Table(
        Column("Name"),
        Column("Type"),
        Column("Version", justify="right"),
        Column("Active"),
        Column("ID"),
        box=box.SIMPLE,
        title="Latest deployments",
    )
    for d in deployments:
        table.add_row(
            d.get("name", ""),
            d.get("stub_type", ""),
            str(d.get("version", "")),
            "yes" if d.get("active") else "no",
            d.get("id", ""),
        )
    terminal.print(table)
    terminal.detail(f"{len(containers)} running container(s)")
