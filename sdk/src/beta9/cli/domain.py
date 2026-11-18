from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote

import click
from rich.table import Column, Table, box

from .. import terminal
from ..channel import GatewayHTTPError, ServiceClient
from . import extraclick
from .extraclick import ClickManagementGroup


@click.group(
    name="domain",
    help="Manage custom domains.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _api(
    service: ServiceClient, method: str, path: str = "", format: Optional[str] = None, **kwargs
) -> Any:
    try:
        return service.http.json(method, f"/api/v1/domain/{{ws}}{path}", **kwargs)
    except GatewayHTTPError as exc:
        if terminal.json_output(format) and not terminal.json_output():
            terminal.print_json({"error": exc.message, "code": exc.code})
            raise click.exceptions.Exit(1)
        terminal.error(exc.message, code=exc.code)


def _format_timestamp(raw: Optional[str]) -> str:
    if not raw:
        return "-"
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return terminal.humanize_date(parsed)
    except ValueError:
        return str(raw)


def _format_status(status: str) -> str:
    normalized = (status or "").lower()
    if normalized == "active":
        return "[green]Active[/green]"
    if normalized == "pending":
        return "[yellow]Pending[/yellow]"
    if normalized == "failed":
        return "[red]Failed[/red]"
    return status or "-"


def _print_domain_detail(item: Dict[str, Any]) -> None:
    fields = {
        "ID": item.get("id", ""),
        "Domain": item.get("domain", ""),
        "URL": item.get("url", ""),
        "Deployment": item.get("deployment_name", ""),
        "Stub Type": item.get("stub_type", ""),
        "Version": f"v{item['version']}" if item.get("version") else "latest",
        "Port": str(item["port"]) if item.get("port") else "",
        "Status": item.get("status", ""),
        "CNAME Target": item.get("cname_target", ""),
        "TXT Name": item.get("txt_record_name", ""),
        "TXT Value": item.get("verification_token", ""),
    }
    terminal.resource("Custom Domain", fields)

    if item.get("status") != "active" and item.get("verification_token"):
        if item.get("cname_target"):
            terminal.detail(
                "Point a CNAME record to the CNAME Target or add the TXT record above, then run "
                f"`{extraclick.command_hint()} domain verify {item.get('domain', '')}`."
            )
        else:
            terminal.detail(
                "Add the TXT record above, then run "
                f"`{extraclick.command_hint()} domain verify {item.get('domain', '')}`."
            )


def _print_domain_table(items: Iterable[Dict[str, Any]]) -> None:
    domains = list(items)
    if not domains:
        terminal.resource(
            "No custom domains",
            {
                "Add": f"{extraclick.command_hint()} domain add api.example.com --deployment my-app",
            },
        )
        return

    table = Table(
        Column("Domain"),
        Column("Deployment"),
        Column("Status"),
        Column("Version", justify="right"),
        Column("Port", justify="right"),
        Column("URL"),
        Column("Created"),
        box=box.SIMPLE,
        header_style="bold cyan",
    )

    for item in domains:
        table.add_row(
            item.get("domain", ""),
            item.get("deployment_name", ""),
            _format_status(item.get("status", "")),
            f"v{item['version']}" if item.get("version") else "latest",
            str(item["port"]) if item.get("port") else "-",
            item.get("url", "") or "-",
            _format_timestamp(item.get("created_at")),
        )

    table.add_section()
    table.add_row(f"[bold]{len(domains)} domains")
    terminal.print(table)


@management.command(
    name="add",
    help="Bind a custom domain to a deployment.",
)
@click.argument("domain")
@click.option(
    "--deployment",
    "-d",
    "deployment_name",
    type=click.STRING,
    default="",
    help="Deployment name to route traffic to.",
)
@click.option(
    "--deployment-id",
    type=click.STRING,
    default="",
    help="Specific deployment ID to bind (pins deployment name/version).",
)
@click.option(
    "--stub-type",
    type=click.STRING,
    default="",
    help="Optional stub type (endpoint, asgi, pod).",
)
@click.option(
    "--version",
    type=click.IntRange(min=0, max=2147483647),
    default=0,
    help="Specific deployment version (0 routes to latest).",
)
@click.option(
    "--port",
    type=click.IntRange(min=0, max=65535),
    default=0,
    help="Container port to forward to (primarily for Pods).",
)
@extraclick.format_option
@extraclick.pass_service_client
def add_domain(
    service: ServiceClient,
    domain: str,
    deployment_name: str,
    deployment_id: str,
    stub_type: str,
    version: int,
    port: int,
    format: str,
):
    if not deployment_name and not deployment_id:
        if terminal.json_output(format) and not terminal.json_output():
            terminal.print_json(
                {"error": "Specify --deployment or --deployment-id.", "code": "INVALID_ARGS"}
            )
            raise click.exceptions.Exit(1)
        terminal.error("Specify --deployment or --deployment-id.", code="INVALID_ARGS")

    item = _api(
        service,
        "POST",
        "",
        format=format,
        json={
            "domain": domain,
            "deployment_name": deployment_name,
            "deployment_id": deployment_id,
            "stub_type": stub_type,
            "version": version,
            "port": port,
        },
    )

    if terminal.json_output(format):
        terminal.print_json(item)
        return

    terminal.success(f"Added custom domain: {item.get('domain', domain)}")
    _print_domain_detail(item)


@management.command(
    name="list",
    help="List custom domains in this workspace.",
)
@click.option(
    "--deployment",
    "-d",
    "deployment_name",
    type=click.STRING,
    default="",
    help="Filter custom domains by deployment name.",
)
@extraclick.format_option
@extraclick.pass_service_client
def list_domains(service: ServiceClient, deployment_name: str, format: str):
    params = {"deployment_name": deployment_name} if deployment_name else {}
    items = _api(service, "GET", "", format=format, params=params) or []

    if terminal.json_output(format):
        terminal.print_json(items)
        return

    _print_domain_table(items)


@management.command(
    name="get",
    help="Show details and DNS records for a custom domain.",
)
@click.argument("domain_or_id")
@extraclick.format_option
@extraclick.pass_service_client
def get_domain(service: ServiceClient, domain_or_id: str, format: str):
    item = _api(service, "GET", f"/{quote(domain_or_id, safe='')}", format=format)

    if terminal.json_output(format):
        terminal.print_json(item)
        return

    _print_domain_detail(item)


@management.command(
    name="verify",
    help="Verify DNS ownership for a custom domain and mark it active.",
)
@click.argument("domain_or_id")
@extraclick.format_option
@extraclick.pass_service_client
def verify_domain(service: ServiceClient, domain_or_id: str, format: str):
    item = _api(
        service,
        "POST",
        f"/{quote(domain_or_id, safe='')}/verify",
        format=format,
    )

    if terminal.json_output(format):
        terminal.print_json(item)
        return

    terminal.success(f"Verified custom domain: {item.get('domain', domain_or_id)}")
    _print_domain_detail(item)


@management.command(
    name="remove",
    help="Remove a custom domain.",
)
@click.argument("domain_or_id")
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@extraclick.format_option
@extraclick.pass_service_client
def remove_domain(service: ServiceClient, domain_or_id: str, yes: bool, format: str):
    if not yes:
        if terminal.json_output(format):
            if terminal.json_output():
                terminal.error(
                    "Confirmation required. Use an explicit confirmation flag such as --yes.",
                    code="NEEDS_CONFIRMATION",
                )
            terminal.print_json(
                {
                    "error": "Confirmation required. Use an explicit confirmation flag such as --yes.",
                    "code": "NEEDS_CONFIRMATION",
                }
            )
            raise click.exceptions.Exit(1)
        if not terminal.confirm(f"Remove custom domain {domain_or_id}?", default=False):
            terminal.error("Cancelled.", code="CANCELLED")

    _api(service, "DELETE", f"/{quote(domain_or_id, safe='')}", format=format)

    if terminal.json_output(format):
        terminal.print_json({"deleted": domain_or_id})
        return

    terminal.success(f"Removed custom domain: {domain_or_id}")


management.add_command(get_domain, "show")
management.add_command(remove_domain, "delete")
