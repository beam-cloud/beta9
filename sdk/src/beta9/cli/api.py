import json
from typing import Any, Dict, List, Optional

import click
import requests

from .. import terminal
from ..channel import ServiceClient, http_error_code
from . import extraclick
from .extraclick import ClickCommonGroup

OPENAPI_SPECS = [
    "https://raw.githubusercontent.com/beam-cloud/beta9/main/docs/openapi/gateway.swagger.json",
    "https://raw.githubusercontent.com/beam-cloud/beta9/main/docs/openapi/pod.swagger.json",
    "https://raw.githubusercontent.com/beam-cloud/beta9/main/docs/openapi/volume.swagger.json",
    "https://raw.githubusercontent.com/beam-cloud/beta9/main/docs/openapi/disk.swagger.json",
]

# Echo-router routes; `{ws}` is the caller's workspace id.
ECHO_ROUTES: List[Dict[str, str]] = [
    {"method": "GET", "path": "/api/v1/app/{ws}", "summary": "List apps"},
    {
        "method": "GET",
        "path": "/api/v1/app/{ws}/latest",
        "summary": "Apps with latest stub or deployment",
    },
    {"method": "GET", "path": "/api/v1/app/{ws}/{appId}", "summary": "Retrieve app"},
    {"method": "DELETE", "path": "/api/v1/app/{ws}/{appId}", "summary": "Delete app"},
    {"method": "GET", "path": "/api/v1/deployment/{ws}", "summary": "List deployments"},
    {"method": "GET", "path": "/api/v1/deployment/{ws}/latest", "summary": "Latest deployments"},
    {
        "method": "GET",
        "path": "/api/v1/deployment/{ws}/{deploymentId}",
        "summary": "Retrieve deployment",
    },
    {
        "method": "POST",
        "path": "/api/v1/deployment/{ws}/stop/{deploymentId}",
        "summary": "Stop deployment",
    },
    {
        "method": "POST",
        "path": "/api/v1/deployment/{ws}/start/{deploymentId}",
        "summary": "Start deployment",
    },
    {
        "method": "DELETE",
        "path": "/api/v1/deployment/{ws}/{deploymentId}",
        "summary": "Delete deployment",
    },
    {"method": "GET", "path": "/api/v1/stub/{ws}", "summary": "List stubs"},
    {"method": "GET", "path": "/api/v1/stub/{ws}/{stubId}", "summary": "Retrieve stub"},
    {
        "method": "PATCH",
        "path": "/api/v1/stub/{ws}/{stubId}/config",
        "summary": "Patch stub config {fields:{path:value}}",
    },
    {
        "method": "POST",
        "path": "/api/v1/stub/{ws}/{stubId}/scale",
        "summary": "Scale pod deployment {containers}",
    },
    {"method": "GET", "path": "/api/v1/stub/{ws}/{stubId}/url", "summary": "Stub URL"},
    {"method": "GET", "path": "/api/v1/task/{ws}", "summary": "List tasks (paginated)"},
    {"method": "GET", "path": "/api/v1/task/{ws}/{taskId}", "summary": "Retrieve task"},
    {"method": "GET", "path": "/api/v1/container/{ws}", "summary": "List containers"},
    {
        "method": "POST",
        "path": "/api/v1/container/{ws}/{containerId}/stop",
        "summary": "Stop container",
    },
    {"method": "GET", "path": "/api/v1/logs/{ws}", "summary": "Query logs"},
    {"method": "GET", "path": "/api/v1/events/{ws}/history", "summary": "Event history"},
    {
        "method": "GET",
        "path": "/api/v1/metrics/{ws}/stub-timeseries",
        "summary": "Stub metric timeseries",
    },
    {"method": "GET", "path": "/api/v1/workspace/current", "summary": "Current workspace"},
    {
        "method": "GET",
        "path": "/api/v1/workspace/{ws}/limits",
        "summary": "Workspace limits and GPU options",
    },
    {"method": "GET", "path": "/api/v1/token/{ws}", "summary": "List workspace tokens"},
    {"method": "GET", "path": "/secret/{ws}/", "summary": "List secrets"},
    {"method": "POST", "path": "/secret/{ws}/", "summary": "Create secret {name,value}"},
    {"method": "GET", "path": "/volume/{ws}/", "summary": "List volumes"},
]


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="api", help="Call the gateway REST API directly (discovery first).")
def api():
    pass


def _load_specs() -> List[Dict[str, Any]]:
    specs = []
    for url in OPENAPI_SPECS:
        try:
            response = requests.get(url, timeout=10)
            if response.ok:
                specs.append(response.json())
        except requests.RequestException:
            continue
    return specs


def _spec_routes(specs: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    routes = []
    for spec in specs:
        for path, methods in (spec.get("paths") or {}).items():
            for method, op in methods.items():
                if method.upper() not in ("GET", "POST", "PUT", "PATCH", "DELETE"):
                    continue
                routes.append(
                    {
                        "method": method.upper(),
                        "path": f"/api/v1/gateway{path}",
                        "summary": op.get("summary") or op.get("operationId", ""),
                        "operation": op.get("operationId", ""),
                    }
                )
    return routes


@api.command(name="search", help="Find REST routes by keyword.")
@click.argument("term", required=False, default="")
@extraclick.format_option
def search(term: str, format: str):
    routes = ECHO_ROUTES + _spec_routes(_load_specs())
    needle = term.lower()
    matches = [
        r
        for r in routes
        if not needle
        or needle in r["path"].lower()
        or needle in r.get("summary", "").lower()
        or needle in r.get("operation", "").lower()
    ]
    if terminal.json_output(format):
        terminal.print_json(matches)
        return
    for r in matches:
        terminal.print(
            f"[bold]{r['method']:<6}[/bold] {r['path']}  [dim]{r.get('summary', '')}[/dim]"
        )
    terminal.detail(f"{len(matches)} route(s). Call one with: api call METHOD PATH [--data JSON]")


@api.command(
    name="call", help="Call a REST route. `{ws}` in PATH is replaced with your workspace id."
)
@click.argument(
    "method", type=click.Choice(("GET", "POST", "PUT", "PATCH", "DELETE"), case_sensitive=False)
)
@click.argument("path")
@click.option("--data", "-d", type=click.STRING, default=None, help="JSON request body.")
@click.option("--query", "-q", multiple=True, help="Query parameter KEY=VALUE (repeatable).")
@extraclick.pass_service_client
def call(service: ServiceClient, method: str, path: str, data: Optional[str], query: List[str]):
    path = path.replace("{workspace_id}", "{ws}")
    params = dict(q.split("=", 1) for q in query if "=" in q)
    body = None
    if data:
        try:
            body = json.loads(data)
        except json.JSONDecodeError as exc:
            terminal.error(f"--data is not valid JSON: {exc}", code="INVALID_CONFIG")

    try:
        response = service.http.request(method.upper(), path, params=params, json=body)
    except requests.RequestException as exc:
        terminal.error(f"Request failed: {exc}", code="GATEWAY_UNAVAILABLE")

    try:
        payload: Any = response.json()
    except ValueError:
        payload = response.text

    if terminal.json_output():
        terminal.print_json({"status": response.status_code, "body": payload})
    else:
        terminal.detail(f"{method.upper()} {service.http.url(path)} → {response.status_code}")
        if isinstance(payload, (dict, list)):
            terminal.print_json(payload)
        else:
            terminal.print(payload)

    if response.status_code >= 400:
        terminal.error(f"HTTP {response.status_code}", code=http_error_code(response.status_code))
