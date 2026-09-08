import json

import click
import requests

from .. import terminal
from ..channel import rpc_timeout
from ..clients.gateway import AuthorizeRequest
from .extraclick import ClickCommonGroup, pass_service_client


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command("logs", help="Read historical logs, or follow new logs as text or JSON lines.")
@click.option("--container-id")
@click.option("--task-id")
@click.option("--stub-id")
@click.option("--app-id")
@click.option("--tail", type=click.IntRange(1, 10000), default=100)
@click.option("--since", help="Start time in RFC 3339 format.")
@click.option("--follow", "-f", is_flag=True)
@click.option("--format", type=click.Choice(("text", "json")), default="text")
@pass_service_client
def logs(service, container_id, task_id, stub_id, app_id, tail, since, follow, format):
    params = {
        k: v
        for k, v in {
            "container_id": container_id,
            "task_id": task_id,
            "stub_id": stub_id,
            "app_id": app_id,
            "start_time": since,
            "limit": tail,
        }.items()
        if v
    }
    if not any((container_id, task_id, stub_id, app_id)):
        raise click.UsageError("Select a container, task, stub, or app to read logs.")
    with rpc_timeout(10):
        auth = service.gateway.authorize(AuthorizeRequest())
        if not auth.ok:
            terminal.error(auth.error_msg)
    url = f"{service._config.http_url}/api/v1/logs/{auth.workspace_id}" + (
        "/stream" if follow else ""
    )
    try:
        with requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {service._config.token}"},
            stream=follow,
            timeout=(10, 30),
            allow_redirects=False,
        ) as response:
            if not response.ok:
                terminal.error(
                    f"Log request failed (HTTP {response.status_code}): {response.text[:1000]}"
                )
            if not follow:
                data = response.json()
                if format == "json":
                    terminal.print_json(data)
                else:
                    for record in data.get("logs", []):
                        click.echo(record["message"])
                return
            event = b""
            for line in response.iter_lines(chunk_size=None):
                if line.startswith(b"event:"):
                    event = line[6:].strip()
                elif line.startswith(b"data:") and event in (b"log", b"error"):
                    record = json.loads(line[5:])
                    if event == b"error":
                        terminal.error(record["error"])
                    click.echo(json.dumps(record) if format == "json" else record["message"])
                elif not line:
                    event = b""
    except requests.RequestException as exc:
        terminal.error(f"Unable to read logs: {exc}")
