import json
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import click
import requests

from .. import terminal
from ..channel import rpc_timeout
from ..clients.gateway import ExportWorkspaceConfigRequest, ListDeploymentsRequest, StringList
from .extraclick import ClickCommonGroup, pass_service_client


def _parse_timestamp(value):
    # Python 3.10 requires fractional seconds to have three or six digits.
    value = re.sub(r"\.(\d+)", lambda match: "." + match[1][:6].ljust(6, "0"), value)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_time(ctx, param, value):
    if value is None:
        return None
    try:
        if match := re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", value):
            seconds = float(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]]
            return datetime.now(timezone.utc) - timedelta(seconds=seconds)
        result = _parse_timestamp(value)
        return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result
    except (ValueError, OverflowError):
        raise click.BadParameter("Use an ISO timestamp or a duration such as 30m or 2h.") from None


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    "logs",
    help="Read recent logs. TARGET is a deployment name/ID or container ID.",
    epilog="""
    Examples:
      {cli_name} logs my-api --since 30m
      {cli_name} logs --container-id CONTAINER_ID --follow
      {cli_name} logs my-api --json
    """,
)
@click.argument("target", required=False)
@click.option("--container-id", help="Read a container's logs.")
@click.option("--deployment-id", help="Read a deployment's logs.")
@click.option("--task-id", help="Read a task's logs.")
@click.option("--stub-id", help="Read a runtime's logs.")
@click.option("--app-id", help="Read an app's logs.")
@click.option(
    "--tail",
    "--lines",
    "-n",
    type=click.IntRange(0, 1000),
    metavar="N",
    default=100,
    help="Recent entries; 0 follows new logs only.",
)
@click.option(
    "--since", callback=_parse_time, help="Start time, such as 30m, 2h, or an ISO timestamp."
)
@click.option(
    "--until", callback=_parse_time, help="End time, exclusive; UTC when no offset is given."
)
@click.option("--search", help="Match text, ignoring case.")
@click.option(
    "--source",
    type=click.Choice(("stdout", "stderr", "system")),
    metavar="STREAM",
    help="stdout, stderr, or system.",
)
@click.option("--timestamps", "--show-timestamp", is_flag=True, help="Include UTC timestamps.")
@click.option("--show-container-id", is_flag=True, help="Include container IDs.")
@click.option("--follow", "-f", is_flag=True, help="Read recent entries, then keep following.")
@click.option("--json", "json_output", is_flag=True, help="Output one JSON record per line.")
@click.option(
    "--format",
    type=click.Choice(("text", "json", "jsonl")),
    metavar="FORMAT",
    default="text",
    help="text, json (history), or jsonl.",
)
@pass_service_client
def logs(
    service,
    target,
    container_id,
    deployment_id,
    task_id,
    stub_id,
    app_id,
    tail,
    since,
    until,
    search,
    source,
    timestamps,
    show_container_id,
    follow,
    json_output,
    format,
):
    targets = dict(
        container=container_id, deployment=deployment_id, task=task_id, stub=stub_id, app=app_id
    )
    if sum(bool(value) for value in [target, *targets.values()]) != 1:
        raise click.UsageError("Select one target: TARGET or one of the --*-id options.")
    if until and follow:
        raise click.UsageError("--until cannot be combined with --follow.")
    if since and until and since >= until:
        raise click.UsageError("--since must be before --until.")
    if tail == 0 and not follow:
        raise click.UsageError("--tail 0 requires --follow.")
    if json_output:
        format = "jsonl"

    with rpc_timeout(10):
        if target:
            if target.startswith(
                ("pod-", "sandbox-", "function-", "endpoint-", "asgi-", "taskqueue-")
            ):
                targets["container"] = target
            elif re.fullmatch(r"[a-fA-F0-9-]{36}", target):
                targets["deployment"] = target
            else:
                result = service.gateway.list_deployments(
                    ListDeploymentsRequest(
                        filters={
                            "name": StringList([target]),
                            "active": StringList(["true"]),
                        },
                        limit=2,
                    )
                )
                if not result.ok or len(result.deployments) != 1:
                    terminal.error(
                        result.err_msg or "Deployment not found or ambiguous; use --deployment-id."
                    )
                targets["deployment"] = result.deployments[0].id
        config = service.gateway.export_workspace_config(ExportWorkspaceConfigRequest())

    kind, identifier = next((kind, value) for kind, value in targets.items() if value)
    base_url = (
        service._config.api_url
        or f"{'https' if config.gateway_http_tls else 'http'}://{config.gateway_http_host}:{config.gateway_http_port}"
    )
    url = f"{base_url.rstrip('/')}/api/v1/logs/{config.workspace_id}"
    params = {"object_id": identifier, "object_type": f"BETA9_{kind.upper()}", "limit": tail or 100}
    params.update(
        {
            key: value
            for key, value in {
                "start_time": since.isoformat() if since else None,
                "end_time": until.isoformat() if until else None,
                "query": search,
            }.items()
            if value
        }
    )

    def emit(record):
        if not _matches_source(record, source):
            return
        if format in ("json", "jsonl"):
            click.echo(json.dumps(record, ensure_ascii=False))
            return
        prefix = []
        if timestamps:
            timestamp = _parse_timestamp(record["timestamp"])
            prefix.append(
                timestamp.astimezone(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
        if show_container_id and record.get("container_id"):
            prefix.append(record["container_id"])
        message = record["message"]
        if prefix:
            message = click.style(" ".join(prefix), dim=True) + " " + message
        click.echo(message, nl=not message.endswith("\n"))

    try:
        with requests.Session() as session:
            session.headers["Authorization"] = f"Bearer {service._config.token}"
            if follow:
                _follow(session, url, params, source, tail, emit)
            else:
                data = _history(session, url, params, source, tail)
                if format == "json":
                    terminal.print_json(data)
                else:
                    for record in data["logs"]:
                        emit(record)
    except KeyboardInterrupt:
        raise click.exceptions.Exit(130) from None
    except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
        terminal.error(f"Unable to read logs: {exc}")


def _request(session, url, **kwargs):
    response = session.get(url, timeout=(10, 30), allow_redirects=False, **kwargs)
    if not response.ok:
        with response:
            terminal.error(
                f"Log request failed (HTTP {response.status_code}): {response.text[:1000]}"
            )
    return response


def _matches_source(record, source):
    return not source or (record.get("stream") or "system") == source


def _history(session, url, params, source, tail):
    records = []
    params = {**params, "limit": 1000 if source else tail}
    page = 0
    while True:
        with _request(session, url, params={**params, "page": page}) as response:
            data = response.json()
        records = [r for r in data["logs"] if _matches_source(r, source)] + records
        if not source or len(records) >= tail or not data.get("next_cursor"):
            data["logs"] = records[-tail:]
            return data
        page = int(data["next_cursor"])


def _record_key(record):
    # History and live streams may have different sequence numbers for task logs.
    return tuple(
        record.get(key) for key in ("container_id", "task_id", "timestamp", "stream", "message")
    )


def _follow(session, url, params, source, tail, emit):
    overlap = Counter()
    headers = {}
    attempts = 0
    initial = True
    stream_params = {key: value for key, value in params.items() if key != "start_time"}
    while True:
        try:
            with _request(
                session, url + "/stream", params=stream_params, headers=headers, stream=True
            ) as response:
                if initial:
                    # Subscribe before reading history so records written during the fetch aren't lost.
                    if tail:
                        for record in _history(session, url, params, source, tail)["logs"]:
                            emit(record)
                            overlap[_record_key(record)] += 1
                    initial = False
                event = b""
                event_id = ""
                for line in response.iter_lines(chunk_size=None):
                    if line.startswith(b"id:"):
                        event_id = line[3:].strip().decode()
                    elif line.startswith(b"event:"):
                        event = line[6:].strip()
                    elif line.startswith(b"data:") and event in (b"log", b"error"):
                        record = json.loads(line[5:])
                        field = "error" if event == b"error" else "message"
                        if not isinstance(record, dict) or not isinstance(record.get(field), str):
                            raise ValueError(
                                f"Invalid log stream event: expected a string {field}."
                            )
                        if event == b"error":
                            terminal.error(record["error"])
                        attempts = 0
                        key = _record_key(record)
                        if overlap[key]:
                            overlap[key] -= 1
                        else:
                            emit(record)
                        if event_id:
                            headers["Last-Event-ID"] = event_id
                    elif not line:
                        event = b""
                        event_id = ""
            raise requests.ConnectionError("Log stream closed")
        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ):
            attempts += 1
            if attempts > 3:
                raise
            terminal.warn(f"Log connection lost; reconnecting ({attempts}/3).")
            time.sleep(attempts)
