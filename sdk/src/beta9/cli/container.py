import datetime
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextvars import copy_context
from typing import List, Optional

import click
import grpc
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..abstractions.base.container import Container
from ..abstractions.image import Image
from ..abstractions.sandbox import Sandbox
from ..channel import ServiceClient, rpc_timeout
from ..cli import extraclick
from ..clients.gateway import (
    CheckpointContainerRequest,
    ListContainersRequest,
    StopContainerRequest,
    StopContainerResponse,
)
from ..logging import StoredStdoutInterceptor
from ..exceptions import SandboxConnectionError, SandboxFileSystemError, SandboxProcessError
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="container",
    help="Manage containers.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command("create", help="Create a sandbox for exec and file operations.")
@click.option("--image-id", help="Use an existing image built with image build.")
@click.option("--name", help="App name for this sandbox.")
@click.option("--cpu", type=float, default=1.0)
@click.option("--memory", type=int, default=256, help="Memory in MiB.")
@click.option("--ttl", type=click.IntRange(min=1), default=600, help="Sandbox lifetime in seconds.")
@click.option("--pool", default=None)
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def create_container(service, image_id, name, cpu, memory, ttl, pool, format):
    with StoredStdoutInterceptor(capture_logs=format == "json"):
        sandbox = Sandbox(
            image=Image.from_id(image_id) if image_id else Image(python_version="python3.11"),
            name=name,
            cpu=cpu,
            memory=memory,
            keep_warm_seconds=ttl,
            pool=pool,
        ).create()
        if not sandbox.ok:
            terminal.error(sandbox.error_msg)
    if format == "json":
        terminal.print_json(
            {
                "container_id": sandbox.container_id,
                "stub_id": sandbox.stub_id,
                "context": extraclick.selected_context(),
                "status": "submitted",
            }
        )
    else:
        cli = extraclick.command_hint()
        terminal.resource(
            "Next steps",
            {
                "Exec": f"{cli} container exec {sandbox.container_id} -- COMMAND",
                "Stop": f"{cli} container stop {sandbox.container_id}",
            },
            show_labels=False,
        )


@management.command(
    "exec", help="Execute argv in a sandbox; preserve output streams and exit status."
)
@click.argument("container_id")
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
@click.option("--cwd", default=None)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0, min_open=True),
    default=300,
    help="Execution deadline in seconds.",
)
@extraclick.pass_service_client
def exec_container(service, container_id, command, cwd, timeout):
    process = None
    deadline = time.monotonic() + timeout
    timed_out = False
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            try:
                with rpc_timeout(timeout):
                    sandbox = _connect_sandbox(container_id, timeout)
                    process = sandbox.process.exec(
                        *command, cwd=cwd, stdin=None if sys.stdin.isatty() else sys.stdin.buffer
                    )
                    outputs = [
                        executor.submit(_copy_output, stream, target, deadline)
                        for stream, target in (
                            (process.stdout, sys.stdout),
                            (process.stderr, sys.stderr),
                        )
                    ]
                    result = executor.submit(
                        copy_context().run, process.wait, max(0, deadline - time.monotonic())
                    )
                    for completed in as_completed([*outputs, result]):
                        completed.result()
                    exit_code = result.result()
            except BaseException:
                timed_out = time.monotonic() >= deadline
                try:
                    if process is not None and process.exit_code < 0:
                        # The execution deadline has unwound; cancellation gets its own budget.
                        with rpc_timeout(3):
                            process.kill()
                except Exception as exc:
                    terminal.warn(f"Unable to cancel process {process.pid}: {exc}")
                finally:
                    service.close()
                raise
        raise click.exceptions.Exit(exit_code)
    except (
        SandboxConnectionError,
        SandboxProcessError,
        OSError,
        UnicodeError,
        grpc.RpcError,
    ) as exc:
        if timed_out and isinstance(
            exc, (SandboxConnectionError, SandboxProcessError, grpc.RpcError)
        ):
            terminal.error(f"Command timed out after {timeout:g}s")
        if isinstance(exc, grpc.RpcError):
            raise
        terminal.error(str(exc))


def _connect_sandbox(container_id, timeout):
    with rpc_timeout(timeout):
        return Sandbox().connect(container_id)


def _copy_output(stream, target, deadline):
    with rpc_timeout(deadline - time.monotonic()):
        for chunk in stream:
            target.write(chunk)
            target.flush()


@management.command("cp", help="Copy a file. Use CONTAINER_ID:/path for the remote side.")
@click.argument("source")
@click.argument("destination")
@extraclick.pass_service_client
def copy_container_file(service, source, destination):
    if (":" in source) == (":" in destination):
        raise click.UsageError("Specify one local path and one CONTAINER_ID:/path.")
    download = ":" in source
    remote, local = (source, destination) if download else (destination, source)
    container_id, remote_path = remote.split(":", 1)
    try:
        sandbox = _connect_sandbox(container_id, 30)
        if download:
            sandbox.fs.download_file(remote_path, local)
        else:
            sandbox.fs.upload_file(local, remote_path)
    except (SandboxConnectionError, SandboxFileSystemError, OSError) as exc:
        terminal.error(str(exc))


AVAILABLE_LIST_COLUMNS = {
    "container_id": "ID",
    "status": "Status",
    "stub_id": "Stub ID",
    "deployment_id": "Deployment ID",
    "scheduled_at": "Scheduled At",
    "uptime": "Uptime",
    "worker_id": "Worker ID",
    "machine_id": "Machine ID",
}


def _format_uptime(started_at: Optional[datetime.datetime], now: datetime.datetime) -> str:
    if started_at is None:
        return "N/A"
    epoch = datetime.datetime.fromtimestamp(0, tz=started_at.tzinfo)
    if started_at <= epoch:
        return "N/A"
    return terminal.humanize_duration(now - started_at)


@management.command(
    name="list",
    help="""
    List all current containers.
    """,
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@click.option(
    "--columns",
    type=click.STRING,
    default="container_id,status,uptime",
    help="""
      Specify columns to display.
      Available columns: container_id, status, stub_id, scheduled_at, deployment_id, uptime
    """,
)
@click.option(
    "--machine",
    "machine_id",
    type=click.STRING,
    default="",
    help="Only show containers running on this machine.",
)
@extraclick.pass_service_client
@click.pass_context
def list_containers(
    ctx: click.Context,
    service: ServiceClient,
    format: str,
    columns: str,
    machine_id: str,
):
    res = service.gateway.list_containers(ListContainersRequest())
    if not res.ok:
        terminal.error(res.error_msg)

    if machine_id:
        res.containers = [c for c in res.containers if c.machine_id == machine_id]

    now = datetime.datetime.now(datetime.timezone.utc)
    if format == "json":
        containers = []
        for c in res.containers:
            container_dict = c.to_dict(casing=Casing.SNAKE)
            container_dict["uptime"] = _format_uptime(c.started_at, now)
            containers.append(container_dict)
        terminal.print_json(containers)
        return

    user_requested_columns = set(columns.split(","))
    if unknown := user_requested_columns - AVAILABLE_LIST_COLUMNS.keys():
        raise click.BadParameter(
            f"Unknown columns: {', '.join(sorted(unknown))}", param_hint="--columns"
        )

    if machine_id:
        user_requested_columns.add("machine_id")

    # Build the ordered list of columns based on the ordering of AVAILABLE_LIST_COLUMNS
    ordered_columns = [
        col for col in AVAILABLE_LIST_COLUMNS.keys() if col in user_requested_columns
    ]

    table_cols = [
        Column(AVAILABLE_LIST_COLUMNS[col], no_wrap=True, overflow="ellipsis")
        for col in ordered_columns
    ]

    if len(res.containers) == 0:
        terminal.resource(
            "No running containers", {"Create": f"{extraclick.command_hint()} container create"}
        )
        return

    table = Table(*table_cols, box=box.SIMPLE, header_style="bold cyan")
    for container in res.containers:
        row = []
        for col in ordered_columns:
            if col == "uptime":
                value = _format_uptime(container.started_at, now)
            else:
                value = getattr(container, col)
                if isinstance(value, datetime.datetime):
                    value = terminal.humanize_date(value)
            row.append(value)
        table.add_row(*row)

    table.add_section()
    table.add_row(f"[bold]{len(res.containers)} total")
    terminal.print(table)
    terminal.detail("Use --format json for full IDs and resource details.")


@management.command(
    name="stop",
    help="Stop a container.",
)
@click.argument(
    "container_ids",
    nargs=-1,
    required=True,
)
@extraclick.pass_service_client
def stop_container(service: ServiceClient, container_ids: List[str]):
    failed = False
    for container_id in container_ids:
        res: StopContainerResponse
        res = service.gateway.stop_container(StopContainerRequest(container_id=container_id))

        if res.ok:
            terminal.success(f"Stopped container: {container_id}")
        else:
            terminal.error(f"{res.error_msg}", exit=False)
            failed = True
    if failed:
        raise click.exceptions.Exit(1)


@management.command(
    name="attach",
    help="Attach to a running container.",
)
@click.argument(
    "container_id",
    required=True,
)
@extraclick.pass_service_client
def attach_to_container(_: ServiceClient, container_id: str):
    container = Container(container_id=container_id)
    container.attach(container_id=container_id)


@management.command(
    name="checkpoint",
    help="Checkpoint a running container.",
)
@click.argument(
    "container_id",
    required=True,
)
@extraclick.pass_service_client
def checkpoint_container(service: ServiceClient, container_id: str):
    with terminal.progress("Creating checkpoint..."):
        res = service.gateway.checkpoint_container(
            CheckpointContainerRequest(container_id=container_id)
        )

    if res.ok:
        terminal.success(f"Checkpoint created for container: {container_id} -> {res.checkpoint_id}")
    else:
        terminal.error(f"{res.error_msg}")
