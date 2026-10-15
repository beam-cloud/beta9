"""Persistent CPU VMs. File/exec operations share the sandbox transport."""

import os
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
import webbrowser
from contextlib import nullcontext, redirect_stdout, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, urlencode

import click
import grpc
import websocket
from rich import box
from rich.table import Column, Table
from rich.text import Text

from .. import terminal
from ..abstractions.image import Image
from ..abstractions.vm import VM, identity, prepare_image
from ..abstractions.volume import Volume
from ..channel import GatewayHTTPError, ServiceClient, rpc_timeout
from ..clients.gateway import ListContainersRequest
from ..config import SDKSettings, get_config_context, get_settings, set_settings
from ..exceptions import SandboxConnectionError
from ..logging import StoredStdoutInterceptor
from ..tunnel import bridge_tunnel
from . import extraclick
from .extraclick import ClickManagementGroup
from .machine_format import format_cpu, format_memory


class VMGroup(ClickManagementGroup):
    def invoke(self, ctx):
        try:
            return super().invoke(ctx)
        except GatewayHTTPError as exc:
            raise click.ClickException(exc.message) from exc
        except SandboxConnectionError as exc:
            raise click.ClickException(exc.message) from exc
        except (click.ClickException, click.exceptions.Exit):
            raise
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(f"{exc.cmd[0]} exited with status {exc.returncode}") from exc
        except (RuntimeError, TimeoutError, OSError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc


@click.group(name="vm", cls=VMGroup, help="Create and manage persistent CPU microVMs.")
def management():
    pass


def _key_values(values):
    try:
        return extraclick.env_vars_to_dict(values)
    except ValueError as exc:
        raise click.UsageError("Expected KEY=VALUE") from exc


def _vm(service, name):
    return VM.get(name, _service=service)


def _progress(message, done, as_json=False):
    return nullcontext() if as_json else terminal.StepTracker().step(message, done)


def _image(dockerfile=None, build_context=None, image_id=None, image_uri=None, secrets=()):
    image = (
        Image.from_dockerfile(dockerfile, build_context)
        if dockerfile
        else (
            Image.from_id(image_id) if image_id else Image(base_image=image_uri or "ubuntu:22.04")
        )
    )
    image.ignore_python = True
    return image.with_secrets(list(secrets))


def _command(command):
    return command[1:] if command[:1] == ("--",) else command


def _published_urls(info):
    urls = {str(port): url for port, url in (info.get("urls") or {}).items()}
    for feature, port in (("terminal", 7681), ("desktop", 8080)):
        if info.get(feature + "_url"):
            urls.setdefault(str(port), info[feature + "_url"])
    return urls


def _port_url(vm, port):
    url = _published_urls(vm.info).get(str(port))
    if not url:
        spec = vm.info.get("spec") or {}
        command = extraclick.command_hint()
        name = shlex.quote(vm.name)
        if port == 2222 and spec.get("ssh"):
            raise click.ClickException(f"Port 2222 uses SSH. Run: {command} vm ssh {name}")
        if port in (spec.get("private_ports") or []):
            raise click.ClickException(
                f"Port {port} is tunnel-only. Run: {command} vm port-forward {name} {port}"
            )
        raise click.ClickException(
            f"Port {port} is not published. Run: {command} vm expose {name} {port}"
        )
    return url


def _browser_info(service, info):
    """Show the actual VM endpoints; account authentication happens in the browser."""
    return {**info, "urls": _published_urls(info)}


def _open_browser(vm, port, url):
    if not vm.info.get("browser_auth") and port in (
        (vm.info.get("spec") or {}).get("protected_ports") or []
    ):
        # Older/self-hosted gateways can still seed a cookie directly. Keep
        # the temporary credential out of normal terminal output.
        url = vm.access_url(port, ttl=3600)
    webbrowser.open(url)


def _show(info, as_json, service=None):
    if as_json or terminal.json_output():
        terminal.print_json(info)
    elif isinstance(info, list):
        table = Table("Name", "Status", "ID", box=box.SIMPLE, header_style="bold cyan")
        for record in info:
            table.add_row(
                record["name"], record.get("status", record.get("kind", "")), record["id"]
            )
        terminal.print(table)
    else:
        if service is not None and not terminal.json_output():
            info = _browser_info(service, info)
        protected = (info.get("spec") or {}).get("protected_ports") or []
        fields = {
            label: info[key]
            for key, label in (
                ("id", "ID"),
                ("status", "Status"),
                ("error", "Error"),
                ("public_ipv6", "IPv6"),
                ("terminal_url", "Terminal" if 7681 in protected else "Terminal (public)"),
                ("desktop_url", "Desktop" if 8080 in protected else "Desktop (public)"),
            )
            if info.get(key)
        }
        for port, url in (info.get("urls") or {}).items():
            if (str(port) == "7681" and info.get("terminal_url")) or (
                str(port) == "8080" and info.get("desktop_url")
            ):
                continue
            access = "private" if int(port) in protected else "public"
            fields[f"Port {port} ({access})"] = url
        terminal.resource(
            info.get("name", "VM"),
            fields,
        )
        for port in info.get("urls") or {}:
            if int(port) in protected:
                terminal.detail(
                    "Private links require workspace access. "
                    f"Share temporary access: {extraclick.command_hint()} vm share "
                    f"{shlex.quote(info['name'])} {port}"
                )
                break


@management.command("new")
@click.argument("name", required=False)
@click.option(
    "--cpu",
    type=click.FloatRange(min=0.1),
    metavar="CORES",
    help="CPUs; defaults to 4.",
)
@click.option(
    "--memory",
    type=click.IntRange(min=256),
    metavar="MiB",
    help="RAM in MiB; defaults to 8192.",
)
@click.option(
    "--disk-size",
    help="Writable disk over the base image; 1–100 GiB, default 50 GiB.",
)
@click.option("--image", "image_uri", help="Base registry image; defaults to ubuntu:22.04.")
@click.option("--image-id", help="Existing image with Beam VM services installed.")
@click.option(
    "--dockerfile", type=click.Path(exists=True, dir_okay=False), help="Build a custom VM image."
)
@click.option(
    "--build-context",
    type=click.Path(exists=True, file_okay=False),
    help="Dockerfile build context.",
)
@click.option(
    "--build-secret",
    multiple=True,
    metavar="NAME",
    help="Secret available during image build; repeatable.",
)
@click.option("--template", metavar="NAME", help="Create from a saved template.")
@click.option(
    "--desktop/--no-desktop",
    default=None,
    help="Include a browser desktop (default for new images; templates retain their settings).",
)
@click.option("--docker-enabled", is_flag=True, default=None, help="Enable Docker inside the VM.")
@click.option("--env", multiple=True, metavar="KEY=VALUE", help="Environment variable; repeatable.")
@click.option(
    "--secret",
    multiple=True,
    metavar="NAME",
    help="Runtime secret with approved hosts; repeatable.",
)
@click.option(
    "--port",
    multiple=True,
    type=click.IntRange(1, 65535),
    metavar="PORT",
    help="Publish an application port; repeatable.",
)
@click.option("--no-ssh", is_flag=True, help="Disable SSH access.")
@click.option(
    "--sync",
    "sync_dir",
    type=click.Path(exists=True, file_okay=False),
    help="Upload a local directory after creation.",
)
@click.option(
    "--ttl",
    type=click.IntRange(0, 31536000),
    metavar="SECONDS",
    help="Snapshot and sleep after idle seconds; unset or 0 keeps running.",
)
@click.option(
    "--idle-action",
    type=click.Choice(["stop", "pause"]),
    help="Stop from disk (default), or pause with RAM.",
)
@click.option("--pool", metavar="NAME", help="CPU microVM worker pool.")
@click.option(
    "--metadata", multiple=True, metavar="KEY=VALUE", help="Resource metadata; repeatable."
)
@click.option("--auto-resume/--no-auto-resume", default=None, help="Wake on SDK or URL access.")
@click.option(
    "--block-network",
    is_flag=True,
    default=None,
    help="Block outbound access except bound credential hosts.",
)
@click.option(
    "--allow-network",
    multiple=True,
    metavar="HOST_OR_CIDR",
    help="Allow only these hostnames, *.wildcards, IPs or CIDRs; up to 64 entries.",
)
@click.option(
    "--protected-port",
    multiple=True,
    type=click.IntRange(1, 65535),
    metavar="PORT",
    help="Publish a protected port; use 'vm open NAME PORT' for browser access.",
)
@click.option("--request-id", type=click.UUID, help="Reuse a UUID to retry creation safely.")
@click.option(
    "--public-port",
    multiple=True,
    type=click.IntRange(1, 65535),
    metavar="PORT",
    help="Publish a port without an access token, including terminal/desktop ports.",
)
@click.option(
    "--disk",
    multiple=True,
    type=extraclick.DurableDiskSpec(),
    help="Additional durable disk NAME:/mount[:SIZE].",
)
@click.option(
    "--volume",
    multiple=True,
    metavar="NAME:/mount",
    help="Shared workspace volume; persists independently of the VM.",
)
@click.option("--json", "as_json", is_flag=True, help="Print the complete result as JSON.")
@extraclick.pass_service_client
def new(
    service,
    name,
    cpu,
    memory,
    disk_size,
    image_uri,
    image_id,
    dockerfile,
    build_context,
    build_secret,
    template,
    desktop,
    docker_enabled,
    env,
    secret,
    port,
    no_ssh,
    sync_dir,
    ttl,
    pool,
    as_json,
    metadata=(),
    auto_resume=None,
    block_network=None,
    allow_network=(),
    protected_port=(),
    public_port=(),
    request_id=None,
    disk=(),
    volume=(),
    idle_action=None,
):
    """Create a VM with a durable root. NAME defaults to a friendly generated name."""
    if sum(bool(v) for v in (image_uri, image_id, dockerfile, template)) > 1:
        raise click.UsageError("Choose one of --image, --image-id, --dockerfile or --template")
    if build_context and not dockerfile:
        raise click.UsageError("--build-context requires --dockerfile")
    if no_ssh and sync_dir:
        raise click.UsageError("--sync requires SSH; remove --no-ssh")
    try:
        env_map = extraclick.env_vars_to_dict(env)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc
    image = _image(dockerfile, build_context, image_id, image_uri, build_secret)
    vm = VM(
        name,
        image=image,
        cpu=cpu,
        memory=memory,
        disk_size=disk_size,
        desktop=True if desktop is None and not (template or image_id) else desktop,
        docker_enabled=docker_enabled,
        env=env_map if env else None,
        secrets=list(secret) if secret else None,
        ports=(
            list(dict.fromkeys((*port, *protected_port, *public_port)))
            if port or protected_port or public_port
            else None
        ),
        ssh=False if no_ssh else None,
        ttl=ttl,
        idle_action=idle_action,
        pool=pool,
        template=template,
        metadata=_key_values(metadata) if metadata else None,
        auto_resume=auto_resume,
        block_network=block_network,
        allow_list=list(allow_network) if allow_network else None,
        protected_ports=list(protected_port) if protected_port else None,
        public_ports=list(public_port) if public_port else None,
        request_id=str(request_id) if request_id else None,
        disks=list(disk) if disk else None,
        volumes=_volumes(volume) if volume else None,
        _service=service,
    )
    with StoredStdoutInterceptor(capture_logs=as_json):
        with _progress("Creating VM", "VM ready", as_json):
            vm.create(wait=False)
            vm.wait()
        if sync_dir:
            _sync(vm, sync_dir, False)
    _show(vm.info, as_json, service)


def _volumes(entries):
    result = []
    for entry in entries:
        name, mount = extraclick.MountSpec().convert(entry, None, None)
        result.append(Volume(name=name, mount_path=mount))
    return result


@management.group("image")
def image_group():
    """Build images with the VM services installed."""


@image_group.command("build")
@click.argument("build_context", default=".", type=click.Path(exists=True, file_okay=False))
@click.option("-f", "--file", "dockerfile", type=click.Path(exists=True, dir_okay=False))
@click.option("--secret", "--build-secret", "build_secret", multiple=True)
@click.option("--desktop", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def image_build(service, build_context, dockerfile, build_secret, desktop, as_json):
    """Build a VM image from a local Dockerfile."""
    dockerfile = dockerfile or str(Path(build_context) / "Dockerfile")
    image = _image(dockerfile, build_context, secrets=build_secret)
    with StoredStdoutInterceptor(capture_logs=as_json):
        with _progress("Preparing image", "Image ready", as_json):
            result = prepare_image(image, service, desktop).build()
            if not result.success:
                raise click.ClickException(result.error or "VM image build failed")
    if as_json:
        terminal.print_json({"image_id": result.image_id, "desktop": desktop})
    else:
        click.echo(result.image_id)


@management.command("list")
@click.option("--metadata", multiple=True, metavar="KEY=VALUE")
@click.option("--status")
@click.option("--all", "include_all", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def list_vms(service, include_all, as_json, metadata=(), status=None):
    """List VM resources, disk sizes and current uptime."""
    records = VM.list(
        all=include_all,
        metadata=_key_values(metadata) if metadata else None,
        status=status,
        _service=service,
    )
    if as_json:
        terminal.print_json(records)
        return

    if not records:
        terminal.resource("No VMs found", {"Create": f"{terminal.cli_name()} vm new"})
        return

    starts = {}
    if any(record.get("status") == "running" for record in records):
        # One bounded lookup for the current boot, rather than the VM's creation date.
        with suppress(grpc.RpcError), rpc_timeout(5):
            response = service.gateway.list_containers(ListContainersRequest())
            if response.ok:
                starts = {c.container_id: c.started_at for c in response.containers}
    terminal.print(_vm_table(records, starts))


def _vm_table(records, starts):
    table = Table(
        Column("Name", overflow="ellipsis"),
        Column("Status", min_width=8, no_wrap=True),
        Column("Size", min_width=16, no_wrap=True),
        Column("Disk", justify="right", no_wrap=True),
        Column("Uptime", no_wrap=True),
        Column("ID", min_width=19, max_width=19, style="dim", no_wrap=True),
        box=box.SIMPLE,
        header_style="bold cyan",
        collapse_padding=True,
    )
    now = datetime.now(timezone.utc)
    colors = {
        "running": "green",
        "starting": "cyan",
        "stopping": "yellow",
        "paused": "yellow",
        "stopped": "dim",
        "error": "red",
        "deleted": "dim",
    }
    for record in records:
        spec = record.get("spec", {})
        size = " · ".join(
            formatter(spec[key])
            for key, formatter in (("cpu", format_cpu), ("memory", format_memory))
            if spec.get(key)
        )
        status = record.get("status", "")
        disk = str(spec.get("disk_size") or "")
        if disk.isdigit():
            disk = terminal.humanize_memory(int(disk)).replace(".00", "").replace(" ", "")
        started = starts.get(record.get("container_id")) if status == "running" else None
        uptime = (
            terminal.humanize_duration(max(now - started, timedelta()), compact=True)
            if started and started.timestamp() > 0
            else "-"
        )
        table.add_row(
            record["name"],
            Text(status, style=colors.get(status, "")),
            size or "-",
            disk or "-",
            uptime,
            record["id"],
        )
    count, suffix = terminal.pluralize(records)
    table.add_section()
    table.add_row(f"[bold]{count} VM{suffix}")
    return table


@management.command("get")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def get_vm(service, name, as_json):
    """Show a VM's identity, state and URLs."""
    _show(_vm(service, name).info, as_json, service)


@management.command("ip")
@click.argument("name")
@extraclick.pass_service_client
def ip_vm(service, name):
    """Print the persistent public IPv6 address."""
    address = _vm(service, name).public_ipv6
    if not address:
        raise click.ClickException("VM does not have a public IPv6 address")
    click.echo(address)


@management.command("update")
@click.argument("name")
@click.option(
    "--ttl",
    type=click.IntRange(0, 31536000),
    metavar="SECONDS",
    help="Snapshot and sleep after idle seconds; 0 disables it.",
)
@click.option(
    "--idle-action",
    type=click.Choice(["stop", "pause"]),
    help="Stop from disk (default), or pause with RAM.",
)
@click.option("--auto-resume/--no-auto-resume", default=None)
@click.option("--metadata", multiple=True, metavar="KEY=VALUE")
@click.option("--clear-metadata", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def update_vm(service, name, ttl, auto_resume, metadata, clear_metadata, as_json, idle_action=None):
    """Update idle sleep, automatic resume and metadata."""
    if metadata and clear_metadata:
        raise click.UsageError("Choose --metadata or --clear-metadata")
    values = _key_values(metadata) if metadata or clear_metadata else None
    vm = _vm(service, name).update(
        ttl=ttl, idle_action=idle_action, auto_resume=auto_resume, metadata=values
    )
    _show(vm.info, as_json, service)


@management.command("network")
@click.argument("name")
@click.option("--block/--open", default=False)
@click.option("--allow", multiple=True, metavar="HOST_OR_CIDR")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def network_vm(service, name, block, allow, as_json):
    """Set outbound network permissions."""
    vm = _vm(service, name).update_network_permissions(block_network=block, allow_list=list(allow))
    _show(vm.info, as_json, service)


@management.command("access-token")
@click.argument("name")
@click.option("--rotate", is_flag=True)
@extraclick.pass_service_client
def access_token_vm(service, name, rotate):
    """Print or rotate the token for protected URLs."""
    vm = _vm(service, name)
    click.echo(vm.rotate_access_token() if rotate else vm.traffic_access_token)


@management.command("access-url")
@click.argument("name")
@click.argument("port", type=click.IntRange(1, 65535))
@click.option(
    "--ttl",
    type=click.IntRange(1, 3600),
    default=600,
    show_default=True,
    metavar="SECONDS",
    help="Access link lifetime, from 1 to 3600 seconds.",
)
@extraclick.pass_service_client
def access_url_vm(service, name, port, ttl):
    """Share temporary browser access to a protected port; anyone with the link can use it."""
    click.echo(_vm(service, name).access_url(port, ttl=ttl))


management.add_command(access_url_vm, "share")


@management.command("start")
@click.argument("name")
@click.option("--cold", is_flag=True, help="Explicitly discard paused RAM and boot from disk.")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def start_vm(service, name, as_json, cold=False):
    """Start a stopped VM or resume its saved memory."""
    with _progress("Starting VM", "VM ready", as_json):
        vm = _vm(service, name).start(cold=cold)
    _show(vm.info, as_json, service)


management.add_command(start_vm, "resume")


@management.command("pause")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def pause_vm(service, name, as_json):
    """Save memory and disk, then release compute."""
    with _progress("Pausing VM", "VM paused", as_json):
        vm = _vm(service, name).pause()
    _show(vm.info, as_json, service)


@management.command("stop")
@click.argument("name")
@click.option("--no-snapshot", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def stop_vm(service, name, no_snapshot, as_json):
    """Save the root and shut down the VM."""
    message = "Stopping VM" if no_snapshot else "Snapshotting and stopping VM"
    with _progress(message, "VM stopped", as_json):
        vm = _vm(service, name).stop(no_snapshot)
    _show(vm.info, as_json, service)


@management.command("rm")
@click.argument("name")
@extraclick.pass_service_client
def remove_vm(service, name):
    """Delete a VM and its root disk."""
    with _progress("Removing VM", "VM removed"):
        _vm(service, name).remove()


@management.command("fork")
@click.argument("source")
@click.argument("name", required=False)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def fork_vm(service, source, name, as_json):
    """Create an independent VM from a VM or snapshot."""
    with _progress("Forking VM", "VM ready", as_json):
        snapshots = VM(_service=service)._api("GET", "/artifacts/snapshot")
        if any(item["id"] == source for item in snapshots):
            vm = VM(name, snapshot=source, _service=service).create()
        else:
            vm = _vm(service, source).fork(name)
    _show(vm.info, as_json, service)


@management.command(
    "exec",
    context_settings={"ignore_unknown_options": True, "allow_interspersed_args": False},
)
@click.argument("name")
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
@click.option("--cwd", default="/workspace")
@click.option(
    "--timeout",
    type=click.IntRange(min=0),
    default=0,
    help="Command deadline in seconds; 0 waits indefinitely.",
)
@click.option("--detach", is_flag=True, help="Return a reattachable process ID immediately.")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def exec_vm(service, name, command, cwd, timeout=0, detach=False, as_json=False):
    """Run a command; put CLI options before NAME."""
    from .container import exec_container

    command = _command(command)
    if not command:
        raise click.UsageError("A command is required")
    if detach and timeout:
        raise click.UsageError("--timeout requires foreground execution; omit --detach")
    vm = _vm(service, name)
    sandbox = vm._sandbox()
    if detach or as_json:
        with nullcontext() if detach else vm.keep_alive():
            process = sandbox.process.exec(
                *command,
                cwd=cwd,
                stdin=sys.stdin.buffer if not detach and not sys.stdin.isatty() else None,
            )
            result = {
                "vm_id": vm.id,
                "container_id": vm.info["container_id"],
                "pid": process.pid,
            }
            if not detach:
                try:
                    result.update(
                        exit_code=process.wait(timeout or None),
                        stdout=process.stdout.read(),
                        stderr=process.stderr.read(),
                    )
                except (KeyboardInterrupt, Exception):
                    with suppress(Exception):
                        process.kill()
                    raise
        if as_json:
            terminal.print_json(result)
        else:
            click.echo(process.pid)
        if not detach:
            raise click.exceptions.Exit(result["exit_code"])
        return
    # The existing exec implementation streams both output channels, retains
    # argv boundaries, cancellation and the child's exit code.
    with vm.keep_alive():
        exec_container.callback.__wrapped__(service, vm.info["container_id"], command, cwd, timeout)


@management.command("ps")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def processes_vm(service, name, as_json):
    """List managed processes and their exit status."""
    processes = _vm(service, name).process.list_processes()
    rows = [
        {"pid": p.pid, "args": p.args, "cwd": p.cwd, "exit_code": p.exit_code}
        for p in processes.values()
    ]
    if as_json:
        terminal.print_json(rows)
    else:
        for row in rows:
            click.echo(f"{row['pid']}\t{row['exit_code']}\t{shlex.join(row['args'])}")


@management.command("kill")
@click.argument("name")
@click.argument("pid", type=click.IntRange(1))
@click.option(
    "--container-id",
    help="Refuse to kill if the VM has restarted since this process was launched.",
)
@extraclick.pass_service_client
def kill_vm(service, name, pid, container_id):
    """Stop a managed process."""
    vm = _vm(service, name)
    if container_id and container_id != vm.info["container_id"]:
        raise click.ClickException("The VM restarted; this process belongs to a previous runtime")
    sandbox = vm._sandbox(auto_resume=False)
    sandbox.process.get_process(pid).kill()


@management.command("metrics")
@click.argument("name")
@extraclick.pass_service_client
def metrics_vm(service, name):
    """Show CPU, memory and disk usage as JSON."""
    terminal.print_json(_vm(service, name).metrics())


def _url(service, name, field, open_url):
    vm = _vm(service, name)
    url = vm.info.get(field)
    port = 8080 if field == "desktop_url" else 7681
    if not url:
        raise click.ClickException(f"VM does not have {field.replace('_url', '')} enabled")
    url = _port_url(vm, port)
    click.echo(url)
    if open_url:
        _open_browser(vm, port, url)


def _url_command(feature):
    @management.command(feature, help=f"Print or open the browser {feature} URL.")
    @click.argument("name")
    @click.option("--open/--no-open", "open_url", default=True)
    @click.option(
        "--url",
        "url_only",
        is_flag=True,
        help="Print the URL without opening a browser.",
    )
    @extraclick.pass_service_client
    def command(service, name, open_url, url_only):
        _url(service, name, feature + "_url", open_url and not url_only)

    return command


desktop_vm = _url_command("desktop")
terminal_vm = _url_command("terminal")


@management.command("open")
@click.argument("name")
@click.argument("port", type=click.IntRange(1, 65535))
@click.option("--url", "url_only", is_flag=True, help="Print the URL without opening a browser.")
@extraclick.pass_service_client
def open_vm(service, name, port, url_only):
    """Open a published HTTP port. Private links use your Beam account."""
    vm = _vm(service, name)
    url = _port_url(vm, port)
    click.echo(url)
    if not url_only:
        _open_browser(vm, port, url)


@management.command("ports")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def ports_vm(service, name, as_json):
    """List ports, access modes and ready-to-open browser links."""
    vm = _vm(service, name)
    if as_json or terminal.json_output():
        terminal.print_json(vm.info["urls"])
    else:
        info = _browser_info(service, vm.info)
        spec = info.get("spec") or {}
        table = Table("Port", "Access", "URL / command", box=box.SIMPLE)
        for port, url in info["urls"].items():
            access = "Private" if int(port) in (spec.get("protected_ports") or []) else "Public"
            table.add_row(str(port), access, Text(url))
        command = extraclick.command_hint()
        for port in spec.get("private_ports") or []:
            table.add_row(
                str(port), "Tunnel only", f"{command} vm port-forward {shlex.quote(vm.name)} {port}"
            )
        if spec.get("ssh"):
            table.add_row("2222", "SSH key", f"{command} vm ssh {shlex.quote(vm.name)}")
        terminal.print(table)
        if spec.get("protected_ports"):
            terminal.detail(
                "Private links require workspace access. Use vm share for temporary access."
            )
        terminal.detail(f"Open any HTTP port: {command} vm open {shlex.quote(vm.name)} PORT")


@management.command("expose")
@click.option("--protected/--public", default=None)
@click.option("--json", "as_json", is_flag=True, help="Print VM metadata with stable URLs.")
@click.option("--url", "url_only", is_flag=True, help="Print only the ready-to-use browser link.")
@click.argument("name")
@click.argument("port", type=click.IntRange(1, 65535))
@extraclick.pass_service_client
def expose_vm(service, name, port, protected=None, as_json=False, url_only=False):
    """Publish a port and show how to open it; private links sign you in."""
    if as_json and url_only:
        raise click.UsageError("Choose --json or --url")
    vm = _vm(service, name)
    vm.expose(port, protected=protected)
    if url_only:
        click.echo(_port_url(vm, port))
    else:
        _show(vm.info, as_json, service)


@management.command("unexpose")
@click.argument("name")
@click.argument("port", type=click.IntRange(1, 65535))
@extraclick.pass_service_client
def unexpose_vm(service, name, port):
    """Remove a published port."""
    _vm(service, name).unexpose(port)


def _socket(vm, port, session):
    url = vm._service.http.url(f"/api/v1/vm/{{ws}}/{quote(vm.id, safe='')}/tunnel/{port}")
    url = url.replace("https://", "wss://").replace("http://", "ws://")
    return websocket.create_connection(
        url + "?" + urlencode({"protocol": 2, "session": session}),
        header=vm._service.http.headers,
        timeout=30,
    )


def _bridge(vm, port, source, target):
    bridge_tunnel(lambda session: _socket(vm, port, session), source, target)


@management.command("tunnel", hidden=True)
@click.argument("name")
@click.argument("port", type=int)
@extraclick.pass_service_client
def tunnel_vm(service, name, port):
    target = sys.stdout.buffer
    with redirect_stdout(sys.stderr):
        _bridge(_vm(service, name), port, sys.stdin.buffer, target)


def _run_tunnel(profile, config_path, cli_name, vm_id, port):
    """OpenSSH helper: bypass interactive CLI setup and keep stdout binary."""
    token = os.getenv("BEAM_TOKEN" if cli_name.lower() == "beam" else "BETA9_TOKEN")
    set_settings(SDKSettings(name=cli_name, config_path=Path(config_path), api_token=token))
    target = sys.stdout.buffer
    # OpenSSH terminates its proxy after a remote command exits. Unwind the
    # tunnel first so the worker can release its reconnect lease immediately.
    previous = {}

    def terminate(*_):
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        raise SystemExit(0)

    for name in ("SIGHUP", "SIGTERM"):
        if sig := getattr(signal, name, None):
            previous[sig] = signal.signal(sig, terminate)
    try:
        with redirect_stdout(sys.stderr):
            with ServiceClient(get_config_context(profile)) as service:
                _bridge(_vm(service, vm_id), port, sys.stdin.buffer, target)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _ssh_options(vm):
    if not vm.info["spec"]["ssh"]:
        raise click.ClickException("SSH was disabled for this VM")
    if vm.info.get("status") in ("stopped", "paused"):
        vm._sandbox()
    vm.wait(services=True)
    settings = get_settings()
    helper = (
        "from beta9.cli.vm import _run_tunnel; "
        f"_run_tunnel({extraclick.selected_context()!r}, {str(settings.config_path)!r}, "
        f"{settings.name!r}, {vm.id!r}, 2222)"
    )
    proxy = shlex.join([sys.executable, "-c", helper])
    return [
        "-i",
        str(identity()),
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"UserKnownHostsFile={identity().parent / 'known_hosts'}",
        "-o",
        f"HostKeyAlias={vm.id if vm.id.startswith('vm-') else 'vm-' + vm.id}",
        "-o",
        f"ProxyCommand={proxy}",
    ]


@management.command(
    "ssh",
    context_settings={"ignore_unknown_options": True, "allow_interspersed_args": False},
)
@click.argument("name")
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
@extraclick.pass_service_client
def ssh_vm(service, name, command):
    """Open an SSH shell or run a remote command."""
    command = _command(command)
    vm = _vm(service, name)
    argv = ["ssh", *_ssh_options(vm), "-p", "2222", "--", "root@" + vm.name]
    if command:
        argv += [shlex.join(command)]
    raise click.exceptions.Exit(subprocess.call(argv))


@management.command("scp")
@click.argument("source")
@click.argument("destination")
@click.option("-r", "recursive", is_flag=True)
@extraclick.pass_service_client
def scp_vm(service, source, destination, recursive):
    """Copy files to or from NAME:/remote/path."""
    remote = [value for value in (source, destination) if ":" in value]
    if len(remote) != 1:
        raise click.UsageError("Specify one VM path: NAME:/remote/path")
    name = remote[0].split(":", 1)[0]
    vm = _vm(service, name)
    source = "root@" + source if source == remote[0] else source
    destination = "root@" + destination if destination == remote[0] else destination
    args = (
        ["scp", *_ssh_options(vm), "-P", "2222"]
        + (["-r"] if recursive else [])
        + [source, destination]
    )
    raise click.exceptions.Exit(subprocess.call(args))


def _sync(vm, directory, watch):
    ssh = shlex.join(["ssh", *_ssh_options(vm), "-p", "2222"])
    args = [
        "rsync",
        "-az",
        "--exclude=.git",
        "-e",
        ssh,
        str(Path(directory).resolve()) + "/",
        f"root@{vm.name}:/workspace/",
    ]
    if not watch:
        subprocess.run(args, check=True)
    else:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        changed = threading.Event()

        class Handler(FileSystemEventHandler):
            def on_any_event(self, event):
                changed.set()

        observer = Observer()
        observer.schedule(Handler(), str(directory), recursive=True)
        observer.start()
        try:
            subprocess.run(args, check=True)
            while True:
                if changed.wait(1):
                    time.sleep(0.2)
                    changed.clear()
                    subprocess.run(args, check=True)
        finally:
            observer.stop()
            observer.join()


@management.command("sync")
@click.argument("name")
@click.argument("directory", default=".", type=click.Path(exists=True, file_okay=False))
@click.option("--watch", is_flag=True)
@extraclick.pass_service_client
def sync_vm(service, name, directory, watch):
    """Upload a directory, optionally watching for edits."""
    _sync(_vm(service, name), directory, watch)


@management.command("port-forward")
@click.argument("name")
@click.argument("port", metavar="PORT_OR_LOCAL:REMOTE")
@click.option("--local-port", type=click.IntRange(1, 65535))
@extraclick.pass_service_client
def forward_vm(service, name, port, local_port):
    """Forward a private VM port to localhost."""
    try:
        parts = [int(value) for value in port.split(":")]
        if len(parts) == 2 and local_port is None:
            local_port, port = parts
        elif len(parts) == 1:
            port = parts[0]
        else:
            raise ValueError()
        if not 1 <= port <= 65535 or (local_port is not None and not 1 <= local_port <= 65535):
            raise ValueError()
    except ValueError:
        raise click.UsageError("Use PORT or LOCAL:REMOTE, with ports between 1 and 65535")
    vm = _vm(service, name)
    if port not in vm.info["spec"].get("ports", []) and port not in vm.info["spec"].get(
        "private_ports", []
    ):
        vm.bind(port)
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", local_port or port))
    listener.listen()
    click.echo(f"127.0.0.1:{local_port or port} -> {vm.name}:{port}")

    def connection(client):
        with client, client.makefile("rb") as source, client.makefile("wb") as target:
            try:
                _bridge(vm, port, source, target)
            finally:
                # Wake the input reader before closing its buffered file.
                with suppress(OSError):
                    client.shutdown(socket.SHUT_RDWR)

    try:
        while True:
            client, _ = listener.accept()
            threading.Thread(target=connection, args=(client,), daemon=True).start()
    finally:
        listener.close()


@management.group("snapshot")
def snapshot_group():
    """Capture and list filesystem restore points."""


@snapshot_group.command("create")
@click.argument("vm_name")
@click.option("--name", "--label")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def snapshot_create(service, vm_name, name, as_json):
    """Capture a filesystem restore point."""
    with _progress("Snapshotting VM", "Snapshot created", as_json):
        snapshot = _vm(service, vm_name).snapshot(name)
    _show(snapshot, as_json)


@snapshot_group.command("list")
@click.argument("vm_name", required=False)
@click.option("--all", "include_all", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def snapshot_list(service, vm_name, include_all, as_json):
    """List filesystem restore points."""
    items = VM(_service=service)._api("GET", "/artifacts/snapshot")
    if vm_name:
        vm = _vm(service, vm_name)
        items = [item for item in items if item["vm_id"] == vm.id]
    _show(items, as_json)


@snapshot_group.command("rm")
@click.argument("name")
@extraclick.pass_service_client
def snapshot_rm(service, name):
    """Delete a filesystem restore point."""
    VM(_service=service).remove_snapshot(name)


@management.command("screenshot")
@click.argument("name")
@click.argument("path", type=click.Path(dir_okay=False))
@extraclick.pass_service_client
def screenshot_vm(service, name, path):
    """Save a desktop screenshot as a local PNG."""
    _vm(service, name).desktop.screenshot(path)
    click.echo(path)


@management.group("template")
def template_group():
    """Manage reusable private VM roots."""


@template_group.command("create")
@click.argument("vm_name")
@click.argument("name", required=False)
@click.option("-d", "--description", default="")
@click.option("--public", "public", is_flag=True, help="Reserved; complete VM roots are private.")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def template_create(service, vm_name, name, description, public, as_json):
    """Save the VM root as a reusable private template."""
    if public:
        raise click.UsageError("Public templates are unsupported; complete VM roots remain private")
    with _progress("Creating template", "Template created", as_json):
        template = _vm(service, vm_name).create_template(name, description)
    _show(template, as_json)


@template_group.command("list")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def template_list(service, as_json):
    """List private VM templates."""
    _show(VM(_service=service)._api("GET", "/artifacts/template"), as_json)


@template_group.command("show")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def template_show(service, name, as_json):
    """Show a private VM template."""
    for item in VM(_service=service)._api("GET", "/artifacts/template"):
        if item["name"] == name or item["id"] == name:
            _show(item, as_json)
            return
    raise click.ClickException("Template not found")


@template_group.command("rm")
@click.argument("name")
@extraclick.pass_service_client
def template_rm(service, name):
    """Delete a private VM template."""
    VM(_service=service)._api("DELETE", "/artifacts/template/" + quote(name, safe=""))


@management.command("prompt")
@click.argument("name")
@click.argument("prompt")
@click.option("--agent", type=click.Choice(["claude", "codex"]), default="claude")
@click.option("--background", "--detach", is_flag=True)
@click.option("--cwd", default="/workspace")
@extraclick.pass_service_client
def prompt_vm(service, name, prompt, agent, background, cwd):
    """Run an installed agent with durable session logs."""
    vm = _vm(service, name)
    session = "prompt-" + uuid.uuid4().hex[:12]
    log_dir = "/workspace/.beam-vm/logs"
    command = ["claude", "-p", prompt] if agent == "claude" else ["codex", "exec", prompt]
    shell = f"mkdir -p {log_dir}; set -o pipefail; {shlex.join(command)} 2>&1 | tee {log_dir}/{session}.log"
    if background:
        process = vm.process.exec(
            "systemd-run",
            "--collect",
            "--unit=" + session,
            "--working-directory=" + cwd,
            "bash",
            "-lc",
            shell,
        )
        if process.wait() != 0:
            raise click.ClickException(process.stderr.read() or "Failed to launch prompt service")
        click.echo(session)
    else:
        exec_vm.callback.__wrapped__(service, name, ("bash", "-lc", shell), cwd)


@management.command("logs")
@click.argument("name")
@click.option("--unit", help="Read the systemd journal for this unit.")
@click.option("--session", help="Read a prompt session's durable log.")
@click.option("--pid", type=click.IntRange(1), help="Read a managed process's output.")
@click.option("-f", "follow", is_flag=True)
@extraclick.pass_service_client
def logs_vm(service, name, unit, session, follow, pid=None):
    """Read systemd, prompt session or managed process logs."""
    if sum(value is not None for value in (pid, unit, session)) > 1:
        raise click.UsageError("Choose one of --pid, --unit, or --session")
    if pid is not None:
        vm = _vm(service, name)
        if follow:
            with vm.keep_alive():
                for line in vm.process.get_process(pid).logs:
                    click.echo(line, nl=False)
        else:
            process = vm.process.get_process(pid)
            click.echo(process.stdout.read(), nl=False)
            click.echo(process.stderr.read(), nl=False, err=True)
        return
    if session:
        if not all(c.isalnum() or c in "-_" for c in session):
            raise click.UsageError("Invalid session")
        command = (
            "tail",
            "-n",
            "200",
            *(["-f"] if follow else []),
            f"/workspace/.beam-vm/logs/{session}.log",
        )
    else:
        command = (
            "journalctl",
            "--no-pager",
            "-n",
            "200",
            *(["-f"] if follow else []),
            *(["-u", unit] if unit else []),
        )
    exec_vm.callback.__wrapped__(service, name, command, "/workspace")
