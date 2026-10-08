"""Persistent CPU VMs. File/exec operations share the sandbox transport."""

import shlex
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
import webbrowser
from pathlib import Path
from contextlib import redirect_stdout
from urllib.parse import quote

import click
import websocket

from .. import terminal
from ..abstractions.image import Image
from ..abstractions.vm import VM, identity, prepare_image
from ..channel import GatewayHTTPError, ServiceClient
from ..config import SDKSettings, get_config_context, get_settings, set_settings
from ..logging import StoredStdoutInterceptor
from . import extraclick
from .extraclick import ClickManagementGroup


class VMGroup(ClickManagementGroup):
    def invoke(self, ctx):
        try:
            return super().invoke(ctx)
        except GatewayHTTPError as exc:
            raise click.ClickException(exc.message) from exc
        except (click.ClickException, click.exceptions.Exit):
            raise
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(f"{exc.cmd[0]} exited with status {exc.returncode}") from exc
        except (RuntimeError, TimeoutError, OSError) as exc:
            raise click.ClickException(str(exc)) from exc


@click.group(name="vm", cls=VMGroup, help="Create and manage persistent CPU microVMs.")
def management():
    pass


def _vm(service, name):
    try:
        return VM.get(name, _service=service)
    except GatewayHTTPError as exc:
        raise click.ClickException(exc.message) from exc


def _show(info, as_json):
    if as_json:
        terminal.print_json(info)
    elif isinstance(info, list):
        for record in info:
            click.echo(
                f"{record['name']}\t{record.get('status', record.get('kind', ''))}\t{record['id']}"
            )
    else:
        terminal.resource(
            info.get("name", "VM"),
            {
                key: value
                for key, value in info.items()
                if key
                in (
                    "id",
                    "status",
                    "error",
                    "container_id",
                    "terminal_url",
                    "desktop_url",
                    "root_snapshot_id",
                )
                and value
            },
        )


@management.command("new")
@click.argument("name", required=False)
@click.option(
    "--cpu", type=click.FloatRange(min=0.1), help="CPUs; defaults to 1, or 2 for desktop."
)
@click.option(
    "--memory",
    type=click.IntRange(min=256),
    help="RAM in MiB; defaults to 1024, or 2048 for desktop.",
)
@click.option("--disk-size", help="Durable root size; defaults to 50GiB.")
@click.option("--image", "image_uri", help="Base registry image; defaults to ubuntu:22.04.")
@click.option("--image-id", help="Existing image with Beam VM services installed.")
@click.option("--dockerfile", type=click.Path(exists=True, dir_okay=False))
@click.option("--build-context", type=click.Path(exists=True, file_okay=False))
@click.option("--build-secret", multiple=True)
@click.option("--template")
@click.option("--desktop", is_flag=True, default=None)
@click.option("--docker-enabled", is_flag=True, default=None)
@click.option("--env", multiple=True, metavar="KEY=VALUE")
@click.option("--secret", multiple=True)
@click.option("--port", multiple=True, type=click.IntRange(1, 65535))
@click.option("--no-ssh", is_flag=True)
@click.option("--sync", "sync_dir", type=click.Path(exists=True, file_okay=False))
@click.option(
    "--ttl",
    type=click.IntRange(min=0),
    help="Auto-stop after this many idle seconds; 0 disables it.",
)
@click.option("--pool")
@click.option("--json", "as_json", is_flag=True)
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
):
    if sum(bool(v) for v in (image_uri, image_id, dockerfile, template)) > 1:
        raise click.UsageError("Choose one of --image, --image-id, --dockerfile or --template")
    if build_context and not dockerfile:
        raise click.UsageError("--build-context requires --dockerfile")
    if no_ssh and sync_dir:
        raise click.UsageError("--sync requires SSH; remove --no-ssh")
    env_map = {}
    for assignment in env:
        key, separator, value = assignment.partition("=")
        if not separator or not key:
            raise click.UsageError("--env requires KEY=VALUE")
        env_map[key] = value
    image = (
        Image.from_dockerfile(dockerfile, build_context)
        if dockerfile
        else Image.from_id(image_id)
        if image_id
        else Image(base_image=image_uri or "ubuntu:22.04")
    )
    image.ignore_python = True
    image.with_secrets(list(build_secret))
    vm = VM(
        name,
        image=image,
        cpu=cpu,
        memory=memory,
        disk_size=disk_size,
        desktop=desktop,
        docker_enabled=docker_enabled,
        env=env_map if env else None,
        secrets=list(secret) if secret else None,
        ports=list(port) if port else None,
        ssh=False if no_ssh else None,
        ttl=ttl,
        pool=pool,
        template=template,
        _service=service,
    )
    with StoredStdoutInterceptor(capture_logs=as_json):
        vm.create()
        if sync_dir:
            _sync(vm, sync_dir, False)
    _show(vm.info, as_json)


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
    dockerfile = dockerfile or str(Path(build_context) / "Dockerfile")
    image = Image.from_dockerfile(dockerfile, build_context).with_secrets(list(build_secret))
    image.ignore_python = True
    with StoredStdoutInterceptor(capture_logs=as_json):
        result = prepare_image(image, service, desktop).build()
    if not result.success:
        raise click.ClickException(result.error or "VM image build failed")
    if as_json:
        terminal.print_json({"image_id": result.image_id, "desktop": desktop})
    else:
        click.echo(result.image_id)


@management.command("list")
@click.option("--all", "include_all", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def list_vms(service, include_all, as_json):
    _show(VM.list(all=include_all, _service=service), as_json)


@management.command("get")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def get_vm(service, name, as_json):
    _show(_vm(service, name).info, as_json)


@management.command("start")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def start_vm(service, name, as_json):
    _show(_vm(service, name).start().info, as_json)


management.add_command(start_vm, "resume")


@management.command("stop")
@click.argument("name")
@click.option("--no-snapshot", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def stop_vm(service, name, no_snapshot, as_json):
    _show(_vm(service, name).stop(no_snapshot).info, as_json)


@management.command("rm")
@click.argument("name")
@extraclick.pass_service_client
def remove_vm(service, name):
    _vm(service, name).remove()


@management.command("fork")
@click.argument("source")
@click.argument("name", required=False)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def fork_vm(service, source, name, as_json):
    for item in VM(_service=service)._api("GET", "/artifacts/snapshot"):
        if item["id"] == source:
            _show(VM(name, snapshot=source, _service=service).create().info, as_json)
            return
    _show(_vm(service, source).fork(name).info, as_json)


@management.command(
    "exec", context_settings={"ignore_unknown_options": True, "allow_interspersed_args": False}
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
@extraclick.pass_service_client
def exec_vm(service, name, command, cwd, timeout=0):
    from .container import exec_container

    if command[:1] == ("--",):
        command = command[1:]
    if not command:
        raise click.UsageError("A command is required")
    vm = _vm(service, name)
    vm._action("touch")
    # The existing exec implementation streams both output channels, retains
    # argv boundaries, cancellation and the child's exit code.
    exec_container.callback.__wrapped__(service, vm.info["container_id"], command, cwd, timeout)


def _url(service, name, field, open_url):
    vm = _vm(service, name)
    url = vm.info.get(field)
    if not url:
        raise click.ClickException(f"VM does not have {field.replace('_url', '')} enabled")
    click.echo(url)
    if open_url:
        webbrowser.open(url)


@management.command("desktop")
@click.argument("name")
@click.option("--open/--no-open", "open_url", default=True)
@click.option("--url", "url_only", is_flag=True, help="Print the URL without opening a browser.")
@extraclick.pass_service_client
def desktop_vm(service, name, open_url, url_only):
    _url(service, name, "desktop_url", open_url and not url_only)


@management.command("terminal")
@click.argument("name")
@click.option("--open/--no-open", "open_url", default=True)
@click.option("--url", "url_only", is_flag=True, help="Print the URL without opening a browser.")
@extraclick.pass_service_client
def terminal_vm(service, name, open_url, url_only):
    _url(service, name, "terminal_url", open_url and not url_only)


@management.command("ports")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def ports_vm(service, name, as_json):
    urls = _vm(service, name).info["urls"]
    if as_json:
        terminal.print_json(urls)
    else:
        for port, url in urls.items():
            click.echo(f"{port}\t{url}")


@management.command("expose")
@click.argument("name")
@click.argument("port", type=click.IntRange(1, 65535))
@extraclick.pass_service_client
def expose_vm(service, name, port):
    click.echo(_vm(service, name).expose(port))


@management.command("unexpose")
@click.argument("name")
@click.argument("port", type=click.IntRange(1, 65535))
@extraclick.pass_service_client
def unexpose_vm(service, name, port):
    _vm(service, name).unexpose(port)


def _socket(vm, port):
    url = vm._service.http.url(f"/api/v1/vm/{{ws}}/{quote(vm.id, safe='')}/tunnel/{port}")
    return websocket.create_connection(
        url.replace("https://", "wss://").replace("http://", "ws://"),
        header=vm._service.http.headers,
        timeout=30,
    )


def _bridge(vm, port, source, target):
    remote = _socket(vm, port)
    remote.settimeout(None)
    stopped = threading.Event()

    def upload():
        try:
            while not stopped.is_set():
                data = source.read1(65536) if hasattr(source, "read1") else source.read(65536)
                if not data:
                    remote.send("EOF")
                    return
                remote.send_binary(data)
        except (OSError, websocket.WebSocketException):
            stopped.set()
            remote.close()

    thread = threading.Thread(target=upload, daemon=True)
    thread.start()
    try:
        while not stopped.is_set():
            data = remote.recv()
            if not data:
                break
            if not isinstance(data, bytes):
                raise RuntimeError("Tunnel returned a non-binary frame")
            target.write(data)
            target.flush()
    except (OSError, websocket.WebSocketException):
        pass
    finally:
        stopped.set()
        remote.close()


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
    with redirect_stdout(sys.stderr):
        with ServiceClient(get_config_context(profile)) as service:
            _bridge(_vm(service, vm_id), port, sys.stdin.buffer, target)


def _ssh_options(vm):
    if not vm.info["spec"]["ssh"]:
        raise click.ClickException("SSH was disabled for this VM")
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
        f"HostKeyAlias=beam-vm-{vm.id}",
        "-o",
        f"ProxyCommand={proxy}",
    ]


@management.command(
    "ssh", context_settings={"ignore_unknown_options": True, "allow_interspersed_args": False}
)
@click.argument("name")
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
@extraclick.pass_service_client
def ssh_vm(service, name, command):
    if command[:1] == ("--",):
        command = command[1:]
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
    _sync(_vm(service, name), directory, watch)


@management.command("port-forward")
@click.argument("name")
@click.argument("port", metavar="PORT_OR_LOCAL:REMOTE")
@click.option("--local-port", type=click.IntRange(1, 65535))
@extraclick.pass_service_client
def forward_vm(service, name, port, local_port):
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
            _bridge(vm, port, source, target)

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
    _show(_vm(service, vm_name).snapshot(name), as_json)


@snapshot_group.command("list")
@click.argument("vm_name", required=False)
@click.option("--all", "include_all", is_flag=True)
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def snapshot_list(service, vm_name, include_all, as_json):
    items = VM(_service=service)._api("GET", "/artifacts/snapshot")
    if vm_name:
        vm = _vm(service, vm_name)
        items = [item for item in items if item["vm_id"] == vm.id]
    _show(items, as_json)


@management.group("template")
def template_group():
    """Save reusable private VM roots, independently of the source VM."""


@template_group.command("create")
@click.argument("vm_name")
@click.argument("name", required=False)
@click.option("-d", "--description", default="")
@click.option("--public", "public", is_flag=True, help="Reserved; complete VM roots are private.")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def template_create(service, vm_name, name, description, public, as_json):
    if public:
        raise click.UsageError("Public templates are unsupported; complete VM roots remain private")
    _show(_vm(service, vm_name).create_template(name, description), as_json)


@template_group.command("list")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def template_list(service, as_json):
    _show(VM(_service=service)._api("GET", "/artifacts/template"), as_json)


@template_group.command("show")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
@extraclick.pass_service_client
def template_show(service, name, as_json):
    for item in VM(_service=service)._api("GET", "/artifacts/template"):
        if item["name"] == name or item["id"] == name:
            _show(item, as_json)
            return
    raise click.ClickException("Template not found")


@template_group.command("rm")
@click.argument("name")
@extraclick.pass_service_client
def template_rm(service, name):
    VM(_service=service)._api("DELETE", "/artifacts/template/" + quote(name, safe=""))


@management.command("prompt")
@click.argument("name")
@click.argument("prompt")
@click.option("--agent", type=click.Choice(["claude", "codex"]), default="claude")
@click.option("--background", "--detach", is_flag=True)
@click.option("--cwd", default="/workspace")
@extraclick.pass_service_client
def prompt_vm(service, name, prompt, agent, background, cwd):
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
@click.option("-f", "follow", is_flag=True)
@extraclick.pass_service_client
def logs_vm(service, name, unit, session, follow):
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
