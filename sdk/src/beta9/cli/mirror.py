"""Manage the files and settings this machine makes available to Beam VMs."""

import socket
import sys
import time
from contextlib import suppress

import click

from .. import mirror, terminal
from ..abstractions.vm.desktop_tunnel import share_desktop
from . import extraclick, vm as vm_cli


@click.group(cls=extraclick.ClickCommonGroup)
def common(**_):
    pass


@common.group("mirror", cls=extraclick.ClickCommonGroup)
@extraclick.config_context_option
def mirror_group(context):
    """Choose local files to sync, then launch or reopen a VM."""


def _selection():
    context = extraclick.selected_context()
    command = extraclick.command_hint(context)
    try:
        if mirror.installed():
            current = mirror.local_status(context)
            if current.get("mirror"):
                return current
    except mirror.MirrorError as exc:
        raise click.ClickException(f"{exc} Run: {command} setup mirror") from exc
    raise click.ClickException(f"Set up a mirror first. Run: {command} setup mirror")


def _utility_command(name, help_text):
    @mirror_group.command(
        name,
        help=help_text,
        add_help_option=False,
        context_settings={"ignore_unknown_options": True},
    )
    @click.argument("args", nargs=-1, type=click.UNPROCESSED)
    def invoke(args):
        if name not in ("status", "doctor") and not {"--help", "-h"}.intersection(args):
            _selection()
        try:
            result = mirror.run(name, *args)
        except mirror.MirrorError as exc:
            raise click.ClickException(str(exc)) from exc
        raise click.exceptions.Exit(result)


for subcommand, description in (
    ("suggest", "Review settings and select individual files with --add."),
    ("add", "Choose files or folders under your home."),
    ("rm", "Remove a selected path."),
    ("status", "Show selected paths, uploads and the latest local generation."),
    ("push", "Upload changes in selected paths."),
    ("run", "Keep changes in selected paths syncing."),
    ("tray", "Open the macOS menu bar app."),
    ("pause", "Pause uploads."),
    ("resume", "Resume uploads."),
    ("restore", "Restore a generation to an empty directory."),
    ("doctor", "Diagnose the mirror on this machine."),
):
    _utility_command(subcommand, description)


@mirror_group.command("launch")
@click.argument("name", required=False)
@click.option("--pool", metavar="NAME", help="MicroVM worker pool.")
@click.option(
    "--open/--no-open", "open_desktop", default=True, help="Open the desktop in a browser."
)
@click.option("--json", "as_json", is_flag=True, help="Print JSON for automation.")
@click.pass_context
def launch(ctx, name, pool, open_desktop, as_json):
    """Upload selected files and launch a new private desktop VM."""
    current = _selection()
    command = extraclick.command_hint()
    if not current.get("roots"):
        raise click.ClickException(
            f"Choose files first with {command} mirror suggest or {command} mirror add PATH."
        )
    if current.get("paused"):
        raise click.ClickException(f"Uploads are paused. Run: {command} mirror resume")
    as_json = as_json or terminal.json_output()
    try:
        result = mirror.run("push", stdout=sys.stderr if as_json else None)
    except mirror.MirrorError as exc:
        raise click.ClickException(str(exc)) from exc
    if result:
        raise click.ClickException("Mirror upload failed. Resolve the error above and retry.")
    ctx.invoke(
        vm_cli.new,
        name=name,
        pool=pool,
        mirror=current["mirror"],
        desktop=True,
        as_json=as_json,
        open_desktop=open_desktop,
    )


@mirror_group.command("open")
@click.argument("name")
@click.pass_context
def open_vm(ctx, name):
    """Reopen the same private desktop VM and start it if needed."""
    ctx.invoke(vm_cli.desktop_vm, name=name, resume=True, open_url=True, url_only=False)


@mirror_group.command("share-desktop")
@click.argument("name")
@click.option("--protocol", required=True, type=click.Choice(["vnc", "rdp"]))
@click.option(
    "--local-port", type=click.IntRange(1, 65535), help="Desktop-sharing port on this machine."
)
@click.option(
    "--open/--no-open", "open_url", default=True, help="Open the VM desktop in a browser."
)
@extraclick.pass_service_client
def share_host_desktop(service, name, protocol, local_port, open_url):
    """Control a local desktop you have chosen to share from a private VM."""
    vm = vm_cli._vm(service, name)
    spec = vm.info.get("spec") or {}
    if (
        not spec.get("desktop")
        or not spec.get("desktop_transport")
        or spec.get("kiosk") is not None
        or 8080 not in (spec.get("protected_ports") or [])
    ):
        raise click.ClickException(
            "Desktop sharing requires a full VM desktop on protected port 8080."
        )

    local_port = local_port or (5900 if protocol == "vnc" else 3389)
    try:
        with socket.create_connection(("127.0.0.1", local_port), timeout=3):
            pass
    except OSError as exc:
        raise click.ClickException(
            f"Start your local {protocol.upper()} sharing server on 127.0.0.1:{local_port} and retry."
        ) from exc

    process = None
    try:
        ssh_options = vm_cli._ssh_options(vm)
        configuration = vm.process.exec("/usr/sbin/sshd", "-T", cwd="/")
        if (
            "gatewayports no"
            not in vm._guest_output(
                configuration, error="Could not verify VM SSH forwarding settings.", timeout=15
            ).splitlines()
        ):
            raise click.ClickException(
                "The VM SSH server must enforce loopback-only forwarding (GatewayPorts no)."
            )
        viewer = _desktop_viewer(vm, protocol)
        terminal.detail(
            "This share grants the VM access to the desktop provided by your local sharing server."
        )
        with share_desktop(ssh_options, "root@" + vm.name, local_port) as (port, connection):
            if protocol == "vnc":
                command = (
                    viewer,
                    "-AcceptClipboard=0",
                    "-SendClipboard=0",
                    "-ReconnectOnError=0",
                    "-SecurityTypes=VncAuth,TLSVnc,X509Vnc",
                    f"127.0.0.1::{port}",
                )
            else:
                command = (
                    "/usr/bin/gnome-terminal",
                    "--wait",
                    "--",
                    viewer,
                    "-clipboard",
                    f"/v:127.0.0.1:{port}",
                )
            process = vm.desktop.launch(*command)
            vm_cli._url(vm, "desktop_url", open_url)
            terminal.detail("Close the viewer or press Ctrl+C here to end sharing.")
            while connection.poll() is None:
                code, _ = process.status()
                if code >= 0:
                    process = None
                    if code:
                        raise click.ClickException(
                            "The desktop viewer exited. Check your local sharing server and viewer settings."
                        )
                    return
                time.sleep(1)
            raise click.ClickException(
                "The private SSH connection closed. Run the command again to reconnect."
            )
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    except KeyboardInterrupt:
        terminal.detail("Desktop share disconnected.")
        raise click.exceptions.Exit(130) from None
    finally:
        if process is not None:
            with suppress(Exception):
                process.kill()


def _desktop_viewer(vm, protocol):
    lookup, package = (
        ("command -v xtigervncviewer", "tigervnc-viewer")
        if protocol == "vnc"
        else (
            "command -v xfreerdp3 || command -v xfreerdp",
            "freerdp3-x11 (Ubuntu 24.04) or freerdp2-x11 (Ubuntu 22.04)",
        )
    )
    process = vm.process.exec("/bin/sh", "-c", lookup, cwd="/")
    return vm._guest_output(process, error=f"Install {package} in the VM and retry.", timeout=15)
