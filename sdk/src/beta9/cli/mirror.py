"""Manage the files and settings this machine makes available to Beam VMs."""

import sys

import click

from .. import mirror, terminal
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
