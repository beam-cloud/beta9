import inspect

import click

from .. import terminal
from ..abstractions.base.container import Container
from ..abstractions.base.runner import RUNTIME_PREPARE_FAILED_MSG
from ..abstractions.pod import Pod, PodInstance
from ..channel import ServiceClient
from ..utils import load_module_spec
from .extraclick import (
    ClickCommonGroup,
    handle_config_override,
    override_config_options,
    pass_service_client,
)


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="run",
    help="""
    Run a container.

    """,
    epilog="""
      Examples:

        {cli_name} run app.py:handler

        {cli_name} run app.py:my_func

        {cli_name} run --image python:3.10 --gpu T4
        \b
    """,
)
@click.argument(
    "handler",
    nargs=1,
    required=False,
)
@click.option(
    "--sync",
    is_flag=True,
    default=False,
    help="Recursively sync the current directory to the container and watch for changes",
)
@click.option(
    "--detach",
    is_flag=True,
    default=False,
    help="Submit the container and return without streaming logs.",
)
@click.option(
    "--machine",
    "machine_id",
    type=str,
    default="",
    help="Run on a specific reserved machine.",
)
@override_config_options
@pass_service_client
def run(
    _: ServiceClient,
    handler: str,
    sync: bool,
    detach: bool,
    machine_id: str,
    **kwargs,
):
    if sync and detach:
        terminal.error("--sync cannot be used with --detach.")

    entrypoint = kwargs.get("entrypoint")
    if handler:
        pod_spec, _, _ = load_module_spec(handler, "run")

        if not inspect.isclass(type(pod_spec)) or pod_spec.__class__.__name__ != "Pod":
            terminal.error("Invalid handler function specified. Expected a Pod abstraction.")

    else:
        pod_spec = Pod(entrypoint=entrypoint)

    if not handle_config_override(pod_spec, kwargs):
        return

    result: PodInstance = pod_spec.create(machine_id=machine_id)
    if not result.ok:
        if result.error_msg == RUNTIME_PREPARE_FAILED_MSG:
            return  # prepare_runtime already reported the specific failure
        terminal.error(result.error_msg or "Failed to create container.")
        return

    container = Container(container_id=result.container_id)

    if detach:
        _print_detached_run(result, pod_spec)
        return

    # Print the app/task links up front so the run can be tracked on the
    # dashboard even while attached (or after detaching with Ctrl+C)
    _print_run_links(result)

    sync_dir = None
    if sync:
        sync_dir = "./"
    else:
        sync_dir = None

    try:
        container.attach(container_id=result.container_id, sync_dir=sync_dir)
    except KeyboardInterrupt:
        terminal.print()
        _print_detached_run(result, pod_spec)
        raise SystemExit(0)


def _get_cli_name() -> str:
    return click.get_current_context().command_path.split()[0]


def _app_dashboard_url(app_id: str) -> str:
    from ..config import get_settings

    template = get_settings().app_url_template
    if not app_id or not template:
        return ""
    return template.format(app_id=app_id)


def _print_run_links(result: PodInstance) -> None:
    cli_name = _get_cli_name()
    if app_url := _app_dashboard_url(result.app_id):
        terminal.detail(f"  app:       {app_url}")
    if result.task_id:
        terminal.detail(f"  task:      {result.task_id} ({cli_name} task list)")


def _print_detached_run(result: PodInstance, pod_spec: Pod) -> None:
    cli_name = _get_cli_name()
    terminal.success(f"Detached; container {result.container_id} keeps running")
    _print_run_links(result)
    if result.management_url:
        terminal.detail(f"  dashboard: {result.management_url}")
    terminal.detail(f"  shell:     {cli_name} shell --container-id {result.container_id}")
    terminal.detail(f"  reattach:  {cli_name} container attach {result.container_id}")
    terminal.detail(f"  stop:      {cli_name} container stop {result.container_id}")

    pool = getattr(pod_spec, "pool_config", None)
    if pool is not None and pool.name:
        terminal.detail(f"  hardware:  {cli_name} machine release --pool {pool.name}")
