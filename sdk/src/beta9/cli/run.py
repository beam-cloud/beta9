import inspect

import click

from .. import terminal
from ..abstractions.base.container import Container
from ..abstractions.base.runner import RUNTIME_PREPARE_FAILED_MSG
from ..abstractions.pod import Pod, PodInstance
from ..channel import ServiceClient
from ..logging import StoredStdoutInterceptor
from ..utils import load_module_spec
from .extraclick import (
    ClickCommonGroup,
    command_hint,
    handle_config_override,
    override_config_options,
    pass_service_client,
    selected_context,
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

        {cli_name} run app.py:pod
        {cli_name} run app.py:pod --detach --json
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
@click.option(
    "--json", "json_output", is_flag=True, help="Return detached container metadata as JSON."
)
@pass_service_client
def run(
    _: ServiceClient,
    handler: str,
    sync: bool,
    detach: bool,
    machine_id: str,
    json_output: bool,
    **kwargs,
):
    if sync and detach:
        raise click.UsageError("--sync cannot be used with --detach.")
    if json_output and not detach:
        raise click.UsageError("--json requires --detach.")

    with StoredStdoutInterceptor(capture_logs=json_output):
        pod_spec, result = _create_pod(handler, machine_id, kwargs)

    if json_output:
        terminal.print_json(
            {
                "container_id": result.container_id,
                "task_id": result.task_id,
                "stub_id": pod_spec.stub_id,
                "app_id": result.app_id,
                "context": selected_context(),
                "status": "submitted",
            }
        )
        return

    if detach:
        _print_detached_run(result, pod_spec)
        return

    if app_url := _app_dashboard_url(result.app_id):
        terminal.url(app_url)
    try:
        Container(container_id=result.container_id).attach(
            container_id=result.container_id, sync_dir="./" if sync else None
        )
    except KeyboardInterrupt:
        terminal.print()
        _print_detached_run(result, pod_spec)


def _create_pod(handler, machine_id, kwargs):
    entrypoint = kwargs.get("entrypoint")
    if handler:
        pod_spec, _, _ = load_module_spec(handler, "run")

        if not inspect.isclass(type(pod_spec)) or pod_spec.__class__.__name__ != "Pod":
            terminal.error("Invalid handler function specified. Expected a Pod abstraction.")

    else:
        pod_spec = Pod(entrypoint=entrypoint)

    if not handle_config_override(pod_spec, kwargs):
        raise click.exceptions.Exit(1)

    result: PodInstance = pod_spec.create(machine_id=machine_id)
    if not result.ok:
        if result.error_msg == RUNTIME_PREPARE_FAILED_MSG:
            # prepare_runtime already reported the specific failure. Preserve
            # that message without printing the generic error again, but make
            # sure scripts and CI still receive a failing exit status.
            raise click.exceptions.Exit(1)
        terminal.error(result.error_msg or "Failed to create container.")
        return

    return pod_spec, result


def _app_dashboard_url(app_id: str) -> str:
    from ..config import get_settings

    template = get_settings().app_url_template
    if not app_id or not template:
        return ""
    return template.format(app_id=app_id)


def _print_detached_run(result: PodInstance, pod_spec: Pod) -> None:
    cli_name = command_hint()
    terminal.resource(
        "Detached · container keeps running",
        {
            "Dashboard": _app_dashboard_url(result.app_id),
            "Logs": f"{cli_name} logs --container-id {result.container_id} --follow",
            "Attach": f"{cli_name} container attach {result.container_id}",
            "Stop": f"{cli_name} container stop {result.container_id}",
        },
    )

    pool = getattr(pod_spec, "pool_config", None)
    if pool is not None and pool.name:
        terminal.detail(f"  hardware:  {cli_name} machine release --pool {pool.name}")
