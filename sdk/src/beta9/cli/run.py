import inspect

import click

from .. import terminal
from ..abstractions.pod import Pod, PodInstance
from ..channel import ServiceClient
from ..utils import load_module_spec
from .container import _attach_to_container
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
@override_config_options
@pass_service_client
def run(
    service: ServiceClient,
    handler: str,
    **kwargs,
):
    entrypoint = kwargs["entrypoint"]
    if handler:
        pod_spec, _, _ = load_module_spec(handler, "run")

        if not inspect.isclass(type(pod_spec)) or pod_spec.__class__.__name__ != "Pod":
            terminal.error("Invalid handler function specified. Expected a Pod abstraction.")

    elif entrypoint:
        pod_spec = Pod(entrypoint=entrypoint)

    else:
        terminal.error("No handler or entrypoint specified.")
        return

    if not handle_config_override(pod_spec, kwargs):
        return

    result: PodInstance = pod_spec.create()
    if not result.ok:
        terminal.error("Failed to create container.")
        return

    _attach_to_container(service, result.container_id)
