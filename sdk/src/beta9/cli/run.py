import inspect
import shlex

import click

from .. import terminal
from ..abstractions.pod import Pod
from ..utils import load_module_spec
from .extraclick import (
    ClickCommonGroup,
    handle_config_override,
    override_config_options,
)


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="run",
    help="""
    Run a pod.

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
    "entrypoint",
    nargs=1,
    required=True,
)
@override_config_options
def run(
    entrypoint: str,
    **kwargs,
):
    pod_spec = None
    module = None
    try:
        pod_spec, module, _ = load_module_spec(entrypoint)

        if not inspect.isclass(type(pod_spec)) or pod_spec.__class__.__name__ != "Pod":
            terminal.error("Invalid handler function specified. Expected a Pod abstraction.")
    except BaseException:
        pod_spec = Pod(entrypoint=shlex.split(entrypoint))
        kwargs["entrypoint"] = pod_spec.entrypoint

    if not handle_config_override(pod_spec, kwargs):
        terminal.error("Failed to handle config overrides.")
        return

    if not module:
        # If there no module specified, we need to generate a module file for the pod
        pod_spec.generate_deployment_artifacts(**kwargs)

    if not pod_spec.create():
        terminal.error("Failed to create pod.")

    pod_spec.cleanup_deployment_artifacts()
