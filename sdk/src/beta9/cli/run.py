import importlib
import inspect
import os
import shlex
import sys
from pathlib import Path

import click

from .. import terminal
from ..abstractions.pod import Pod
from .extraclick import ClickCommonGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="run",
    help="""
    Run a pod.

    The specfile is a YAML file that contains the configuration for the pod.
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
    "specfile",
    nargs=1,
    required=False,
)
@click.option(
    "--image",
    help="The image to use for the pod.",
    type=str,
)
@click.option(
    "--gpu",
    help="The GPU to use for the pod.",
    type=str,
)
@click.option(
    "--cpu",
    help="The CPU to use for the pod.",
    type=str,
)
@click.option(
    "--memory",
    help="The memory to use for the pod.",
    type=str,
)
@click.option(
    "--entrypoint",
    help="The entrypoint to use for the pod.",
    type=str,
)
def run(
    specfile: str,
    image: str,
    gpu: str,
    cpu: str,
    memory: str,
    entrypoint: str,
):
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    pod_spec = None
    if specfile:
        # TODO: clean this up
        module_path, obj_name, *_ = specfile.split(":") if ":" in specfile else (specfile, "")
        module_name = module_path.replace(".py", "").replace(os.path.sep, ".")

        if not Path(module_path).exists():
            terminal.error(f"Unable to find file: '{module_path}'")

        if not obj_name:
            terminal.error(
                "Invalid handler function specified. Expected format: beam serve [file.py]:[function]"
            )

        module = importlib.import_module(module_name)

        pod_spec = getattr(module, obj_name, None)
        if pod_spec is None:
            terminal.error(
                f"Invalid handler function specified. Make sure '{module_path}' contains the function: '{obj_name}'"
            )

        if not inspect.isclass(type(pod_spec)) and pod_spec.__class__.__name__ != "Pod":
            terminal.error("Invalid handler function specified. Expected a Pod abstraction.")

    if pod_spec is None:
        if not entrypoint:
            terminal.error("No entrypoint specified.")

        pod_spec = Pod(entrypoint=shlex.split(entrypoint))

    if image:
        pod_spec.image.base = image

    if gpu:
        pod_spec.gpu = gpu

    if cpu:
        pod_spec.cpu = cpu

    if memory:
        pod_spec.memory = pod_spec.parse_memory(memory)

    pod_spec.run()
