import click

from ..abstractions.pod import Pod
from ..channel import ServiceClient
from ..cli import extraclick
from ..utils import load_module_spec
from .extraclick import ClickCommonGroup, handle_config_override, override_config_options


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="dev",
    help="""
    Spins up a remote environment to develop in. Will automatically sync the current directory to the container.
    You can optionally specify a handler to use as the base environment, or leave it blank to use a default environment.
    """,
    epilog="""
      Examples:

        {cli_name} dev app.py:handler

        {cli_name} dev app.py:my_func --gpu T4

        {cli_name} dev --sync ./newproj
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
    help="The directory to sync to the container",
    default="./",
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@override_config_options
@extraclick.pass_service_client
@click.pass_context
def dev(
    ctx: click.Context,
    service: ServiceClient,
    handler: str,
    sync: str,
    url_type: str = "path",
    **kwargs,
):
    entrypoint = kwargs["entrypoint"]
    if handler:
        user_obj, module_name, obj_name = load_module_spec(handler, "dev")

        if hasattr(user_obj, "set_handler"):
            user_obj.set_handler(f"{module_name}:{obj_name}")
    elif entrypoint:
        user_obj = Pod(entrypoint=entrypoint)
    else:
        user_obj = Pod()

    if not handle_config_override(user_obj, kwargs):
        return

    user_obj.shell(url_type=url_type, sync_dir=sync)  # type:ignore
