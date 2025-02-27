import functools
import inspect
import shlex
import sys
import textwrap
from gettext import gettext
from typing import Any, Callable, Dict, List, Optional

import click

from .. import terminal
from ..abstractions import base as base_abstraction
from ..abstractions.image import Image
from ..channel import ServiceClient, with_grpc_error_handling
from ..clients.gateway import (
    StringList,
)
from ..config import DEFAULT_CONTEXT_NAME, get_config_context
from ..utils import get_init_args_kwargs

CLICK_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"],
    show_default=True,
)

config_context_param = click.Option(
    param_decls=["-c", "--context"],
    default=DEFAULT_CONTEXT_NAME,
    required=False,
    help="The config context to use.",
    hidden=False,
)

config_context_option = click.option(
    "-c",
    "--context",
    required=config_context_param.required,
    help=config_context_param.help,
    hidden=config_context_param.hidden,
)


class Beta9Command(click.Command):
    def cli_name(self, ctx: click.Context) -> str:
        name, *_ = ctx.command_path.split()
        return name

    def format_epilog(self, ctx: click.Context, formatter: click.HelpFormatter):
        """
        Writes the epilog text to the formatter if it exists.
        """
        if not self.epilog:
            return

        name = self.cli_name(ctx)
        text = self.epilog.format(cli_name=name)
        text = textwrap.dedent(text)
        formatter.write(text)
        formatter.write("\n")

    def format_help_text(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """
        Writes the help text to the formatter if it exists.
        """
        if self.help is not None:
            # truncate the help text to the first form feed
            text = inspect.cleandoc(self.help).partition("\f")[0]
        else:
            text = ""

        if self.deprecated:
            text = gettext("(Deprecated) {text}").format(text=text)

        if text:
            name = self.cli_name(ctx)
            text = text.format(cli_name=name)

            formatter.write_paragraph()

            with formatter.indentation():
                text = textwrap.indent(text, " " * formatter.current_indent)
                formatter.write(text)
                formatter.write("\n")


class ClickCommonGroup(click.Group):
    command_class = Beta9Command

    def list_commands(self, ctx) -> List[str]:
        return list(self.commands)


class ClickManagementGroup(click.Group):
    command_class = Beta9Command

    def list_commands(self, ctx) -> List[str]:
        return list(self.commands)


class CommandGroupCollection(click.CommandCollection):
    def __init__(self, *args, **kwargs):
        params = kwargs.get("params", [])
        params.append(config_context_param)
        kwargs["params"] = params

        super().__init__(*args, **kwargs)

    def add_command(self, cmd: click.MultiCommand):
        """
        Alias method so it looks like a group.
        """
        return self.add_source(cmd)

    @property
    def sources_map(self) -> Dict[str, click.Group]:
        """
        A dictionary representation of {"command name": click_group}.
        """
        r = {}
        for source in self.sources:
            if not isinstance(source, click.Group):
                continue
            for command in source.commands:
                r[command] = source

        return r

    def invoke(self, ctx: click.Context) -> Any:
        if ctx.protected_args:
            if group := self.sources_map.get(ctx.protected_args[0]):
                group.invoke(ctx)
            else:
                print(self.get_help(ctx))
                sys.exit(1)
        else:
            super().invoke(ctx)

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """
        Extra format methods for multi methods that adds all the commands after
        the options.
        """
        commands = {}
        commands["common"] = []
        commands["management"] = []

        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            if cmd is None:
                continue
            if cmd.hidden:
                continue

            if isinstance(cmd, ClickManagementGroup):
                commands["management"].append((subcommand, cmd))
            else:
                commands["common"].append((subcommand, cmd))

        # sort the management commands
        commands["management"].sort(key=lambda x: x[0])

        for cmdtype, cmds in commands.items():
            if not len(cmds):
                continue

            limit = formatter.width - 6 - max(len(cmd[0]) for cmd in cmds)

            rows = []
            for subcommand, cmd in cmds:
                help = cmd.get_short_help_str(limit)
                rows.append((subcommand, help))

            if rows:
                with formatter.section(gettext(cmdtype.title() + " Commands")):
                    formatter.write_dl(rows)

    def list_commands(self, ctx):
        sources = []
        for source in self.sources:
            sources.extend(source.list_commands(ctx))
        return sources


def pass_service_client(func: Callable):
    """
    Decorator that sets a ServiceClient as the first argument.

    We take the right most --context option from the command and work
    our way left of each subcommand. If no --context option is found, we use
    the default value.
    """

    @config_context_option
    @functools.wraps(func)
    @with_grpc_error_handling
    def decorator(context: Optional[str] = None, *args, **kwargs):
        ctx = click.get_current_context()

        context = context or ctx.params.get("context", None)

        if context is None and hasattr(ctx, "parent") and hasattr(ctx.parent, "params"):
            context = ctx.parent.params.get("context", None)

        if (
            context is None
            and hasattr(ctx, "parent")
            and hasattr(ctx.parent, "parent")
            and hasattr(ctx.parent.parent, "params")
        ):
            context = ctx.parent.parent.params.get("context", "")

        config = get_config_context(context or DEFAULT_CONTEXT_NAME)

        with ServiceClient(config) as client:
            base_abstraction.set_channel(client.channel)

            return func(client, *args, **kwargs)

    return decorator


def filter_values_callback(
    ctx: click.Context,
    param: click.Option,
    values: List[str],
) -> Dict[str, StringList]:
    filters: Dict[str, StringList] = {}

    for value in values:
        key, value = value.split("=")
        value_list = value.split(",") if "," in value else [value]

        if key == "status":
            value_list = [v.upper() for v in value_list]

        if not key or not value:
            raise click.BadParameter("Filter must be in the format key=value")

        filters[key] = StringList(values=value_list)

    return filters


class ImageParser(click.ParamType):
    name = "base_image"

    def convert(self, value, param, ctx):
        return Image(
            base_image=value,
        )


class ShlexParser(click.ParamType):
    name = "shlex"

    def convert(self, value, param, ctx):
        if not value:
            return []
        return shlex.split(value)


class CommaSeparatedList(click.ParamType):
    name = "comma_separated_list"

    def __init__(self, type: click.ParamType):
        self.type = type

    def convert(self, value, param, ctx):
        if not value:
            return []
        values = value.split(",")
        return [self.type.convert(v, param, ctx) for v in values]


def override_config_options(func: click.Command):
    f = click.option(
        "--cpu",
        type=click.FLOAT,
        help="The amount of CPU to allocate (in cores, e.g. --cpu 0.5).",
        required=False,
    )(func)
    f = click.option(
        "--memory",
        type=click.STRING,
        help="The amount of memory to allocate (in MB).",
        required=False,
    )(f)
    f = click.option(
        "--gpu", type=click.STRING, help="The type of GPU to allocate.", required=False
    )(f)
    f = click.option(
        "--gpu-count",
        type=click.INT,
        help="The number of GPUs to allocate to the container.",
        required=False,
    )(f)
    f = click.option(
        "--secrets",
        type=CommaSeparatedList(click.STRING),
        help="The secrets to inject into the container (e.g. --secrets SECRET1,SECRET2).",
    )(f)
    f = click.option(
        "--ports",
        type=CommaSeparatedList(click.INT),
        help="The ports to expose inside the container (e.g. --ports 8000,8001).",
    )(f)
    f = click.option(
        "--entrypoint",
        type=ShlexParser(),
        help="The entrypoint for the container - only used if a handler is not provided.",
    )(f)
    f = click.option(
        "--image",
        type=ImageParser(),
        help="The image to use for the container (e.g. --image python:3.10).",
        required=False,
    )(f)
    f = click.option(
        "--env",
        type=click.STRING,
        multiple=True,
        help="Environment variables to pass to the container (e.g. --env VAR1=value --env VAR2=value).",
    )(f)
    f = click.option(
        "--keep-warm-seconds",
        type=click.INT,
        help="The number of seconds to keep the container up after the last request (e.g. --keep-warm-seconds 600).",
        required=False,
    )(f)
    return f


PARSE_CONFIG_PREFIX = "parse_"


def handle_config_override(func, kwargs: Dict[str, str]) -> bool:
    current_key = None
    try:
        config_class_instance = None
        if hasattr(func, "parent"):
            config_class_instance = func.parent
        else:
            config_class_instance = func

        # We only want to override the config if the config class has an __init__ method
        # For example, ports is only available on a Pod
        init_kwargs = get_init_args_kwargs(config_class_instance)

        for key, value in kwargs.items():
            current_key = key
            if value is not None and key in init_kwargs:
                if isinstance(value, tuple):
                    value = list(value)

                    if len(value) == 0:
                        continue

                if hasattr(config_class_instance, f"{PARSE_CONFIG_PREFIX}{key}"):
                    value = config_class_instance.__getattribute__(f"{PARSE_CONFIG_PREFIX}{key}")(
                        value
                    )

                setattr(config_class_instance, key, value)

        return True
    except BaseException as e:
        terminal.error(f"Invalid CLI argument ==> {current_key}: {e}", exit=False)
        return False
