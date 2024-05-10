import functools
import inspect
import os
import sys
import textwrap
from gettext import gettext
from pathlib import Path, PurePath, PurePosixPath
from typing import Any, Callable, ClassVar, Dict, List, Optional

import click

from ..abstractions import base as base_abstraction
from ..channel import ServiceClient
from ..clients.gateway import (
    StringList,
)
from ..config import DEFAULT_CONTEXT_NAME, get_config_context

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


class ClickManagementGroup(click.Group):
    command_class = Beta9Command


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


def pass_service_client(func: Callable):
    """
    Decorator that sets a ServiceClient as the first argument.

    We take the right most --context option from the command and work
    our way left of each subcommand. If no --context option is found, we use
    the default value.
    """

    @config_context_option
    @functools.wraps(func)
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


class RemotePath(PurePosixPath):
    supported_prefixes: ClassVar[List[str]] = [
        "vol",  # beta9
        "volume",  # beta9
        "s3",  # aws
        "gs",  # google
        "oci",  # oracle
        "az",  # azure
        "cos",  # ibm
    ]

    @property
    def prefix(self) -> str:
        return self._prefix

    @prefix.setter
    def prefix(self, value: str) -> None:
        if value not in self.supported_prefixes:
            raise ValueError("Unsupported path prefix")
        self._prefix = value

    def find_credentials(self):
        if self.prefix == "s3":
            access_key = os.getenv("AWS_ACCESS_KEY_ID", None)
            secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", None)
            session_token = os.getenv("AWS_SESSION_TOKEN", None)
            return access_key, secret_key, session_token


def validate_path_callback(ctx: click.Context, param: click.Parameter, value: str):
    path: PurePath
    parts = value.split("://", 1)

    if len(parts) < 1:
        raise click.BadParameter("Value not provided", ctx, param)
    if len(parts) == 1:
        path = Path(parts[0])
    elif len(parts) == 2:
        path = RemotePath(parts[1])
        try:
            path.prefix = parts[0]
        except ValueError:
            raise click.BadParameter(
                f"Prefix must be one of {'://, '.join(RemotePath.supported_prefixes)}://.",
                ctx,
                param,
            )
    else:
        raise click.BadParameter(
            "Well this is embarrassing. We don't know how you got here.",
            ctx,
            param,
        )

    return path
