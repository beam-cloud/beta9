import functools
import inspect
import os
import shlex
import textwrap
from gettext import gettext
from typing import Any, Callable, Dict, List, Optional

import click

from .. import terminal
from ..abstractions import base as base_abstraction
from ..abstractions.image import Image
from ..channel import ServiceClient, with_grpc_error_handling
from ..clients.gateway import (
    SecretVar,
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
    default=None,
    required=config_context_param.required,
    help=config_context_param.help,
    hidden=config_context_param.hidden,
)


def set_cli_flag(ctx, param, value):
    if value:
        key = f"BETA9_{param.name.upper()}"
        previous = os.environ.get(key)

        def restore():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous

        ctx.call_on_close(restore)
        os.environ[key] = "1"


class HelpFormatter(click.HelpFormatter):
    def write_heading(self, heading):
        super().write_heading(click.style(heading, fg="cyan", bold=True))

    def write_dl(self, rows, **kwargs):
        super().write_dl([(click.style(name, bold=True), text) for name, text in rows], **kwargs)


class Context(click.Context):
    formatter_class = HelpFormatter

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "NO_COLOR" in os.environ or os.getenv("TERM") == "dumb":
            self.color = False


def show_all_help(ctx, param, value):
    if value and not ctx.resilient_parsing:
        ctx.meta["help_all"] = True
        click.echo(ctx.get_help(), color=ctx.color)
        ctx.exit()


class Beta9Command(click.Command):
    context_class = Context
    common_options = {
        "name": "Name of the workload.",
        "image": "Base image, such as python:3.12.",
        "dockerfile": "Build from a Dockerfile.",
        "entrypoint": "Command to run when no handler is provided.",
        "cpu": "CPU cores, such as 0.5 or 2.",
        "memory": "Memory, such as 512Mi or 2Gi.",
        "gpu": "GPU type, such as T4.",
        "port": "Port to expose; repeat for multiple ports.",
        "env": "Environment variable; repeat for multiple values.",
        "detach": "Submit and return without attaching to logs.",
        "replicas": "Fixed number of service replicas.",
        "json_output": "Print the operation result as JSON.",
        "context": "Saved connection profile.",
        "help": "Show common options and examples.",
        "help_all": "Show all options, including scaling and advanced settings.",
    }

    def get_params(self, ctx):
        params = list(super().get_params(ctx))
        if self.name in ("run", "deploy"):
            params.append(
                click.Option(
                    ["--help-all"],
                    is_flag=True,
                    is_eager=True,
                    expose_value=False,
                    callback=show_all_help,
                    help="Show every option, including scaling and advanced settings.",
                )
            )
        return params

    def cli_name(self, ctx: click.Context) -> str:
        name, *_ = ctx.command_path.split()
        return name

    def format_options(self, ctx, formatter):
        groups = {"Options": [], "Additional options": []}
        for param in self.get_params(ctx):
            record = param.get_help_record(ctx)
            if record:
                additional = (
                    self.name in ("run", "deploy") and param.name not in self.common_options
                )
                if additional and not ctx.meta.get("help_all"):
                    continue
                if self.name in ("run", "deploy") and not ctx.meta.get("help_all"):
                    record = (record[0], self.common_options[param.name])
                groups["Additional options" if additional else "Options"].append(record)
        for name, records in groups.items():
            if records:
                with formatter.section(name):
                    formatter.write_dl(records)

    def format_epilog(self, ctx: click.Context, formatter: click.HelpFormatter):
        """
        Writes the epilog text to the formatter if it exists.
        """
        if not self.epilog:
            return

        name = self.cli_name(ctx)
        text = self.epilog.format(cli_name=name)
        text = textwrap.dedent(text).replace("\b", "").strip()
        formatter.write_paragraph()
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
                formatter.write_text(text)


class ClickCommonGroup(click.Group):
    command_class = Beta9Command
    context_class = Context

    def list_commands(self, ctx) -> List[str]:
        return list(self.commands)


class ClickManagementGroup(click.Group):
    command_class = Beta9Command
    context_class = Context

    def list_commands(self, ctx) -> List[str]:
        return list(self.commands)


class CommandGroupCollection(click.CommandCollection):
    context_class = Context

    def __init__(self, *args, **kwargs):
        params = kwargs.get("params", [])
        params.append(config_context_param)
        for name in ("no-input", "verbose"):
            params.append(
                click.Option(
                    [f"--{name}"],
                    is_flag=True,
                    expose_value=False,
                    callback=set_cli_flag,
                    help="Disable prompts."
                    if name == "no-input"
                    else "Show SDK diagnostic details.",
                )
            )
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
        if not ctx.protected_args:
            return super().invoke(ctx)
        group = self.sources_map.get(ctx.protected_args[0])
        if group is None:
            ctx.fail(f"No such command '{ctx.protected_args[0]}'.")
        return group.invoke(ctx)

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        groups = {
            "Work": "run deploy dev serve shell logs doctor".split(),
            "Resources": "container deployment image task volume disk db ls cp rm mv".split(),
        }
        remaining = set(self.list_commands(ctx)) - {
            name for names in groups.values() for name in names
        }
        groups["Settings & infrastructure"] = sorted(remaining)
        summaries = {
            "dev": "Develop with live file sync.",
            "shell": "Open a shell in a container.",
            "logs": "Read or follow workload logs.",
            "doctor": "Check your connection and credentials.",
            "disk": "Manage durable disks.",
            "mv": "Move files within a volume.",
            "machine": "Browse and manage machines.",
            "pool": "Manage groups of machines.",
            "worker": "Inspect and maintain workers.",
        }
        for title, names in groups.items():
            rows = [
                (name, summaries.get(name) or command.get_short_help_str(formatter.width - 16))
                for name in names
                if (command := self.get_command(ctx, name)) is not None and not command.hidden
            ]
            if rows:
                with formatter.section(title):
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

        config = get_config_context(context or selected_context(ctx))

        with ServiceClient(config) as client:
            base_abstraction.set_channel(client.channel)
            try:
                return func(client, *args, **kwargs)
            finally:
                base_abstraction.unset_channel()

    return decorator


def selected_context(ctx: Optional[click.Context] = None) -> str:
    ctx = ctx or click.get_current_context()
    while ctx is not None:
        if ctx.params.get("context"):
            return ctx.params["context"]
        ctx = ctx.parent
    return DEFAULT_CONTEXT_NAME


def command_hint() -> str:
    ctx = click.get_current_context()
    return f"{ctx.command_path.split()[0]} --context {shlex.quote(selected_context(ctx))}"


def filter_values_callback(
    ctx: click.Context,
    param: click.Option,
    values: List[str],
) -> Dict[str, StringList]:
    filters: Dict[str, StringList] = {}

    for value in values:
        key, _, value = value.partition("=")
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


class DockerfileParser(click.ParamType):
    name = "dockerfile"

    def convert(self, value, param, ctx):
        if not os.path.exists(value):
            terminal.error(f"Dockerfile not found: {value}")
            return None

        return value


def image_from_dockerfile_option(value) -> Image:
    if isinstance(value, Image):
        return value

    image = Image.from_dockerfile(str(value))
    image.dockerfile_path = str(value)
    image.ignore_python = True
    return image


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
        "--port",
        type=click.INT,
        multiple=True,
        help="Expose a single container port. Can be provided multiple times.",
    )(f)
    f = click.option(
        "--entrypoint",
        "--command",
        "entrypoint",
        type=ShlexParser(),
        help="The command for the container - only used if a handler is not provided.",
    )(f)
    f = click.option(
        "--dockerfile",
        type=DockerfileParser(),
        help="The path to the Dockerfile to use for the container (e.g. --dockerfile Dockerfile).",
        required=False,
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
        "--pool",
        type=click.STRING,
        help="Run the container on a private pool (e.g. --pool web-cpu).",
        required=False,
    )(f)
    f = click.option(
        "--keep-warm-seconds",
        type=click.INT,
        help="Seconds to retain idle containers. Use 0 for immediate scale-to-zero; -1 keeps idle containers warm indefinitely where supported.",
        required=False,
    )(f)
    f = click.option(
        "--checkpoint-enabled",
        is_flag=True,
        default=None,
        show_default=False,
        help="Enable checkpoint/restore for supported workloads.",
    )(f)
    f = click.option(
        "--checkpoint-readiness-path",
        type=click.STRING,
        help="HTTP path that must return 200 before creating an automatic checkpoint.",
        required=False,
    )(f)
    f = click.option(
        "--checkpoint-readiness-port",
        type=click.INT,
        help="Container port for checkpoint HTTP readiness.",
        required=False,
    )(f)
    f = click.option(
        "--checkpoint-readiness-timeout",
        type=click.INT,
        default=None,
        show_default="600",
        help="Seconds to wait for checkpoint HTTP readiness.",
    )(f)
    f = click.option(
        "--checkpoint-readiness-interval",
        type=click.INT,
        default=None,
        show_default="1",
        help="Seconds between checkpoint HTTP readiness probes.",
    )(f)
    f = click.option(
        "--min-replicas",
        "--min-containers",
        type=click.IntRange(min=0),
        help="Minimum service replicas to keep running.",
        required=False,
    )(f)
    f = click.option(
        "--max-replicas",
        "--max-containers",
        type=click.IntRange(min=1),
        help="Maximum service replicas to run.",
        required=False,
    )(f)
    f = click.option(
        "--always-on/--scale-to-zero",
        default=None,
        help="Keep at least one service replica running, or explicitly allow scale-to-zero.",
    )(f)
    f = click.option(
        "--tcp",
        help="Enable raw TCP-proxying [only available for Pods]",
        is_flag=True,
        default=False,
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
                if isinstance(value, (list, tuple)):
                    value = list(value)

                    if len(value) == 0:
                        continue

                if key == "env":
                    existing_env = env_vars_to_dict(getattr(config_class_instance, "env", []))
                    existing_env.update(env_vars_to_dict(value))
                    setattr(
                        config_class_instance,
                        "env",
                        [f"{k}={v}" for k, v in existing_env.items()],
                    )
                    continue

                if key == "secrets":
                    secrets = [value] if isinstance(value, str) else value
                    setattr(
                        config_class_instance,
                        "secrets",
                        [SecretVar(name=str(secret)) for secret in secrets],
                    )
                    continue

                if key == "pool" and hasattr(config_class_instance, "parse_pool"):
                    setattr(config_class_instance, "pool", value)
                    setattr(
                        config_class_instance,
                        "pool_config",
                        config_class_instance.parse_pool(value),
                    )
                    continue

                if hasattr(config_class_instance, f"{PARSE_CONFIG_PREFIX}{key}"):
                    value = config_class_instance.__getattribute__(f"{PARSE_CONFIG_PREFIX}{key}")(
                        value
                    )

                setattr(config_class_instance, key, value)

        replica_args = {
            key: kwargs[key]
            for key in ("min_replicas", "max_replicas", "always_on")
            if kwargs.get(key) is not None
        }
        if replica_args and hasattr(config_class_instance, "configure_replicas"):
            config_class_instance.configure_replicas(**replica_args)

        if kwargs.get("dockerfile") is not None:
            image = image_from_dockerfile_option(kwargs["dockerfile"])
            kwargs["dockerfile"] = image
            config_class_instance.image = image

        return True
    except BaseException as e:
        terminal.error(f"Invalid CLI argument ==> {current_key}: {e}", exit=False)
        return False


def env_vars_to_dict(value) -> Dict[str, str]:
    if not value:
        return {}
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}

    env: Dict[str, str] = {}
    for item in value:
        key, sep, raw_value = str(item).partition("=")
        if not sep or not key:
            raise ValueError("env must be in KEY=value format")
        env[key] = raw_value
    return env
