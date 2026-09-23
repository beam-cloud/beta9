"""
`db` commands: managed Postgres, Redis, MySQL and MongoDB services. The
gateway owns provisioning (`/api/v1/database`); this module only shapes
requests, reads the credential secrets back, and prints.
"""

import os
import shlex
import sys
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import click
from rich.table import Column, Table, box

from .. import terminal
from ..channel import GatewayHTTPError, ServiceClient
from ..clients.gateway import ListDeploymentsRequest, ScaleDeploymentRequest, StringList
from ..clients.secret import GetSecretRequest
from . import extraclick, stubconfig
from .extraclick import ClickCommonGroup

PRODUCTS = {"postgres": "Postgres", "redis": "Redis", "mysql": "MySQL", "mongo": "MongoDB"}

FORMAT_OPTION = click.option(
    "--format", type=click.Choice(("table", "json")), default="table", show_default=True
)


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="db", help="Create and manage database services.")
def db():
    pass


# --- gateway ------------------------------------------------------------------


def _api(service: ServiceClient, method: str, path: str = "", **kwargs) -> Any:
    try:
        return service.http.json(method, f"/api/v1/database/{{ws}}{path}", timeout=660, **kwargs)
    except GatewayHTTPError as exc:
        raise click.ClickException(str(exc))


def _services(service: ServiceClient) -> List[Dict[str, Any]]:
    return _api(service, "GET") or []


def _service_info(service: ServiceClient, kind: str, name: str) -> Dict[str, Any]:
    for info in _services(service):
        if info["name"] == name and info["kind"] == kind:
            return info
    raise click.ClickException(f"{kind} service {name!r} not found.")


def _secret_value(service: ServiceClient, name: str) -> str:
    res = service.secret.get_secret(GetSecretRequest(name=name))
    if not res.ok:
        raise click.ClickException(res.err_msg or f"Secret {name} not found.")
    return res.secret.value


def _deployments_by_name(service: ServiceClient, name: str):
    res = service.gateway.list_deployments(
        ListDeploymentsRequest(filters={"name": StringList(values=[name])}, limit=20)
    )
    if not res.ok:
        raise click.ClickException(res.err_msg or "Unable to list deployments.")
    return [d for d in res.deployments if d.name == name]


# --- inputs -------------------------------------------------------------------


def _password(password: str, password_from_env: str, password_stdin: bool) -> str:
    """One password source at most; empty means the gateway generates one."""
    if sum([bool(password), bool(password_from_env), password_stdin]) > 1:
        raise click.ClickException("Specify only one password source.")
    if password_from_env:
        password = os.getenv(password_from_env, "")
        if not password:
            raise click.ClickException(f"Environment variable {password_from_env!r} is not set.")
    if password_stdin:
        password = sys.stdin.read().strip()
        if not password:
            raise click.ClickException("No password was provided on stdin.")
    return password


def _memory_mb(memory: Optional[str]) -> int:
    if not memory:
        return 0
    value = memory.strip().lower()
    for suffix, factor in (("gi", 1024), ("g", 1024), ("mi", 1), ("m", 1)):
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * factor)
    return int(float(value))


# --- output -------------------------------------------------------------------

RESULT_KEYS = (
    "name",
    "kind",
    "deployment_id",
    "host",
    "username",
    "database",
    "connection_env_name",
    "connection_string",
    "connection_string_secret",
    "username_secret",
    "password_secret",
    "database_secret",
)


def _redis_fields(url: str) -> Dict[str, object]:
    parsed = urlparse(url)
    return {
        "host": parsed.hostname or "",
        "port": parsed.port or 443,
        "username": parsed.username or "default",
        "password": parsed.password or "",
        "tls_server_name": parsed.hostname or "",
    }


def _result(info: Dict[str, Any]) -> Dict[str, Any]:
    payload = {key: info[key] for key in RESULT_KEYS if info.get(key)}
    if info.get("kind") == "redis" and info.get("connection_string"):
        payload["tls_server_name"] = _redis_fields(info["connection_string"])["tls_server_name"]
    return payload


def _print_result(format: str, payload: Dict[str, Any]) -> None:
    if format == "json":
        terminal.print_json(payload)
        return
    table = Table(Column("Field"), Column("Value"), box=box.SIMPLE)
    keys = [key for key in RESULT_KEYS if key in payload]
    keys.extend(sorted(key for key in payload if key not in keys))
    for key in keys:
        table.add_row(key, str(payload[key]))
    terminal.print(table)


def _print_list(format: str, services: Iterable[Dict[str, Any]]) -> None:
    services = list(services)
    if format == "json":
        terminal.print_json(services)
        return
    table = Table(
        Column("Name"),
        Column("Kind"),
        Column("Active"),
        Column("Version", justify="right"),
        Column("Connection Env"),
        Column("Connection Secret"),
        box=box.SIMPLE,
    )
    for info in services:
        table.add_row(
            info["name"],
            info["kind"],
            "Yes" if info.get("active") else "No",
            str(info.get("version", "")),
            info.get("connection_env_name") or "-",
            info.get("connection_string_secret") or "-",
        )
    table.add_section()
    table.add_row(f"[bold]{len(services)} databases")
    terminal.print(table)


# --- commands -----------------------------------------------------------------


@db.command(name="list", help="List database services.")
@click.option(
    "--kind",
    type=click.Choice(("all", *PRODUCTS)),
    default="all",
    show_default=True,
    help="Only show one kind of database.",
)
@FORMAT_OPTION
@extraclick.pass_service_client
def list_databases(service: ServiceClient, kind: str, format: str):
    services = _services(service)
    if kind != "all":
        services = [s for s in services if s["kind"] == kind]
    _print_list(format, sorted(services, key=lambda s: s["name"]))


def _create_options(func):
    for option in reversed(
        (
            click.option("--username", default="", help="Database username."),
            click.option("--password", default="", help="Database password. Generated if omitted."),
            click.option(
                "--password-from-env",
                default="",
                help="Read password from an environment variable.",
            ),
            click.option("--password-stdin", is_flag=True, help="Read password from stdin."),
            click.option("--pool", default=None, help="Run on a private pool."),
            click.option(
                "--min-replicas",
                type=click.IntRange(min=0, max=1),
                default=0,
                show_default=True,
                help="Minimum database replicas to keep warm. Use 1 to keep the service warm.",
            ),
            click.option(
                "--cpu", type=click.FLOAT, default=None, help="CPU cores, for example 0.5 or 2."
            ),
            click.option("--memory", default=None, help="Memory, for example 1024 or 2Gi."),
            FORMAT_OPTION,
        )
    ):
        func = option(func)
    return func


def _scale_options(func):
    for option in reversed(
        (
            click.option("--always-on", is_flag=True, help="Keep one database container warm."),
            click.option("--serverless", is_flag=True, help="Scale to zero when idle."),
            click.option(
                "--cpu", type=click.FLOAT, default=None, help="CPU cores, for example 0.5 or 2."
            ),
            click.option("--memory", default=None, help="Memory, for example 1024 or 2Gi."),
            click.option("--pool", default=None, help="Redeploy the database on a pool."),
            FORMAT_OPTION,
        )
    ):
        func = option(func)
    return func


def _scale(
    service: ServiceClient,
    kind: str,
    name: str,
    always_on: bool,
    serverless: bool,
    cpu: Optional[float],
    memory: Optional[str],
    pool: Optional[str],
    format: str,
) -> None:
    if always_on == serverless:
        raise click.ClickException("Specify exactly one of --always-on or --serverless.")
    containers = 1 if always_on else 0
    info = _service_info(service, kind, name)

    if cpu is not None or memory is not None or pool is not None:
        # Resources live in the stub config: new version; warm mode rides along.
        def mutate(config: Dict[str, Any]) -> None:
            runtime = config.setdefault("runtime", {})
            if cpu is not None:
                runtime["cpu"] = int(cpu * 1000)
            if memory is not None:
                runtime["memory"] = _memory_mb(memory)
            if pool is not None:
                config["pool"] = {"name": pool} if pool else None
            config.setdefault("autoscaler", {})["min_containers"] = containers

        result = stubconfig.redeploy_with_config(service, name, info["stub_id"], mutate)
        _print_result(format, {"name": name, "kind": kind, **result})
        return

    # Zero stops every version; warm touches the newest.
    deployments = _deployments_by_name(service, name)
    if containers == 1:
        deployments = [d for d in deployments if d.id == info["deployment_id"]]
    for deployment in deployments:
        res = service.gateway.scale_deployment(
            ScaleDeploymentRequest(id=deployment.id, containers=containers)
        )
        if not res.ok:
            raise click.ClickException(res.err_msg or f"Failed to scale {name}.")
    terminal.success(f"Set {kind} service {name} to {'always-on' if always_on else 'serverless'}")


def _database_group(kind: str, label: str) -> click.Group:
    """The per-kind command set; every kind gets the same verbs."""

    @db.group(name=kind, help=f"Create and manage {label} services.")
    def group():
        pass

    @group.command(name="create", help=f"Create a serverless {label} service.")
    @click.argument("name")
    @click.option("--database", default="", help="Initial database name.", hidden=kind == "redis")
    @_create_options
    @extraclick.pass_service_client
    def create(
        service: ServiceClient,
        name: str,
        database: str,
        username: str,
        password: str,
        password_from_env: str,
        password_stdin: bool,
        pool: Optional[str],
        min_replicas: int,
        cpu: Optional[float],
        memory: Optional[str],
        format: str,
    ):
        info = _api(
            service,
            "POST",
            json={
                "kind": kind,
                "name": name,
                "username": username,
                "password": _password(password, password_from_env, password_stdin),
                "database": database,
                "pool": pool or "",
                "always_on": min_replicas > 0,
                "cpu": int(cpu * 1000) if cpu else 0,
                "memory": _memory_mb(memory),
            },
        )
        _print_result(format, _result(info))

    @group.command(name="credentials", help=f"Show {label} connection details.")
    @click.argument("name")
    @FORMAT_OPTION
    @extraclick.pass_service_client
    def credentials(service: ServiceClient, name: str, format: str):
        info = _service_info(service, kind, name)
        payload = {
            "name": name,
            "kind": kind,
            "username": _secret_value(service, info["username_secret"]),
            "connection_string": _secret_value(service, info["connection_string_secret"]),
            "connection_string_secret": info["connection_string_secret"],
        }
        if info.get("database_secret"):
            payload["database"] = _secret_value(service, info["database_secret"])
        if kind == "redis":
            fields = _redis_fields(payload["connection_string"])
            payload.update({k: v for k, v in fields.items() if k != "password"})
        _print_result(format, payload)

    @group.command(name="secrets", help=f"Print the {label} connection string and its secrets.")
    @click.argument("name")
    @FORMAT_OPTION
    @extraclick.pass_service_client
    def secrets(service: ServiceClient, name: str, format: str):
        info = _service_info(service, kind, name)
        connection_string = _secret_value(service, info["connection_string_secret"])
        if format == "json":
            terminal.print_json(
                {
                    "name": name,
                    "kind": kind,
                    "connection_string": connection_string,
                    "connection_string_secret": info["connection_string_secret"],
                    "secrets": {
                        key: info[key]
                        for key in (
                            "connection_string_secret",
                            "username_secret",
                            "password_secret",
                            "database_secret",
                        )
                        if info.get(key)
                    },
                }
            )
            return
        click.echo(connection_string)

    @group.command(name="status", help=f"Show {label} service status.")
    @click.argument("name")
    @FORMAT_OPTION
    @extraclick.pass_service_client
    def status(service: ServiceClient, name: str, format: str):
        _print_result(format, _result(_service_info(service, kind, name)))

    @group.command(
        name="rotate",
        help=f"Rotate the {label} password; the service restarts with the new credentials.",
    )
    @click.argument("name")
    @FORMAT_OPTION
    @extraclick.pass_service_client
    def rotate(service: ServiceClient, name: str, format: str):
        _print_result(format, _result(_api(service, "POST", f"/{name}/rotate")))

    @group.command(name="delete", help=f"Delete a {label} service and its generated secrets.")
    @click.argument("name")
    @extraclick.pass_service_client
    def delete(service: ServiceClient, name: str):
        _api(service, "DELETE", f"/{name}")
        terminal.success(f"Deleted {kind} service {name}")

    @group.command(name="scale", help=f"Set {label} warm mode or resources.")
    @click.argument("name")
    @_scale_options
    @extraclick.pass_service_client
    def scale(service: ServiceClient, name: str, **options):
        _scale(service, kind, name, **options)

    return group


postgres = _database_group("postgres", PRODUCTS["postgres"])
redis = _database_group("redis", PRODUCTS["redis"])
mysql = _database_group("mysql", PRODUCTS["mysql"])
mongo = _database_group("mongo", PRODUCTS["mongo"])


@postgres.command(name="connect", help="Print the Postgres connection string.")
@click.argument("name")
@click.option("--psql", "psql_command", is_flag=True, help="Print a psql command.")
@extraclick.pass_service_client
def postgres_connect(service: ServiceClient, name: str, psql_command: bool):
    info = _service_info(service, "postgres", name)
    url = _secret_value(service, info["connection_string_secret"])
    click.echo(f"psql {shlex.quote(url)}" if psql_command else url)


@redis.command(name="connect", help="Print the Redis connection string.")
@click.argument("name")
@click.option(
    "--redis-cli", "redis_cli_command", is_flag=True, help="Print a redis-cli command with SNI."
)
@click.option("--node-redis", is_flag=True, help="Print a node-redis connection snippet.")
@click.option("--ioredis", is_flag=True, help="Print an ioredis connection snippet.")
@extraclick.pass_service_client
def redis_connect(
    service: ServiceClient, name: str, redis_cli_command: bool, node_redis: bool, ioredis: bool
):
    if sum([redis_cli_command, node_redis, ioredis]) > 1:
        raise click.ClickException("Specify only one Redis client output format.")
    info = _service_info(service, "redis", name)
    url = _secret_value(service, info["connection_string_secret"])
    fields = _redis_fields(url)
    if redis_cli_command:
        click.echo(
            "redis-cli --tls --sni {host} -h {host} -p {port} --user {username} --pass {password}".format(
                **{k: shlex.quote(str(v)) for k, v in fields.items()}
            )
        )
    elif node_redis:
        click.echo(
            "createClient({ url: %r, socket: { tls: true, servername: %r } })"
            % (url, fields["tls_server_name"])
        )
    elif ioredis:
        click.echo("new Redis(%r, { tls: { servername: %r } })" % (url, fields["tls_server_name"]))
    else:
        click.echo(url)
