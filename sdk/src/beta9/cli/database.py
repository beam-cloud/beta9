import os
import secrets as secrets_lib
import sys
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import quote, urlparse

import click
from rich.table import Column, Table, box

from .. import terminal
from ..abstractions.image import Image
from ..abstractions.service import Service
from ..channel import ServiceClient
from ..clients.gateway import (
    BindServiceRequest,
    DeleteDeploymentRequest,
    GetUrlRequest,
    ListDeploymentsRequest,
    ScaleDeploymentRequest,
    StringList,
)
from ..clients.secret import (
    CreateSecretRequest,
    DeleteSecretRequest,
    GetSecretRequest,
    UpdateSecretRequest,
)
from ..type import DatabaseServingConfig, DiskReplication, DurableDisk, ServingConfig
from . import extraclick
from .extraclick import ClickCommonGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="postgres", help="Create and manage Postgres services.")
def postgres():
    pass


@common.group(name="redis", help="Create and manage Redis services.")
def redis():
    pass


@common.group(name="service", help="Manage service bindings.")
def service():
    pass


@dataclass
class DatabaseProduct:
    kind: str
    image: str
    port: int
    mount_path: str
    default_size: str
    readiness_probe: str
    connection_env_name: str
    default_database: str = ""


POSTGRES = DatabaseProduct(
    kind="postgres",
    image="docker.io/library/postgres:16",
    port=5432,
    mount_path="/var/lib/postgresql/data",
    default_size="10Gi",
    readiness_probe="pg_isready",
    connection_env_name="DATABASE_URL",
    default_database="postgres",
)

REDIS = DatabaseProduct(
    kind="redis",
    image="docker.io/library/redis:7",
    port=6379,
    mount_path="/data",
    default_size="5Gi",
    readiness_probe="PING",
    connection_env_name="REDIS_URL",
)


def _env_prefix(kind: str, name: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in name.upper()).strip("_")
    return f"BETA9_{kind.upper()}_{safe or 'SERVICE'}"


def _password(password: str, password_from_env: str, password_stdin: bool) -> str:
    supplied = [bool(password), bool(password_from_env), password_stdin]
    if sum(supplied) > 1:
        raise click.ClickException("Specify only one password source.")
    if password:
        return password
    if password_from_env:
        value = os.getenv(password_from_env)
        if value is None:
            raise click.ClickException(f"Environment variable {password_from_env!r} is not set.")
        return value
    if password_stdin:
        return sys.stdin.read().strip()
    return secrets_lib.token_urlsafe(32)


def _secret_names(product: DatabaseProduct, name: str) -> Dict[str, str]:
    prefix = _env_prefix(product.kind, name)
    names = {
        "username": f"{prefix}_USERNAME",
        "password": f"{prefix}_PASSWORD",
        "url": f"{prefix}_URL",
    }
    if product.kind == "postgres":
        names["database"] = f"{prefix}_DATABASE"
    return names


def _upsert_secret(service: ServiceClient, name: str, value: str) -> None:
    existing = service.secret.get_secret(GetSecretRequest(name=name))
    if existing.ok:
        res = service.secret.update_secret(UpdateSecretRequest(name=name, value=value))
        if not res.ok:
            raise click.ClickException(res.err_msg or f"Failed to update secret {name}.")
        return

    res = service.secret.create_secret(CreateSecretRequest(name=name, value=value))
    if not res.ok:
        raise click.ClickException(res.err_msg or f"Failed to create secret {name}.")


def _delete_secret(service: ServiceClient, name: str) -> None:
    res = service.secret.delete_secret(DeleteSecretRequest(name=name))
    if not res.ok:
        terminal.error(res.err_msg or f"Failed to delete secret {name}.", exit=False)


def _get_secret_value(service: ServiceClient, name: str) -> str:
    res = service.secret.get_secret(GetSecretRequest(name=name))
    if not res.ok:
        raise click.ClickException(res.err_msg or f"Secret {name} not found.")
    return res.secret.value


def _deployment_by_name(service: ServiceClient, name: str):
    res = service.gateway.list_deployments(
        ListDeploymentsRequest(filters={"name": StringList(values=[name])}, limit=20)
    )
    if not res.ok:
        raise click.ClickException(res.err_msg or "Unable to list deployments.")
    for deployment in res.deployments:
        if deployment.name == name:
            return deployment
    return None


def _tcp_host_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc or parsed.path
    host = host.rstrip("/")
    if ":" not in host:
        host = f"{host}:443"
    return host or "<host>"


def _tcp_host_for_service(service: Service) -> str:
    res = service.gateway_stub.get_url(
        GetUrlRequest(
            stub_id=service.stub_id,
            deployment_id=getattr(service, "deployment_id", ""),
        )
    )
    if not res.ok:
        raise click.ClickException(res.err_msg or "Failed to get database TCP endpoint.")
    return _tcp_host_from_url(res.url)


def _postgres_url(username: str, password: str, host: str, database: str) -> str:
    return "postgresql://{}:{}@{}/{}?sslmode=require".format(
        quote(username),
        quote(password),
        host,
        quote(database),
    )


def _redis_url(username: str, password: str, host: str) -> str:
    if username and username != "default":
        return "rediss://{}:{}@{}/0".format(quote(username), quote(password), host)
    return "rediss://:{}@{}/0".format(quote(password), host)


def _print_result(format: str, payload: Dict[str, object]) -> None:
    if format == "json":
        terminal.print_json(payload)
        return

    table = Table(
        Column("Field"),
        Column("Value"),
        box=box.SIMPLE,
    )
    for key in (
        "name",
        "kind",
        "deployment_id",
        "host",
        "connection_string",
        "connection_string_secret",
    ):
        table.add_row(key, str(payload.get(key, "")))
    terminal.print(table)


def _database_serving_config(
    product: DatabaseProduct,
    secret_names: Dict[str, str],
) -> ServingConfig:
    return ServingConfig(
        database=DatabaseServingConfig(
            kind=product.kind,
            port=product.port,
            readiness_probe=product.readiness_probe,
            connection_env_name=product.connection_env_name,
            credential_secret_names=list(secret_names.values()),
            username_secret_name=secret_names["username"],
            password_secret_name=secret_names["password"],
            database_secret_name=secret_names.get("database", ""),
            connection_url_secret_name=secret_names["url"],
        )
    )


def _postgres_entrypoint(secret_names: Dict[str, str]) -> list:
    return [
        "sh",
        "-lc",
        "export POSTGRES_USER=\"${%s}\" POSTGRES_PASSWORD=\"${%s}\" POSTGRES_DB=\"${%s}\" PGDATA=/var/lib/postgresql/data/pgdata; exec docker-entrypoint.sh postgres"
        % (secret_names["username"], secret_names["password"], secret_names["database"]),
    ]


def _redis_entrypoint(secret_names: Dict[str, str]) -> list:
    return [
        "sh",
        "-lc",
        "printf 'appendonly yes\\nappendfsync always\\ndir /data\\nuser default off\\nuser %s on >%s ~* &* +@all\\n' \"${%s}\" \"${%s}\" > /tmp/redis.conf; exec redis-server /tmp/redis.conf"
        % (
            "%s",
            "%s",
            secret_names["username"],
            secret_names["password"],
        ),
    ]


def _create_database(
    service: ServiceClient,
    product: DatabaseProduct,
    name: str,
    username: str,
    database: str,
    password: str,
    size: str,
    pool: Optional[str],
    min_replicas: int,
    format: str,
    disk_driver: str,
) -> None:
    secret_names = _secret_names(product, name)
    _upsert_secret(service, secret_names["username"], username)
    _upsert_secret(service, secret_names["password"], password)
    if product.kind == "postgres":
        _upsert_secret(service, secret_names["database"], database)

    disk = DurableDisk(
        name=f"{name}-data",
        size=size,
        mount_path=product.mount_path,
        driver=disk_driver,
        replication=DiskReplication(),
    )

    if product.kind == "postgres":
        entrypoint = _postgres_entrypoint(secret_names)
        env = {}
    else:
        entrypoint = _redis_entrypoint(secret_names)
        env = {}

    db_service = Service(
        name=name,
        image=Image.from_registry(product.image),
        entrypoint=entrypoint,
        ports=[product.port],
        tcp=True,
        min_replicas=min_replicas,
        keep_warm_seconds=0,
        pool=pool,
        secrets=[v for k, v in secret_names.items() if k != "url"],
        env=env,
        disks=[disk],
        serving=_database_serving_config(product, secret_names),
    )
    details, ok = db_service.deploy(
        name=name,
        invocation_details_func=lambda **_: None,
    )
    if not ok:
        raise click.ClickException(f"Failed to deploy {product.kind} service {name}.")

    host = _tcp_host_for_service(db_service)
    if product.kind == "postgres":
        connection_url = _postgres_url(username, password, host, database)
    else:
        connection_url = _redis_url(username, password, host)
    _upsert_secret(service, secret_names["url"], connection_url)

    _print_result(
        format,
        {
            "name": name,
            "kind": product.kind,
            "deployment_id": details.get("deployment_id", ""),
            "host": host,
            "connection_string": connection_url,
            "connection_string_secret": secret_names["url"],
            "username_secret": secret_names["username"],
            "password_secret": secret_names["password"],
            "database_secret": secret_names.get("database", ""),
        },
    )


def _create_options(func):
    func = click.option(
        "--disk-driver",
        default="",
        hidden=True,
        help="Durable disk driver override.",
    )(func)
    func = click.option(
        "--format",
        "format",
        type=click.Choice(("table", "json")),
        default="table",
        show_default=True,
        help="Change the format of the output.",
    )(func)
    func = click.option("--min-replicas", type=click.IntRange(min=0), default=0, show_default=True)(func)
    func = click.option("--pool", type=click.STRING, default=None, help="Run on a private pool.")(func)
    func = click.option("--size", type=click.STRING, default=None, help="Durable disk size.")(func)
    func = click.option("--password-stdin", is_flag=True, help="Read password from stdin.")(func)
    func = click.option("--password-from-env", type=click.STRING, default="", help="Read password from an environment variable.")(func)
    func = click.option("--password", type=click.STRING, default="", help="Database password. Generated if omitted.")(func)
    func = click.option("--username", type=click.STRING, default="", help="Database username.")(func)
    return func


@postgres.command(name="create", help="Create a serverless Postgres service.")
@click.argument("name")
@click.option("--database", type=click.STRING, default="", help="Initial database name.")
@_create_options
@extraclick.pass_service_client
def create_postgres(
    service: ServiceClient,
    name: str,
    database: str,
    username: str,
    password: str,
    password_from_env: str,
    password_stdin: bool,
    size: str,
    pool: Optional[str],
    min_replicas: int,
    format: str,
    disk_driver: str,
):
    _create_database(
        service=service,
        product=POSTGRES,
        name=name,
        username=username or name.replace("-", "_"),
        database=database or name.replace("-", "_") or POSTGRES.default_database,
        password=_password(password, password_from_env, password_stdin),
        size=size or POSTGRES.default_size,
        pool=pool,
        min_replicas=min_replicas,
        format=format,
        disk_driver=disk_driver,
    )


@redis.command(name="create", help="Create a serverless Redis service.")
@click.argument("name")
@_create_options
@extraclick.pass_service_client
def create_redis(
    service: ServiceClient,
    name: str,
    username: str,
    password: str,
    password_from_env: str,
    password_stdin: bool,
    size: str,
    pool: Optional[str],
    min_replicas: int,
    format: str,
    disk_driver: str,
):
    _create_database(
        service=service,
        product=REDIS,
        name=name,
        username=username or "default",
        database="",
        password=_password(password, password_from_env, password_stdin),
        size=size or REDIS.default_size,
        pool=pool,
        min_replicas=min_replicas,
        format=format,
        disk_driver=disk_driver,
    )


def _credentials(service: ServiceClient, product: DatabaseProduct, name: str, format: str) -> None:
    secret_names = _secret_names(product, name)
    payload = {
        "name": name,
        "kind": product.kind,
        "username": _get_secret_value(service, secret_names["username"]),
        "connection_string": _get_secret_value(service, secret_names["url"]),
        "connection_string_secret": secret_names["url"],
    }
    if product.kind == "postgres":
        payload["database"] = _get_secret_value(service, secret_names["database"])
    _print_result(format, payload)


@postgres.command(name="credentials", help="Show Postgres connection details.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def postgres_credentials(service: ServiceClient, name: str, format: str):
    _credentials(service, POSTGRES, name, format)


@redis.command(name="credentials", help="Show Redis connection details.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def redis_credentials(service: ServiceClient, name: str, format: str):
    _credentials(service, REDIS, name, format)


@postgres.command(name="connect", help="Print the Postgres connection string.")
@click.argument("name")
@extraclick.pass_service_client
def postgres_connect(service: ServiceClient, name: str):
    terminal.print(_get_secret_value(service, _secret_names(POSTGRES, name)["url"]))


@redis.command(name="connect", help="Print the Redis connection string.")
@click.argument("name")
@extraclick.pass_service_client
def redis_connect(service: ServiceClient, name: str):
    terminal.print(_get_secret_value(service, _secret_names(REDIS, name)["url"]))


def _status(service: ServiceClient, product: DatabaseProduct, name: str, format: str) -> None:
    deployment = _deployment_by_name(service, name)
    if deployment is None:
        raise click.ClickException(f"{product.kind} service {name!r} not found.")
    payload = {
        "name": deployment.name,
        "kind": product.kind,
        "deployment_id": deployment.id,
        "active": deployment.active,
        "version": deployment.version,
        "connection_string_secret": _secret_names(product, name)["url"],
    }
    if format == "json":
        terminal.print_json(payload)
        return
    table = Table(Column("Field"), Column("Value"), box=box.SIMPLE)
    for key, value in payload.items():
        table.add_row(key, str(value))
    terminal.print(table)


@postgres.command(name="status", help="Show Postgres service status.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def postgres_status(service: ServiceClient, name: str, format: str):
    _status(service, POSTGRES, name, format)


@redis.command(name="status", help="Show Redis service status.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def redis_status(service: ServiceClient, name: str, format: str):
    _status(service, REDIS, name, format)


def _rotate(service: ServiceClient, product: DatabaseProduct, name: str, format: str) -> None:
    secret_names = _secret_names(product, name)
    password = secrets_lib.token_urlsafe(32)
    _upsert_secret(service, secret_names["password"], password)
    username = _get_secret_value(service, secret_names["username"])
    old_url = _get_secret_value(service, secret_names["url"])
    host = old_url.split("@", 1)[1].rsplit("/", 1)[0] if "@" in old_url else "<host>"
    if product.kind == "postgres":
        database = _get_secret_value(service, secret_names["database"])
        connection_url = _postgres_url(username, password, host, database)
    else:
        connection_url = _redis_url(username, password, host)
    _upsert_secret(service, secret_names["url"], connection_url)
    _print_result(
        format,
        {
            "name": name,
            "kind": product.kind,
            "connection_string": connection_url,
            "connection_string_secret": secret_names["url"],
        },
    )


@postgres.command(name="rotate", help="Rotate the stored Postgres password.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def postgres_rotate(service: ServiceClient, name: str, format: str):
    _rotate(service, POSTGRES, name, format)


@redis.command(name="rotate", help="Rotate the stored Redis password.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def redis_rotate(service: ServiceClient, name: str, format: str):
    _rotate(service, REDIS, name, format)


def _delete_database(service: ServiceClient, product: DatabaseProduct, name: str) -> None:
    deployment = _deployment_by_name(service, name)
    if deployment is not None:
        res = service.gateway.delete_deployment(DeleteDeploymentRequest(id=deployment.id))
        if not res.ok:
            raise click.ClickException(res.err_msg or f"Failed to delete deployment {deployment.id}.")
    for secret_name in _secret_names(product, name).values():
        _delete_secret(service, secret_name)
    terminal.success(f"Deleted {product.kind} service {name}")


@postgres.command(name="delete", help="Delete a Postgres service and its generated secrets.")
@click.argument("name")
@extraclick.pass_service_client
def postgres_delete(service: ServiceClient, name: str):
    _delete_database(service, POSTGRES, name)


@redis.command(name="delete", help="Delete a Redis service and its generated secrets.")
@click.argument("name")
@extraclick.pass_service_client
def redis_delete(service: ServiceClient, name: str):
    _delete_database(service, REDIS, name)


@service.command(name="bind", help="Bind database services to an app deployment.")
@click.argument("app")
@click.option("--postgres", "postgres_name", type=click.STRING, default="")
@click.option("--redis", "redis_name", type=click.STRING, default="")
@extraclick.pass_service_client
def bind_service(service: ServiceClient, app: str, postgres_name: str, redis_name: str):
    deployment = _deployment_by_name(service, app)
    if deployment is None:
        raise click.ClickException(f"App deployment {app!r} not found.")

    secret_env: Dict[str, str] = {}
    if postgres_name:
        secret_env["DATABASE_URL"] = _secret_names(POSTGRES, postgres_name)["url"]
    if redis_name:
        secret_env["REDIS_URL"] = _secret_names(REDIS, redis_name)["url"]
    if not secret_env:
        raise click.ClickException("Specify at least one database binding.")

    res = service.gateway.bind_service(
        BindServiceRequest(id=deployment.id, secret_env=secret_env)
    )
    if not res.ok:
        raise click.ClickException(res.err_msg or "Failed to bind service.")

    terminal.success(f"Bound {', '.join(sorted(secret_env))} to {app}")


@postgres.command(name="scale", help="Scale a Postgres service.")
@click.argument("name")
@click.option("--replicas", "--containers", "containers", type=click.IntRange(min=0), required=True)
@extraclick.pass_service_client
def postgres_scale(service: ServiceClient, name: str, containers: int):
    _scale_database(service, POSTGRES, name, containers)


@redis.command(name="scale", help="Scale a Redis service.")
@click.argument("name")
@click.option("--replicas", "--containers", "containers", type=click.IntRange(min=0), required=True)
@extraclick.pass_service_client
def redis_scale(service: ServiceClient, name: str, containers: int):
    _scale_database(service, REDIS, name, containers)


def _scale_database(service: ServiceClient, product: DatabaseProduct, name: str, containers: int) -> None:
    deployment = _deployment_by_name(service, name)
    if deployment is None:
        raise click.ClickException(f"{product.kind} service {name!r} not found.")
    res = service.gateway.scale_deployment(
        ScaleDeploymentRequest(id=deployment.id, containers=containers)
    )
    if not res.ok:
        raise click.ClickException(res.err_msg or f"Failed to scale {name}.")
    terminal.success(f"Scaled {product.kind} service {name} to {containers} replicas")
