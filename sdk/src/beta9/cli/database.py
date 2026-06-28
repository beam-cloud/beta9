import os
import secrets as secrets_lib
import shlex
import sys
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import quote, urlparse

import click
from rich.table import Column, Table, box

from .. import terminal
from ..abstractions.base.runner import POD_DEPLOYMENT_STUB_TYPE
from ..abstractions.image import Image
from ..abstractions.service import Service
from ..channel import ServiceClient
from ..clients.gateway import (
    Autoscaler as AutoscalerProto,
    BindServiceRequest,
    DeleteDeploymentRequest,
    DeployStubRequest,
    GetUrlRequest,
    GetOrCreateStubRequest,
    ListDeploymentsRequest,
    SecretVar,
    DatabaseServingConfig as DatabaseServingConfigProto,
    ServingConfig as ServingConfigProto,
    ScaleDeploymentRequest,
    StringList,
    TaskPolicy as TaskPolicyProto,
)
from ..clients.image import BuildImageRequest, ImageServiceStub, VerifyImageBuildRequest
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

DEFAULT_DATABASE_KEEP_WARM_SECONDS = 300
DEFAULT_DATABASE_POOL = "default"
DEFAULT_DATABASE_CPU = 1.0
DEFAULT_DATABASE_MEMORY = 512


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
        if not value:
            raise click.ClickException(f"Environment variable {password_from_env!r} is not set.")
        return value
    if password_stdin:
        value = sys.stdin.read().strip()
        if not value:
            raise click.ClickException("No password was provided on stdin.")
        return value
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
    deployments = _deployments_by_name(service, name)
    if not deployments:
        return None
    return max(deployments, key=_deployment_sort_key)


def _deployments_by_name(service: ServiceClient, name: str):
    res = service.gateway.list_deployments(
        ListDeploymentsRequest(filters={"name": StringList(values=[name])}, limit=20)
    )
    if not res.ok:
        raise click.ClickException(res.err_msg or "Unable to list deployments.")
    return [deployment for deployment in res.deployments if deployment.name == name]


def _ensure_database_name_available(service: ServiceClient, product: DatabaseProduct, name: str) -> None:
    if _deployment_by_name(service, name) is None:
        return
    raise click.ClickException(
        f"{product.kind} service {name!r} already exists. "
        f"Use `beam {product.kind} credentials {name}`, `beam {product.kind} status {name}`, "
        f"or delete it before creating a replacement."
    )


def _deployment_sort_key(deployment):
    return (
        bool(deployment.active),
        deployment.version,
        deployment.updated_at.timestamp() if deployment.updated_at else 0,
        deployment.created_at.timestamp() if deployment.created_at else 0,
    )


def _tcp_host_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc or parsed.path
    host = host.rstrip("/")
    if ":" not in host:
        host = f"{host}:443"
    return host or "<host>"


def _tcp_host_for_stub(service: ServiceClient, stub_id: str, deployment_id: str) -> str:
    res = service.gateway.get_url(GetUrlRequest(stub_id=stub_id, deployment_id=deployment_id))
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
    if username:
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
    preferred = (
        "name",
        "kind",
        "deployment_id",
        "version",
        "disk",
        "disk_size",
        "host",
        "username",
        "database",
        "connection_string",
        "connection_string_secret",
        "username_secret",
        "password_secret",
        "database_secret",
    )
    keys = [key for key in preferred if key in payload]
    keys.extend(sorted(key for key in payload if key not in keys))
    for key in keys:
        table.add_row(key, str(payload[key]))
    terminal.print(table)


def _database_serving_config(
    product: DatabaseProduct,
    secret_names: Dict[str, str],
) -> ServingConfig:
    return ServingConfig(
        app_kind="database",
        serving_protocol=product.kind,
        database=DatabaseServingConfig(
            kind=product.kind,
            port=product.port,
            readiness_probe=product.readiness_probe,
            connection_env_name=product.connection_env_name,
            durability_mode="snapshot_wal" if product.kind == "postgres" else "aof_tail",
            credential_secret_names=list(secret_names.values()),
            username_secret_name=secret_names["username"],
            password_secret_name=secret_names["password"],
            database_secret_name=secret_names.get("database", ""),
            connection_url_secret_name=secret_names["url"],
        )
    )


def _serving_config_proto(config: ServingConfig) -> ServingConfigProto:
    database = config.database
    return ServingConfigProto(
        app_kind=config.app_kind,
        serving_protocol=config.serving_protocol,
        database=DatabaseServingConfigProto(
            kind=database.kind,
            port=database.port,
            readiness_probe=database.readiness_probe,
            connection_env_name=database.connection_env_name,
            credential_secret_names=database.credential_secret_names,
            durability_mode=database.durability_mode,
            username_secret_name=database.username_secret_name,
            password_secret_name=database.password_secret_name,
            database_secret_name=database.database_secret_name,
            connection_url_secret_name=database.connection_url_secret_name,
        )
        if database
        else None,
    )


def _postgres_entrypoint(secret_names: Dict[str, str]) -> list:
    return [
        "sh",
        "-lc",
        (
            "export PATH=/usr/lib/postgresql/16/bin:$PATH "
            f"POSTGRES_USER=\"${{{secret_names['username']}}}\" "
            f"POSTGRES_PASSWORD=\"${{{secret_names['password']}}}\" "
            f"POSTGRES_DB=\"${{{secret_names['database']}}}\" "
            "PGDATA=/var/lib/postgresql/data/pgdata; "
            "exec docker-entrypoint.sh postgres "
            "-c wal_compression=on"
        ),
    ]


def _redis_entrypoint(secret_names: Dict[str, str]) -> list:
    username = secret_names["username"]
    password = secret_names["password"]
    return [
        "sh",
        "-lc",
        (
            f"if [ \"${{{username}}}\" = \"default\" ]; then "
            "printf 'appendonly yes\\nappendfsync always\\ndir /data\\nuser default on >%s ~* &* +@all\\n' "
            f"\"${{{password}}}\" > /tmp/redis.conf; "
            "else "
            "printf 'appendonly yes\\nappendfsync always\\ndir /data\\nuser default off\\nuser %s on >%s ~* &* +@all\\n' "
            f"\"${{{username}}}\" \"${{{password}}}\" > /tmp/redis.conf; "
            "fi; exec redis-server /tmp/redis.conf"
        ),
    ]


def _registry_image_id(service: ServiceClient, image: Image) -> Tuple[str, str]:
    image_client = ImageServiceStub(service.channel)
    python_version = getattr(image.python_version, "value", image.python_version)
    verify = image_client.verify_image_build(
        VerifyImageBuildRequest(
            python_version=python_version,
            existing_image_uri=image.base_image,
        )
    )
    if verify.exists:
        return verify.image_id, python_version

    final = None
    for response in image_client.build_image(
        BuildImageRequest(
            python_version=python_version,
            existing_image_uri=image.base_image,
        )
    ):
        if response.done:
            final = response
            break

    if not final or not final.success:
        raise click.ClickException(
            (final.msg if final else "") or f"Failed to prepare image {image.base_image}."
        )
    return final.image_id, final.python_version or python_version


def _database_service(
    product: DatabaseProduct,
    name: str,
    size: str,
    pool: Optional[str],
    min_replicas: int,
    cpu: Optional[float],
    memory: Optional[str],
) -> Service:
    secret_names = _secret_names(product, name)
    disk = DurableDisk(
        name=f"{name}-data",
        size=size,
        mount_path=product.mount_path,
        replication=DiskReplication(),
    )
    return Service(
        name=name,
        image=Image.from_registry(product.image),
        entrypoint=_postgres_entrypoint(secret_names)
        if product.kind == "postgres"
        else _redis_entrypoint(secret_names),
        ports=[product.port],
        tcp=True,
        min_replicas=min_replicas,
        keep_warm_seconds=DEFAULT_DATABASE_KEEP_WARM_SECONDS,
        pool=pool or DEFAULT_DATABASE_POOL,
        secrets=[v for k, v in secret_names.items() if k != "url"],
        env={},
        disks=[disk],
        serving=_database_serving_config(product, secret_names),
        cpu=cpu or DEFAULT_DATABASE_CPU,
        memory=memory or DEFAULT_DATABASE_MEMORY,
    )


def _deploy_database_stub(service: ServiceClient, db_service: Service) -> Tuple[str, str, int]:
    image_id, python_version = _registry_image_id(service, db_service.image)
    stub_response = service.gateway.get_or_create_stub(
        GetOrCreateStubRequest(
            image_id=image_id,
            stub_type=POD_DEPLOYMENT_STUB_TYPE,
            name=POD_DEPLOYMENT_STUB_TYPE,
            python_version=python_version,
            cpu=db_service.cpu,
            memory=db_service.memory,
            gpu="",
            gpu_count=0,
            keep_warm_seconds=db_service.keep_warm_seconds,
            workers=1,
            max_pending_tasks=100,
            secrets=[SecretVar(name=secret.name) for secret in db_service.secrets],
            autoscaler=AutoscalerProto(
                type="queue_depth",
                max_containers=db_service.max_replicas,
                tasks_per_container=1,
                min_containers=db_service.min_replicas,
            ),
            task_policy=TaskPolicyProto(timeout=3600, max_retries=3),
            concurrent_requests=1,
            entrypoint=db_service.entrypoint or [],
            ports=db_service.ports,
            env=db_service.env,
            app_name=db_service.name or "",
            force_create=True,
            authorized=False,
            tcp=True,
            pool=db_service.pool_config,
            is_service=True,
            serving=_serving_config_proto(db_service.serving),
            disks=[disk.export() for disk in db_service.disks],
        )
    )
    if not stub_response.ok:
        raise click.ClickException(stub_response.err_msg or "Failed to create database service.")

    deploy_response = service.gateway.deploy_stub(
        DeployStubRequest(stub_id=stub_response.stub_id, name=db_service.name or "")
    )
    if not deploy_response.ok:
        raise click.ClickException(
            deploy_response.invoke_url or f"Failed to deploy database service {db_service.name}."
        )
    return stub_response.stub_id, deploy_response.deployment_id, deploy_response.version


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
    cpu: Optional[float],
    memory: Optional[str],
) -> None:
    _ensure_database_name_available(service, product, name)
    secret_names = _secret_names(product, name)
    _upsert_secret(service, secret_names["username"], username)
    _upsert_secret(service, secret_names["password"], password)
    if product.kind == "postgres":
        _upsert_secret(service, secret_names["database"], database)

    _deploy_database_service(
        service=service,
        product=product,
        name=name,
        username=username,
        database=database,
        password=password,
        size=size,
        pool=pool,
        min_replicas=min_replicas,
        format=format,
        cpu=cpu,
        memory=memory,
    )


def _deploy_database_service(
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
    cpu: Optional[float],
    memory: Optional[str],
) -> None:
    secret_names = _secret_names(product, name)
    db_service = _database_service(
        product=product,
        name=name,
        size=size,
        pool=pool,
        min_replicas=min_replicas,
        cpu=cpu,
        memory=memory,
    )
    stub_id, deployment_id, version = _deploy_database_stub(service, db_service)
    host = _tcp_host_for_stub(service, stub_id, deployment_id)
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
            "deployment_id": deployment_id,
            "version": version,
            "disk": db_service.disks[0].name,
            "disk_size": db_service.disks[0].size,
            "host": host,
            "username": username,
            "connection_string": connection_url,
            "connection_string_secret": secret_names["url"],
            "username_secret": secret_names["username"],
            "password_secret": secret_names["password"],
            **({"database": database} if product.kind == "postgres" else {}),
            **({"database_secret": secret_names["database"]} if product.kind == "postgres" else {}),
        },
    )


def _create_options(func):
    func = click.option(
        "--format",
        "format",
        type=click.Choice(("table", "json")),
        default="table",
        show_default=True,
        help="Change the format of the output.",
    )(func)
    func = click.option("--memory", type=click.STRING, default=None, help="Memory to allocate, for example 1024 or 2Gi.")(func)
    func = click.option("--cpu", type=click.FLOAT, default=None, help="CPU cores to allocate, for example 0.5 or 2.")(func)
    func = click.option(
        "--min-replicas",
        type=click.IntRange(min=0, max=1),
        default=0,
        show_default=True,
        help="Minimum database replicas to keep warm. Use 1 to keep the service warm.",
    )(func)
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
    cpu: Optional[float],
    memory: Optional[str],
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
        cpu=cpu,
        memory=memory,
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
    cpu: Optional[float],
    memory: Optional[str],
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
        cpu=cpu,
        memory=memory,
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
@click.option("--psql", "psql_command", is_flag=True, help="Print a psql command.")
@extraclick.pass_service_client
def postgres_connect(service: ServiceClient, name: str, psql_command: bool):
    url = _get_secret_value(service, _secret_names(POSTGRES, name)["url"])
    click.echo(f"psql {shlex.quote(url)}" if psql_command else url)


@redis.command(name="connect", help="Print the Redis connection string.")
@click.argument("name")
@click.option("--redis-cli", "redis_cli_command", is_flag=True, help="Print a redis-cli command with SNI.")
@extraclick.pass_service_client
def redis_connect(service: ServiceClient, name: str, redis_cli_command: bool):
    url = _get_secret_value(service, _secret_names(REDIS, name)["url"])
    if redis_cli_command:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        username = parsed.username or "default"
        password = parsed.password or ""
        port = parsed.port or 443
        click.echo(
            "redis-cli --tls --sni {host} -h {host} -p {port} --user {username} --pass {password}".format(
                host=shlex.quote(host),
                port=shlex.quote(str(port)),
                username=shlex.quote(username),
                password=shlex.quote(password),
            )
        )
        return
    click.echo(url)


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
    raise click.ClickException(
        f"{product.kind} password rotation is not available yet. "
        "Delete and recreate the service to replace credentials."
    )


@postgres.command(name="rotate", help="Rotate the Postgres password. Not available yet.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def postgres_rotate(service: ServiceClient, name: str, format: str):
    _rotate(service, POSTGRES, name, format)


@redis.command(name="rotate", help="Rotate the Redis password. Not available yet.")
@click.argument("name")
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@extraclick.pass_service_client
def redis_rotate(service: ServiceClient, name: str, format: str):
    _rotate(service, REDIS, name, format)


def _delete_database(service: ServiceClient, product: DatabaseProduct, name: str) -> None:
    deployments = _deployments_by_name(service, name)
    for deployment in deployments:
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


def _scale_options(func):
    func = click.option("--format", type=click.Choice(("table", "json")), default="table")(func)
    func = click.option("--size", type=click.STRING, default=None, help="Durable disk size to use when redeploying.")(func)
    func = click.option("--pool", type=click.STRING, default=None, help="Pool to use when redeploying with new resources.")(func)
    func = click.option("--memory", type=click.STRING, default=None, help="Memory to allocate, for example 1024 or 2Gi.")(func)
    func = click.option("--cpu", type=click.FLOAT, default=None, help="CPU cores to allocate, for example 0.5 or 2.")(func)
    return click.option("--replicas", "--containers", "containers", type=click.IntRange(min=0, max=1), required=True)(func)


@postgres.command(name="scale", help="Scale a Postgres service.")
@click.argument("name")
@_scale_options
@extraclick.pass_service_client
def postgres_scale(
    service: ServiceClient,
    name: str,
    containers: int,
    cpu: Optional[float],
    memory: Optional[str],
    pool: Optional[str],
    size: Optional[str],
    format: str,
):
    _scale_database(service, POSTGRES, name, containers, cpu, memory, pool, size, format)


@redis.command(name="scale", help="Scale a Redis service.")
@click.argument("name")
@_scale_options
@extraclick.pass_service_client
def redis_scale(
    service: ServiceClient,
    name: str,
    containers: int,
    cpu: Optional[float],
    memory: Optional[str],
    pool: Optional[str],
    size: Optional[str],
    format: str,
):
    _scale_database(service, REDIS, name, containers, cpu, memory, pool, size, format)


def _scale_database(
    service: ServiceClient,
    product: DatabaseProduct,
    name: str,
    containers: int,
    cpu: Optional[float],
    memory: Optional[str],
    pool: Optional[str],
    size: Optional[str],
    format: str,
) -> None:
    if cpu is not None or memory is not None:
        secret_names = _secret_names(product, name)
        _scale_database_to(service, product, name, 0, quiet=True, all_deployments=True)
        _deploy_database_service(
            service=service,
            product=product,
            name=name,
            username=_get_secret_value(service, secret_names["username"]),
            database=_get_secret_value(service, secret_names["database"]) if product.kind == "postgres" else "",
            password=_get_secret_value(service, secret_names["password"]),
            size=size or product.default_size,
            pool=pool,
            min_replicas=containers,
            format=format,
            cpu=cpu,
            memory=memory,
        )
        return

    _scale_database_to(service, product, name, containers, quiet=False)


def _scale_database_to(
    service: ServiceClient,
    product: DatabaseProduct,
    name: str,
    containers: int,
    quiet: bool,
    all_deployments: bool = False,
) -> None:
    deployments = _deployments_by_name(service, name)
    if not deployments:
        raise click.ClickException(f"{product.kind} service {name!r} not found.")
    targets = deployments if all_deployments or containers == 0 else [max(deployments, key=_deployment_sort_key)]
    for deployment in targets:
        if deployment is None:
            continue
        res = service.gateway.scale_deployment(
            ScaleDeploymentRequest(id=deployment.id, containers=containers)
        )
        if not res.ok:
            raise click.ClickException(res.err_msg or f"Failed to scale {name}.")
    if not quiet:
        terminal.success(f"Scaled {product.kind} service {name} to {containers} replicas")
