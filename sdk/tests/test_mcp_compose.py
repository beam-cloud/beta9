import textwrap
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pytest

from beta9.mcp import compose


class FakeRegistry:
    """Image configs by image reference; unknown images are unreadable. Hosts in
    `proxies` only proxy Docker Hub."""

    def __init__(
        self, configs: Optional[Dict[str, Dict[str, Any]]] = None, proxies: Iterable[str] = ()
    ):
        self.configs = configs or {}
        self.proxies = set(proxies)

    def config(self, image: str) -> Optional[Dict[str, Any]]:
        return self.configs.get(image)

    def docker_hub_image(self, image: str) -> Optional[str]:
        host, _, rest = image.partition("/")
        return f"docker.io/{rest}" if host in self.proxies else None


def translate(root: Path, text: str, **kwargs: Any) -> Dict[str, Any]:
    (root / "compose.yaml").write_text(textwrap.dedent(text))
    kwargs.setdefault("registry", FakeRegistry())
    return compose.translate(str(root), prefix="app", build_root=root / "builds", **kwargs)


def services(result: Dict[str, Any]) -> Dict[str, Any]:
    return result["spec"]["services"]


LANGFUSE_STYLE = """
x-env: &env
  DATABASE_URL: postgresql://postgres:${POSTGRES_PASSWORD:-postgres}@postgres:5432/postgres
  REDIS_HOST: redis
  REDIS_PORT: "6379"
  REDIS_AUTH: myredissecret
  REDIS_TLS_ENABLED: "false"
  CLICKHOUSE_URL: http://clickhouse:8123
  CLICKHOUSE_MIGRATION_URL: clickhouse://clickhouse:9000
  CLICKHOUSE_USER: clickhouse
  CLICKHOUSE_PASSWORD: clickhouse
  S3_ENDPOINT: http://minio:9000
  S3_EXTERNAL_ENDPOINT: http://localhost:9090
  S3_SECRET_ACCESS_KEY: miniosecret
  NEXTAUTH_URL: http://localhost:3000
  ENCRYPTION_KEY: "0000000000000000000000000000000000000000000000000000000000000000"
services:
  worker:
    image: example/worker:1
    depends_on: [postgres, redis, clickhouse, minio]
    ports: ["127.0.0.1:3030:3030"]
    environment: *env
  web:
    image: example/web:1
    depends_on:
      postgres: {condition: service_healthy}
    ports: ["3000:3000"]
    environment:
      <<: *env
      NEXTAUTH_SECRET: mysecret
  clickhouse:
    image: clickhouse/clickhouse-server:25.12
    environment:
      CLICKHOUSE_USER: clickhouse
      CLICKHOUSE_PASSWORD: clickhouse
    volumes: [clickhouse_data:/var/lib/clickhouse]
    ports: ["127.0.0.1:8123:8123", "127.0.0.1:9000:9000"]
    healthcheck:
      test: wget --spider http://localhost:8123/ping || exit 1
  minio:
    image: cgr.dev/chainguard/minio
    entrypoint: sh
    command: -c 'mkdir -p /data/bucket && minio server --address ":9000" /data'
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: miniosecret
    ports: ["9090:9000", "127.0.0.1:9091:9001"]
    volumes: [minio_data:/data]
  redis:
    image: redis:7
    command: --requirepass ${REDIS_AUTH:-myredissecret}
  postgres:
    image: postgres:${POSTGRES_VERSION:-17}
    environment:
      POSTGRES_PASSWORD: postgres
volumes:
  clickhouse_data:
  minio_data:
"""


# Compose services find each other by name on a private network; on Beam every
# address must become a reference the gateway resolves to a public endpoint.
def test_service_addresses_become_references(tmp_path):
    result = translate(tmp_path, LANGFUSE_STYLE)
    env = services(result)["app-worker"]["deploy"]["env"]

    assert env["DATABASE_URL"] == "${{db.app-postgres.DATABASE_URL}}"
    assert env["REDIS_HOST"] == "${{db.app-redis.HOST}}"
    assert env["REDIS_PORT"] == "${{db.app-redis.PORT}}"
    assert env["REDIS_AUTH"] == "${{db.app-redis.PASSWORD}}"
    assert env["CLICKHOUSE_URL"] == "${{app.app-clickhouse.URL.8123}}"
    assert env["CLICKHOUSE_MIGRATION_URL"] == "clickhouse://${{app.app-clickhouse.TCP.9000}}"
    assert env["S3_ENDPOINT"] == "${{app.app-minio.URL.9000}}"
    # Host ports published on localhost reach the publishing service.
    assert env["S3_EXTERNAL_ENDPOINT"] == "${{app.app-minio.URL.9000}}"
    assert env["NEXTAUTH_URL"] == "${{app.app-web.URL.3000}}"
    # A value that only equals a service name is not an address.
    assert env["CLICKHOUSE_USER"] == "clickhouse"


def test_managed_databases_replace_postgres_and_redis_and_require_tls(tmp_path):
    result = translate(tmp_path, LANGFUSE_STYLE)
    spec = services(result)

    assert spec["app-postgres"] == {
        "type": "database",
        "deploy": {"kind": "postgres", "always_on": True},
        "depends_on": [],
    }
    assert spec["app-redis"]["deploy"]["kind"] == "redis"
    assert spec["app-worker"]["deploy"]["env"]["REDIS_TLS_ENABLED"] == "true"
    assert "postgres: compose pins postgres 17; the managed postgres is 16" in result["warnings"]


# The TLS gateway routes by SNI, which Node's ioredis omits unless given a server
# name: a known image gets its setting and a declared one is filled in, while other
# Node images rely on the worker's preload. A URL moved onto the gateway turns its
# TLS switch on.
def test_clients_reaching_the_tls_gateway_are_configured_for_it(tmp_path):
    result = translate(
        tmp_path,
        """
        services:
          known:
            image: langfuse/langfuse:3
            environment:
              REDIS_CONNECTION_STRING: redis://redis:6379
              CLICKHOUSE_MIGRATION_URL: clickhouse://clickhouse:9000
          declared:
            image: example/declared:1
            environment: {REDIS_HOST: redis, QUEUE_REDIS_TLS_SERVERNAME: ""}
          node:
            image: example/node:1
            environment: {REDIS_HOST: redis, EVENTS_URL: "nats://events:4222", EVENTS_TLS: "false"}
          events:
            image: nats:2
            ports: ["4222:4222"]
          clickhouse:
            image: clickhouse/clickhouse-server:25.12
            ports: ["8123:8123", "9000:9000"]
          redis:
            image: redis:7
        """,
        registry=FakeRegistry({"example/node:1": {"Env": ["NODE_VERSION=20"]}}),
    )
    spec = services(result)
    server = "${{db.app-redis.HOST}}"

    known = spec["app-known"]["deploy"]["env"]
    assert known["REDIS_TLS_SERVERNAME"] == server
    assert known["CLICKHOUSE_MIGRATION_SSL"] == "true"
    assert spec["app-declared"]["deploy"]["env"]["QUEUE_REDIS_TLS_SERVERNAME"] == server
    node = spec["app-node"]["deploy"]["env"]
    assert node["EVENTS_TLS"] == "true"
    assert not any("SERVERNAME" in key for key in node)
    assert not any(w.startswith("node: ") and "server name" in w for w in result["warnings"])
    assert not any(w.startswith("declared: ") and "SNI" in w for w in result["warnings"])


# Placeholder credentials that several services must agree on become one
# generated secret; a hex key keeps its format.
def test_shared_placeholder_credentials_become_generated_secrets(tmp_path):
    result = translate(tmp_path, LANGFUSE_STYLE)
    spec = services(result)

    assert result["spec"]["secrets"] == {
        "APP_CLICKHOUSE_PASSWORD": {"length": 32},
        "APP_MINIO_ROOT_PASSWORD": {"length": 32},
        "APP_ENCRYPTION_KEY": {"length": 64, "alphabet": "0123456789abcdef"},
        "APP_NEXTAUTH_SECRET": {"length": 32},
    }
    clickhouse = "${{secret.APP_CLICKHOUSE_PASSWORD}}"
    assert spec["app-worker"]["deploy"]["env"]["CLICKHOUSE_PASSWORD"] == clickhouse
    assert spec["app-clickhouse"]["deploy"]["env"]["CLICKHOUSE_PASSWORD"] == clickhouse
    minio = "${{secret.APP_MINIO_ROOT_PASSWORD}}"
    assert spec["app-web"]["deploy"]["env"]["S3_SECRET_ACCESS_KEY"] == minio
    assert spec["app-minio"]["deploy"]["env"]["MINIO_ROOT_PASSWORD"] == minio


# ClickHouse serves HTTP on 8123 and its native protocol on 9000: it stays an
# HTTP app checked on 8123, and 9000 is reached through the TCP gateway.
def test_multi_port_servers_keep_ports_disks_and_health_checks(tmp_path):
    spec = services(translate(tmp_path, LANGFUSE_STYLE))

    clickhouse = spec["app-clickhouse"]
    assert clickhouse["health_path"] == "/ping"
    assert clickhouse["health_port"] == 8123
    assert clickhouse["deploy"]["ports"] == [8123, 9000]
    assert clickhouse["deploy"]["disks"] == ["app-clickhouse-data:/var/lib/clickhouse:10Gi"]
    assert "tcp" not in clickhouse["deploy"]

    minio = spec["app-minio"]
    assert minio["deploy"]["entrypoint"] == [
        "sh",
        "-c",
        'mkdir -p /data/bucket && minio server --address ":9000" /data',
    ]
    assert minio["health_path"] == "/minio/health/live"
    assert minio["deploy"]["ports"] == [9000]
    assert spec["app-worker"]["depends_on"] == [
        "app-clickhouse",
        "app-minio",
        "app-postgres",
        "app-redis",
    ]


# An app that reaches a dependency at an address built into it names no port, so
# only the port the dependency is checked on opens, not every one it EXPOSEs.
def test_unnamed_dependency_opens_only_its_health_port(tmp_path):
    exposed = {"ExposedPorts": {"8123/tcp": {}, "9000/tcp": {}, "9009/tcp": {}}}
    result = translate(
        tmp_path,
        """
        services:
          app:
            image: example/app:1
            ports: ["8000:8000"]
            depends_on: [events]
          events:
            image: clickhouse/clickhouse-server:24.12-alpine
        """,
        registry=FakeRegistry({"clickhouse/clickhouse-server:24.12-alpine": exposed}),
    )
    events = services(result)["app-events"]

    assert events["deploy"]["ports"] == [8123]
    assert events["health_path"] == "/ping"
    warnings = result["warnings"]
    assert any(w.startswith("events: left out EXPOSEd ports 9000, 9009:") for w in warnings)
    assert any(
        w.startswith("app: depends on events") and "${{app.app-events.URL.8123}}" in w
        for w in warnings
    )


# An image whose name alone is generic is known by its repository.
def test_known_images_are_matched_by_repository_first(tmp_path):
    spec = services(
        translate(
            tmp_path,
            """
            services:
              plausible:
                image: ghcr.io/plausible/community-edition:v3.2.0
                ports: ["8000:8000"]
              n8n:
                image: docker.n8n.io/n8nio/n8n:1.0
                ports: ["5678:5678"]
              other:
                image: example/community-edition:1
                ports: ["8000:8000"]
            """,
        )
    )

    assert spec["app-plausible"]["health_path"] == "/api/health"
    assert spec["app-n8n"]["health_path"] == "/healthz"
    assert "health_path" not in spec["app-other"]


def test_interpolation_follows_compose(tmp_path):
    missing: set = set()
    variables = {"SET": "value", "EMPTY": ""}
    assert compose.interpolate(
        "${SET:-x} ${EMPTY:-x} ${EMPTY-x} ${UNSET-x}", variables, missing
    ) == ("value x  x")
    assert compose.interpolate(
        "${SET:+alt} ${EMPTY:+alt} $$SET ${SET:-${NESTED}}", variables, missing
    ) == ("alt  $SET value")
    assert missing == set()
    assert compose.interpolate("${REQUIRED:?set it} $UNSET", variables, missing) == " "
    assert missing == {"REQUIRED", "UNSET"}

    (tmp_path / ".env").write_text("TAG=2\nexport NAME='quoted' # comment\n")
    result = translate(tmp_path, "services:\n  web:\n    image: example/${NAME}:${TAG}\n")
    assert services(result)["app-web"]["deploy"]["image"] == "example/quoted:2"


# Beam replaces the image's whole argv; compose's command replaces only CMD.
def test_a_command_keeps_the_image_entrypoint_and_portless_services_are_workers(tmp_path):
    registry = FakeRegistry({"example/queue:1": {"Entrypoint": ["/entry.sh"], "Cmd": ["serve"]}})
    result = translate(
        tmp_path,
        """
        services:
          worker:
            image: example/queue:1
            command: ["work", "--queue", "default"]
        """,
        registry=registry,
    )
    worker = services(result)["app-worker"]
    assert worker["deploy"]["entrypoint"] == ["/entry.sh", "work", "--queue", "default"]
    assert worker["deploy"]["ports"] == []
    assert "health_path" not in worker


def test_a_one_shot_service_others_wait_for_becomes_a_job(tmp_path):
    result = translate(
        tmp_path,
        """
        services:
          migrate:
            image: example/app:1
            command: ["migrate"]
          web:
            image: example/app:1
            ports: ["8000:8000"]
            depends_on:
              migrate: {condition: service_completed_successfully}
        """,
    )
    spec = services(result)
    assert spec["app-migrate"]["type"] == "job"
    assert "ports" not in spec["app-migrate"]["deploy"]
    assert spec["app-web"]["depends_on"] == ["app-migrate"]


# Bind-mounted config is baked into a derived image; any service it names by
# host must expose that port, and the file needs a reachable address.
def test_bound_files_are_baked_and_the_services_they_name_expose_ports(tmp_path):
    (tmp_path / "nginx.conf").write_text("server { location / { proxy_pass http://api:8000; } }\n")
    (tmp_path / "api").mkdir()
    (tmp_path / "api" / "Dockerfile").write_text("FROM python:3.12\nCMD python -m app\n")
    result = translate(
        tmp_path,
        """
        services:
          proxy:
            image: nginx:1.27
            ports: ["80:80"]
            volumes: ["./nginx.conf:/etc/nginx/conf.d/default.conf:ro"]
          api:
            build: ./api
        """,
    )
    spec = services(result)
    proxy = spec["app-proxy"]["deploy"]
    assert "image" not in proxy and proxy["dockerfile"] == "Dockerfile"
    assert (Path(proxy["directory"]) / "Dockerfile").read_text() == (
        "FROM nginx:1.27\nCOPY bind0 /etc/nginx/conf.d/default.conf\n"
    )
    assert spec["app-api"]["deploy"]["ports"] == [8000]
    assert any(
        warning.startswith("proxy: nginx.conf names api:8000") for warning in result["warnings"]
    )


# The CLI builds from the Dockerfile's directory, the final stage, and ARG
# defaults; compose may say otherwise for each.
def test_builds_keep_their_context_target_and_args(tmp_path):
    (tmp_path / "docker").mkdir()
    (tmp_path / "docker" / "Dockerfile").write_text(
        "ARG VERSION=1\nFROM python:3.12 AS app\nEXPOSE 8000\nFROM app AS test\nRUN pytest\n"
    )
    result = translate(
        tmp_path,
        """
        services:
          web:
            build:
              context: .
              dockerfile: docker/Dockerfile
              target: app
              args: {VERSION: "2"}
        """,
        registry=FakeRegistry({"python:3.12": {"Cmd": ["python3"]}}),
    )
    deploy = services(result)["app-web"]["deploy"]
    build = ("directory", "dockerfile", "context_dir", "target", "build_args")
    assert {key: deploy[key] for key in build} == {
        "directory": str(tmp_path),
        "dockerfile": "docker/Dockerfile",
        "context_dir": ".",
        "target": "app",
        "build_args": {"VERSION": "2"},
    }
    assert not any("target" in warning for warning in result.get("warnings", []))

    plain = translate(
        tmp_path,
        "services:\n  web:\n    build: {context: ., dockerfile: docker/Dockerfile}\n",
        registry=FakeRegistry({"python:3.12": {}}),
    )
    deploy = services(plain)["app-web"]["deploy"]
    assert (deploy["dockerfile"], deploy["context_dir"]) == ("docker/Dockerfile", ".")


# `changethis` can stand for unrelated secrets; settings are one credential only
# when they read the same variable or a client's endpoint names the server.
def test_a_placeholder_is_one_secret_per_credential(tmp_path):
    (tmp_path / ".env").write_text("DB_PASSWORD=changethis\n")
    result = translate(
        tmp_path,
        """
        services:
          api:
            image: example/api:1
            env_file: .env
            environment:
              SECRET_KEY: changethis
              FIRST_SUPERUSER_PASSWORD: changethis
          database:
            image: example/postgres-with-extensions:14
            environment:
              POSTGRES_PASSWORD: ${DB_PASSWORD}
        """,
    )
    spec = services(result)
    api, database = spec["app-api"]["deploy"]["env"], spec["app-database"]["deploy"]["env"]
    assert set(result["spec"]["secrets"]) == {
        "APP_SECRET_KEY",
        "APP_FIRST_SUPERUSER_PASSWORD",
        "APP_DB_PASSWORD",
    }
    assert api["SECRET_KEY"] != api["FIRST_SUPERUSER_PASSWORD"]
    assert api["DB_PASSWORD"] == database["POSTGRES_PASSWORD"] == "${{secret.APP_DB_PASSWORD}}"


# What compose leaves to the operator's .env: a credential is generated, a public
# URL is the app's own, and an empty connection URL is its one managed database.
# An empty optional integration stays empty.
def test_unset_settings_are_filled_from_the_stack(tmp_path):
    result = translate(
        tmp_path,
        """
        services:
          web:
            image: example/web:1
            ports: ["3000:3000"]
            depends_on: [db]
            environment:
              SECRET_KEY_BASE: ${SECRET_KEY_BASE:?required}
              BASE_URL: ${BASE_URL}
              DATABASE_URL:
              SMTP_PASSWORD: ${SMTP_PASSWORD:-}
          db:
            image: postgres:16
        """,
    )
    env = services(result)["app-web"]["deploy"]["env"]
    assert env["SECRET_KEY_BASE"] == "${{secret.APP_SECRET_KEY_BASE}}"
    assert env["BASE_URL"] == "${{app.app-web.URL}}"
    assert env["DATABASE_URL"] == "${{db.app-db.DATABASE_URL}}"
    assert env["SMTP_PASSWORD"] == ""
    assert not any(warning.startswith("unset variables") for warning in result["warnings"])


# DB_HOST=postgres for a self-hosted database becomes the host and port halves of
# its TCP gateway address; the client must switch TLS on, and the empty password
# both sides read is generated.
def test_a_host_setting_naming_an_app_gets_its_tcp_address(tmp_path):
    (tmp_path / ".env.example").write_text("POSTGRES_PASSWORD=\n")
    result = translate(
        tmp_path,
        """
        services:
          rails:
            image: example/rails:1
            env_file: .env
            environment:
              POSTGRES_HOST: postgres
              POSTGRES_SSLMODE: disable
          postgres:
            image: pgvector/pgvector:pg16
            expose: ["5432"]
            environment:
              POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
        """,
    )
    spec = services(result)
    rails = spec["app-rails"]["deploy"]["env"]
    assert rails["POSTGRES_HOST"] == "${{app.app-postgres.HOST.5432}}"
    assert rails["POSTGRES_PORT"] == "${{app.app-postgres.PORT.5432}}"
    assert rails["POSTGRES_SSLMODE"] == "require"
    password = "${{secret.APP_POSTGRES_PASSWORD}}"
    assert (
        rails["POSTGRES_PASSWORD"]
        == spec["app-postgres"]["deploy"]["env"]["POSTGRES_PASSWORD"]
        == password
    )
    assert ".env is missing; read .env.example instead" in result["warnings"]


# An app given only a managed database's host would fall back to defaults (port
# 6379, no password) that the managed database does not match.
def test_a_managed_database_host_brings_its_port_and_password(tmp_path):
    result = translate(
        tmp_path,
        """
        services:
          worker:
            image: example/worker:1
            environment:
              QUEUE_REDIS_HOST: redis
          redis:
            image: redis:7
        """,
    )
    env = services(result)["app-worker"]["deploy"]["env"]
    assert env["QUEUE_REDIS_PORT"] == "${{db.app-redis.PORT}}"
    assert env["QUEUE_REDIS_PASSWORD"] == "${{db.app-redis.PASSWORD}}"


def test_extra_compose_files_merge_like_docker_compose(tmp_path):
    (tmp_path / "compose.prod.yaml").write_text(
        textwrap.dedent(
            """
            services:
              web:
                command: ["serve", "--prod"]
                ports: ["9000:9000"]
                environment: ["MODE=prod"]
                volumes: ["prod_data:/data"]
                healthcheck: !reset null
            volumes:
              prod_data:
            """
        )
    )
    result = translate(
        tmp_path,
        """
        services:
          web:
            image: example/web:1
            command: ["serve"]
            ports: ["8000:8000"]
            environment: {MODE: dev, KEEP: "1"}
            volumes: ["dev_data:/data"]
            healthcheck:
              test: curl -f http://localhost:8000/health
        volumes:
          dev_data:
        """,
        files=["compose.prod.yaml"],
    )
    deploy = services(result)["app-web"]["deploy"]
    assert deploy["entrypoint"] == ["serve", "--prod"]
    assert deploy["ports"] == [8000, 9000]
    assert (deploy["env"]["MODE"], deploy["env"]["KEEP"]) == ("prod", "1")
    assert deploy["disks"] == ["app-prod-data:/data:10Gi"]
    assert "health_path" not in services(result)["app-web"]
    assert result["merged"] == ["compose.prod.yaml"]


def test_services_that_cannot_or_need_not_run_are_left_out(tmp_path):
    result = translate(
        tmp_path,
        """
        services:
          proxy:
            image: traefik:v3
            volumes: ["/var/run/docker.sock:/var/run/docker.sock"]
          base: &base
            image: example/rails:1
            environment: {RAILS_ENV: production}
          rails:
            <<: *base
            command: ["rails", "s"]
            ports: ["3000:3000"]
        """,
    )
    assert list(services(result)) == ["app-rails"]
    assert any(warning.startswith("left out proxy:") for warning in result["warnings"])
    assert any(warning.startswith("left out base:") for warning in result["warnings"])


# Compose resolves `database` on its private network; here that address must be
# set explicitly, the database must listen on its image's port, and loops that
# wait for it by host never finish.
def test_addresses_built_into_an_app_are_flagged(tmp_path):
    registry = FakeRegistry({"example/postgres:14": {"ExposedPorts": {"5432/tcp": {}}}})
    result = translate(
        tmp_path,
        """
        services:
          server:
            image: example/server:1
            command: ["sh", "-c", "until </dev/tcp/database/5432; do sleep 1; done; serve"]
            depends_on: [database]
          database:
            image: example/postgres:14
        """,
        registry=registry,
    )
    assert services(result)["app-database"]["deploy"]["ports"] == [5432]
    warnings = "\n".join(result["warnings"])
    assert "server: depends on database, but no env value points at it" in warnings
    assert "${{app.app-database.HOST.5432}} and ${{app.app-database.PORT.5432}}" in warnings
    assert "server: its command names another service (/dev/tcp/database)" in warnings


def test_ports_compose_kept_private_are_flagged_when_they_become_public(tmp_path):
    result = translate(tmp_path, LANGFUSE_STYLE)
    warnings = result["warnings"]
    assert any(
        warning.startswith("clickhouse: ports 8123, 9000 were private") for warning in warnings
    )
    assert not any(warning.startswith("web: port") for warning in warnings)
    # The MinIO console nothing uses stays out; the worker's only port stays in.
    assert any(warning.startswith("minio: left out port 9001") for warning in warnings)
    assert services(result)["app-worker"]["deploy"]["ports"] == [3030]
    assert any(warning.startswith("worker: port 3030 was private") for warning in warnings)


def test_images_on_docker_hub_proxies_are_pulled_from_docker_hub(tmp_path):
    result = translate(
        tmp_path,
        """
        services:
          web:
            image: docker.acme.dev/acme/web:4
            ports: ["3000:3000"]
          api:
            image: ghcr.io/acme/api:1
            ports: ["8000:8000"]
        """,
        registry=FakeRegistry(proxies={"docker.acme.dev"}),
    )
    spec = services(result)
    assert spec["app-web"]["deploy"]["image"] == "docker.io/acme/web:4"
    assert spec["app-api"]["deploy"]["image"] == "ghcr.io/acme/api:1"
    assert any(
        warning.startswith("web: pulling docker.io/acme/web:4: docker.acme.dev only proxies")
        for warning in result["warnings"]
    )


class Pings:
    """A session answering registry pings: (final URL, WWW-Authenticate) by URL."""

    def __init__(self, answers: Dict[str, Any]):
        self.answers = answers
        self.calls: list = []

    def get(self, url: str, **_: Any) -> Any:
        self.calls.append(url)
        final, challenge = self.answers[url]
        return type("Response", (), {"url": final, "headers": {"WWW-Authenticate": challenge}})


def test_a_registry_that_redirects_or_authenticates_with_docker_hub_is_a_proxy():
    hub = 'Bearer realm="https://auth.docker.io/token",service="registry.docker.io"'
    registry = compose.Registry()
    registry.session = Pings(
        {
            "https://docker.acme.dev/v2/": ("https://registry-1.docker.io/v2/", hub),
            "https://docker.n8n.dev/v2/": ("https://docker.n8n.dev/v2/", hub),
            "https://ghcr.io/v2/": (
                "https://ghcr.io/v2/",
                'Bearer realm="https://ghcr.io/token",service="ghcr.io"',
            ),
        }
    )
    assert registry.docker_hub_image("docker.acme.dev/acme/web:4") == "docker.io/acme/web:4"
    assert (
        registry.docker_hub_image("docker.n8n.dev/n8nio/n8n@sha256:abc")
        == "docker.io/n8nio/n8n@sha256:abc"
    )
    assert registry.docker_hub_image("ghcr.io/acme/api:1") is None
    assert registry.docker_hub_image("redis:7") is None
    assert registry.docker_hub_image("docker.io/library/redis:7") is None
    registry.docker_hub_image("docker.acme.dev/acme/worker:4")
    assert registry.session.calls.count("https://docker.acme.dev/v2/") == 1


def test_a_project_without_services_is_rejected(tmp_path):
    with pytest.raises(compose.ComposeError, match="defines no services"):
        translate(tmp_path, "volumes: {}\n")
