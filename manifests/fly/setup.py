import json
import os
import secrets
import shutil
import subprocess
from dataclasses import dataclass
from uuid import uuid4


@dataclass
class PostgresConfig:
    app_name: str
    password: str
    user: str
    db: str


@dataclass
class RedisConfig:
    app_name: str
    password: str


@dataclass
class StorageConfig:
    storage_name: str
    access_key: str
    secret_key: str
    endpoint: str
    bucket: str


@dataclass
class TailscaleConfig:
    host: str
    auth_token: str


def generate_name(prefix):
    return f"{prefix}-{uuid4()}"


def generate_password():
    return secrets.token_urlsafe(24)


def setup_postgres() -> PostgresConfig:
    app_name = generate_name("control-plane-postgres")
    postgres_password = generate_password()
    postgres_user = "postgres"
    postgres_db = "main"

    subprocess.run(
        [
            "fly",
            "launch",
            "--no-deploy",
            "--copy-config",
            "--name",
            app_name,
            "-y",
        ],
        cwd="state/postgres",
        env=os.environ,
    )
    subprocess.run(
        [
            "fly",
            "secrets",
            "set",
            f"POSTGRES_PASSWORD={postgres_password}",
            f"POSTGRES_USER={postgres_user}",
            f"POSTGRES_DB={postgres_db}",
            f"SU_PASSWORD={postgres_password}",
            f"OPERATOR_PASSWORD={postgres_password}",
            f"REPL_PASSWORD={postgres_password}",
        ],
        cwd="state/postgres",
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v4", "-y"],
        cwd="state/postgres",
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v6"],
        cwd="state/postgres",
        env=os.environ,
    )
    subprocess.run(
        ["fly", "deploy", "-y"],
        cwd="state/postgres",
        env=os.environ,
    )

    return PostgresConfig(app_name, postgres_password, postgres_user, postgres_db)


def setup_redis(name) -> RedisConfig:
    app_name = generate_name(f"redis-{name}")
    redis_password = generate_password()

    subprocess.run(
        [
            "fly",
            "launch",
            "--no-deploy",
            "--copy-config",
            "--name",
            app_name,
            "-y",
        ],
        cwd="state/redis-" + name,
        env=os.environ,
    )
    subprocess.run(
        [
            "fly",
            "secrets",
            "set",
            f"REDIS_PASSWORD={redis_password}",
        ],
        cwd="state/redis-" + name,
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v4", "-y"],
        cwd="state/redis-" + name,
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v6"],
        cwd="state/redis-" + name,
        env=os.environ,
    )
    subprocess.run(
        ["fly", "deploy", "-y"],
        cwd="state/redis-" + name,
        env=os.environ,
    )

    return RedisConfig(app_name, redis_password)


def setup_storage(name) -> StorageConfig:
    storage_name = generate_name(f"storage-{name}")
    access_key = secret_key = endpoint = bucket = None

    res = subprocess.run(
        ["fly", "storage", "create", "-n", storage_name, "-y", "-o", "personal"],
        env=os.environ,
        text=True,
        check=True,
        capture_output=True,
    )

    res_lines = res.stdout.splitlines()

    for line in res_lines:
        if "AWS_ACCESS_KEY_ID" in line:
            access_key = line.split(":")[1].strip()
        if "AWS_SECRET_ACCESS_KEY" in line:
            secret_key = line.split(":")[1].strip()
        if "AWS_ENDPOINT_URL_S3" in line:
            endpoint = line.split(":")[1].strip()
        if "BUCKET_NAME" in line:
            bucket = line.split(":")[1].strip()

    return StorageConfig(storage_name, access_key, secret_key, endpoint, bucket)


def generate_config_file(
    gateway_app_name: str,
    tailscale_config: TailscaleConfig,
    postgres_config: PostgresConfig,
    cp_redis_config: RedisConfig,
    bc_redis_config: RedisConfig,
    juicefs_redis_config: RedisConfig,
    control_plane_storage_config: StorageConfig,
    images_storage_config: StorageConfig,
):
    with open("./state/gateway/config.tpl.json", "r") as f:
        config = json.load(f)
        postgres = config["database"]["postgres"]
        postgres["host"] = f"{postgres_config.app_name}.fly.dev"
        postgres["password"] = postgres_config.password
        postgres["username"] = postgres_config.user
        postgres["name"] = postgres_config.db

        cp_redis = config["database"]["redis"]
        cp_redis["addrs"] = [f"{cp_redis_config.app_name}.fly.dev:6379"]
        cp_redis["password"] = cp_redis_config.password

        cp_storage = config["storage"]["juicefs"]
        cp_storage["redis_uri"] = (
            f"redis://:{juicefs_redis_config.password}@{juicefs_redis_config.app_name}.fly.dev:6379"
        )
        cp_storage["aws_s3_bucket"] = (
            f"https://fly.storage.tigris.dev/{control_plane_storage_config.bucket}"
        )
        cp_storage["aws_access_key"] = control_plane_storage_config.access_key
        cp_storage["aws_secret_key"] = control_plane_storage_config.secret_key

        gateway_service = config["gateway_service"]
        gateway_service["external_host"] = f"{gateway_app_name}.fly.dev"

        image_service = config["image_service"]
        image_service["registries"]["s3"]["bucket_name"] = images_storage_config.bucket
        image_service["registries"]["s3"]["access_key"] = images_storage_config.access_key
        image_service["registries"]["s3"]["secret_key"] = images_storage_config.secret_key

        tailscale = config["tailscale"]
        tailscale["host_name"] = tailscale_config.host
        tailscale["auth_key"] = tailscale_config.auth_token

        blobcache = config["blobcache"]
        blobcache["tailscale"]["host_name"] = tailscale_config.host
        blobcache["tailscale"]["auth_key"] = tailscale_config.auth_token
        blobcache["metadata"]["redis_addr"] = f"{bc_redis_config.app_name}.fly.dev:6379"
        blobcache["metadata"]["redis_passwd"] = bc_redis_config.password

        blobcache["blobfs"]["sources"] = [
            {
                "mode": "juicefs",
                "fs_name": "beta9-fs",
                "fs_path": "/data",
                "juicefs": {
                    "redis_uri": f"redis://:{juicefs_redis_config.password}@{juicefs_redis_config.app_name}.fly.dev:6379",
                    "bucket": f"https://fly.storage.tigris.dev/{control_plane_storage_config.bucket}",
                    "access_key": control_plane_storage_config.access_key,
                    "secret_key": control_plane_storage_config.secret_key,
                    "cache_size": 0,
                    "block_size": 16384,
                    "prefetch": 128,
                    "buffer_size": 300,
                },
            },
            {
                "mode": "mountpoint",
                "fs_name": "beta9-images",
                "fs_path": "/images",
                "mountpoint": {
                    "bucket_name": images_storage_config.bucket,
                    "access_key": images_storage_config.access_key,
                    "secret_key": images_storage_config.secret_key,
                    "region": "auto",
                    "endpoint_url": "https://fly.storage.tigris.dev",
                },
            },
        ]

        return config


def setup_gateway(
    postgres_config: PostgresConfig,
    cp_redis_config: RedisConfig,
    bc_redis_config: RedisConfig,
    juicefs_redis_config: RedisConfig,
    control_plane_storage_config: StorageConfig,
    images_storage_config: StorageConfig,
):
    tailscale_config = TailscaleConfig(
        host=os.environ.get("TAILSCALE_HOST", ""),
        auth_token=os.environ.get("TAILSCALE_AUTH_TOKEN", ""),
    )

    gateway_app_name = generate_name("control-plane-gateway")

    cfg = generate_config_file(
        gateway_app_name,
        tailscale_config,
        postgres_config,
        cp_redis_config,
        bc_redis_config,
        juicefs_redis_config,
        control_plane_storage_config,
        images_storage_config,
    )

    with open("./state/gateway/config.json", "w") as f:
        json.dump(cfg, f, indent=2)

    subprocess.run(
        [
            "fly",
            "launch",
            "--no-deploy",
            "--copy-config",
            "--name",
            gateway_app_name,
            "-y",
        ],
        cwd="state/gateway",
        env=os.environ,
    )

    subprocess.run(
        [
            "fly",
            "secrets",
            "set",
            f"CONFIG_JSON={json.dumps(cfg)}",
        ],
        cwd="state/gateway",
        env=os.environ,
    )

    subprocess.run(
        ["fly", "ip", "allocate-v4", "-y"],
        cwd="state/gateway",
        env=os.environ,
    )

    subprocess.run(
        ["fly", "ip", "allocate-v6"],
        cwd="state/gateway",
        env=os.environ,
    )

    subprocess.run(
        ["fly", "deploy", "-y"],
        cwd="state/gateway",
        env=os.environ,
    )


if __name__ == "__main__":
    shutil.rmtree("state/", ignore_errors=True)
    shutil.copytree("template/", "state/", dirs_exist_ok=True)

    pg_cfg = setup_postgres()
    cp_redis_cfg = setup_redis(name="control-plane")
    bc_redis_cfg = setup_redis(name="blobcache")
    juicefs_redis_cfg = setup_redis(name="juicefs")
    cp_storage_cfg = setup_storage("control-plane")
    images_storage_cfg = setup_storage("images")

    setup_gateway(
        pg_cfg, cp_redis_cfg, bc_redis_cfg, juicefs_redis_cfg, cp_storage_cfg, images_storage_cfg
    )
