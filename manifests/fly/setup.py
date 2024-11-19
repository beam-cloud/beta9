import json
import os
import random
import subprocess
from dataclasses import dataclass


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


def setup_postgres() -> PostgresConfig:
    app_name = "control-plane-postgres"
    postgres_password = "password"
    postgres_user = "postgres"
    postgres_db = "main"

    return PostgresConfig(app_name, postgres_password, postgres_user, postgres_db)

    subprocess.run(
        [
            "fly",
            "launch",
            "--no-deploy",
            "--copy-config",
            "-y",
        ],
        cwd="postgres",
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
        ],
        cwd="postgres",
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v4", "--shared"],
        cwd="postgres",
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v6"],
        cwd="postgres",
        env=os.environ,
    )
    subprocess.run(
        ["fly", "deploy", "-y"],
        cwd="postgres",
        env=os.environ,
    )

    return PostgresConfig(app_name, postgres_password, postgres_user, postgres_db)


def setup_redis(name) -> RedisConfig:
    app_name = f"redis-{name}"
    redis_password = "password"

    return RedisConfig(app_name, redis_password)

    subprocess.run(
        [
            "fly",
            "launch",
            "--no-deploy",
            "--copy-config",
            "-y",
        ],
        cwd=app_name,
        env=os.environ,
    )
    subprocess.run(
        [
            "fly",
            "secrets",
            "set",
            f"REDIS_PASSWORD={redis_password}",
        ],
        cwd=app_name,
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v4", "--shared"],
        cwd=app_name,
        env=os.environ,
    )
    subprocess.run(
        ["fly", "ip", "allocate-v6"],
        cwd=app_name,
        env=os.environ,
    )
    subprocess.run(
        ["fly", "deploy", "-y"],
        cwd=app_name,
        env=os.environ,
    )

    return RedisConfig(app_name, redis_password)


def setup_storage(name) -> StorageConfig:
    access_key = secret_key = endpoint = bucket = None
    storage_name = f"storage-{name}-{random.randint(0, 1000)}"  # randomize the storage name

    return StorageConfig(storage_name, access_key, secret_key, endpoint, bucket)

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
    with open("./gateway/config.tpl.json", "r") as f:
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
            f"redis://:{juicefs_redis_cfg.password}@{juicefs_redis_cfg.app_name}.fly.dev:6379"
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
        host="gateway",
        auth_token="tailscale-auth-token",
    )

    gateway_app_name = "gateway"

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

    with open("./gateway/config.json", "w") as f:
        json.dump(cfg, f, indent=2)


if __name__ == "__main__":
    pg_cfg = setup_postgres()
    cp_redis_cfg = setup_redis(name="control-plane")
    bc_redis_cfg = setup_redis(name="blobcache")
    juicefs_redis_cfg = setup_redis(name="juicefs")
    cp_storage_cfg = setup_storage("control-plane")
    images_storage_cfg = setup_storage("images")

    setup_gateway(
        pg_cfg, cp_redis_cfg, bc_redis_cfg, juicefs_redis_cfg, cp_storage_cfg, images_storage_cfg
    )
