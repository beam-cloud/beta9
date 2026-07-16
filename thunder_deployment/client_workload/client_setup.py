import os
from pathlib import Path

from beta9 import config as beta9_config


ENV_PATH = Path(__file__).with_name(".env")


def load_env(path: Path = ENV_PATH) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        os.environ[key] = value


def resolve_env_path(name: str) -> None:
    value = os.environ.get(name)
    if not value:
        return

    path = Path(value).expanduser()
    if not path.is_absolute():
        path = ENV_PATH.parent / path
    os.environ[name] = str(path)


def normalize_beam_env() -> None:
    token = os.environ.get("BEAM_TOKEN") or os.environ.get("BETA9_TOKEN")
    gateway_host = os.environ.get("GATEWAY_HOST") or os.environ.get("BETA9_GATEWAY_HOST")
    gateway_port = os.environ.get("GATEWAY_PORT") or os.environ.get("BETA9_GATEWAY_PORT")

    if token:
        os.environ["BEAM_TOKEN"] = token
        os.environ["BETA9_TOKEN"] = token
    if gateway_host:
        os.environ["GATEWAY_HOST"] = gateway_host
        os.environ["BETA9_GATEWAY_HOST"] = gateway_host
    if gateway_port:
        os.environ["GATEWAY_PORT"] = gateway_port
        os.environ["BETA9_GATEWAY_PORT"] = gateway_port

    resolve_env_path("CONFIG_PATH")

    CONFIG_PATH = os.environ.get("CONFIG_PATH")
    if CONFIG_PATH:
        beta9_config.get_settings().config_path = Path(CONFIG_PATH).expanduser()


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

