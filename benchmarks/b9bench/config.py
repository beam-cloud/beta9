from __future__ import annotations

import configparser
import os
import time
from dataclasses import dataclass
from pathlib import Path

from .model import RunConfig, slug


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def normalize_local_host(host: str) -> str:
    if host in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return host


def http_url_from_gateway(host: str, port: str) -> str:
    if host.startswith(("http://", "https://")):
        return host
    if port == "443" and host.startswith("gateway.") and host.endswith(".beam.cloud"):
        http_host = "app." + host[len("gateway."):]
        return f"https://{http_host}"
    if port == "1993":
        return f"http://{host}:1994"
    if port == "80":
        return f"http://{host}"
    if port == "443":
        return f"https://{host}"
    return f"http://{host}:{port}"


@dataclass(frozen=True)
class ProfileConfig:
    token: str = ""
    gateway_host: str = ""
    gateway_port: str = ""
    http_url: str = ""


def read_profile(config_path: Path, profile: str) -> ProfileConfig:
    parser = configparser.ConfigParser()
    parser.read(config_path.expanduser())
    if not parser.has_section(profile):
        return ProfileConfig()
    section = parser[profile]
    http_url = section.get("gateway_http_url", "") or section.get("http_url", "")
    http_host = section.get("gateway_http_host", "") or section.get("http_host", "")
    http_port = section.get("gateway_http_port", "") or section.get("http_port", "")
    if not http_url and http_host:
        http_url = http_url_from_gateway(normalize_local_host(http_host), http_port or "443")

    return ProfileConfig(
        token=section.get("token", ""),
        gateway_host=section.get("gateway_host", ""),
        gateway_port=section.get("gateway_port", ""),
        http_url=http_url.strip(),
    )


def parse_scalar(value: str):
    lowered = value.strip().lower()
    if lowered in {"true", "yes", "on"}:
        return True
    if lowered in {"false", "no", "off"}:
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def parse_key_value(values: list[str]) -> dict[str, object]:
    out: dict[str, object] = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(f"expected KEY=VALUE for --param, got {value!r}")
        key, raw = value.split("=", 1)
        key = key.strip()
        if not key:
            raise SystemExit(f"empty parameter name in {value!r}")
        out[key] = parse_scalar(raw)
    return out


def default_benchmark_out_dir(root: Path, command: str, suite_name: str, profile: str) -> Path:
    suite_context = suite_name
    if "/" in suite_context or "\\" in suite_context:
        suite_context = Path(suite_context).stem
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_context = "-".join(
        part
        for part in (
            timestamp,
            slug(command),
            slug(suite_context),
            slug(profile),
        )
        if part
    )
    return root / "benchmarks" / "runs" / run_context


def resolve_run_config(args) -> RunConfig:
    root = repo_root()
    profile = (
        args.profile
        or os.getenv("CACHE_BENCH_PROFILE")
        or os.getenv("BENCH_PROFILE")
        or os.getenv("BETA9_PROFILE")
        or "default"
    )
    config_path = Path(
        args.config
        or os.getenv("CACHE_BENCH_CONFIG")
        or os.getenv("BENCH_CONFIG")
        or os.getenv("CONFIG_PATH")
        or "~/.beta9/config.ini"
    ).expanduser()

    profile_config = ProfileConfig()
    if config_path.exists():
        profile_config = read_profile(config_path, profile)

    token = (
        args.token
        or os.getenv("BENCH_TOKEN")
        or os.getenv("TOKEN")
        or os.getenv("BETA9_TOKEN")
        or profile_config.token
    )
    grpc_addr = args.grpc_addr or os.getenv("BENCH_GRPC_ADDR")
    gateway_url = args.gateway_url or os.getenv("BENCH_GATEWAY_URL")
    if profile_config.gateway_host and profile_config.gateway_port:
        host = normalize_local_host(profile_config.gateway_host)
        grpc_addr = grpc_addr or f"{host}:{profile_config.gateway_port}"
        gateway_url = gateway_url or profile_config.http_url or http_url_from_gateway(host, profile_config.gateway_port)

    raw_out_dir = args.out_dir or os.getenv("BENCH_OUT_DIR")
    out_dir = (
        Path(raw_out_dir).expanduser()
        if raw_out_dir
        else default_benchmark_out_dir(root, args.command, args.suite, profile)
    )

    extra_args = parse_key_value(args.param or [])
    env_file_plan = os.getenv("CACHE_BENCHMARK_FILE_PLAN") or os.getenv("BENCH_CACHE_FILE_PLAN")
    if env_file_plan and "file_plan" not in extra_args:
        extra_args["file_plan"] = env_file_plan

    return RunConfig(
        root=root,
        command=args.command,
        suite_name=args.suite,
        profile=profile,
        config_path=config_path,
        namespace=args.namespace or os.getenv("BENCH_NAMESPACE", "beta9"),
        gateway_url=gateway_url or "http://127.0.0.1:1994",
        grpc_addr=grpc_addr or "127.0.0.1:1993",
        token=token,
        out_dir=out_dir,
        dry_run=bool(args.dry_run),
        extra_args=extra_args,
        passthrough_args=tuple(args.script_arg or ()),
    )
