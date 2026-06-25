#!/usr/bin/env python3
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from beta9 import Image, LLMConfig, Service


REPO_FULL_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
GIT_REF_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RESULT_PREFIX = os.getenv("BETA9_GITHUB_RESULT_PREFIX", "__BETA9_GITHUB_DEPLOY_RESULT__ ")
DEFAULT_LLM_BASE_IMAGE = "vllm/vllm-openai:v0.10.2"


class BuilderError(ValueError):
    pass


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise BuilderError(f"{name} is required")
    return value


def optional_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def validate_repo_full_name(value: str) -> str:
    if not REPO_FULL_NAME_RE.match(value):
        raise BuilderError("GIT_REPO_FULL_NAME must be owner/repo")
    return value


def validate_git_ref(value: str) -> str:
    if not value or value.startswith("-") or ".." in value or not GIT_REF_RE.match(value):
        raise BuilderError("GIT_REF is invalid")
    return value


def validate_relative_path(value: str, default: str = "") -> str:
    value = (value or default).strip()
    if not value:
        return ""
    path = Path(value)
    if path.is_absolute() or "\x00" in value or any(part == ".." for part in path.parts):
        raise BuilderError(f"{value} must be a relative path")
    return value


def parse_int(name: str, default: Optional[int] = None, minimum: Optional[int] = None) -> int:
    value = os.getenv(name, "")
    if value == "" and default is not None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise BuilderError(f"{name} must be an integer") from exc
    if minimum is not None and parsed < minimum:
        raise BuilderError(f"{name} must be greater than or equal to {minimum}")
    return parsed


def parse_cpu(name: str = "CPU") -> object:
    value = required_env(name)
    if re.match(r"^\d+$", value):
        return int(value)
    if re.match(r"^\d+\.\d+$", value):
        return float(value)
    return value


def parse_env_vars(raw: str) -> Dict[str, str]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BuilderError("ENV_VARS_JSON must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise BuilderError("ENV_VARS_JSON must be an object")

    env_vars: Dict[str, str] = {}
    for key, value in parsed.items():
        if not isinstance(key, str) or not ENV_KEY_RE.match(key):
            raise BuilderError(f"Invalid env var key: {key}")
        env_vars[key] = "" if value is None else str(value)
    return env_vars


def service_gpu(config: Dict[str, object]) -> str:
    gpu = str(config.get("gpu") or "")
    pool = str(config.get("pool") or "")
    if gpu and pool:
        return "any"
    return gpu


def git_remote_url(repo_full_name: str) -> str:
    return f"https://github.com/{repo_full_name}.git"


def git_askpass_script(token: str, directory: Path) -> Path:
    script = directory / "git-askpass.sh"
    script.write_text(
        "#!/bin/sh\n"
        "case \"$1\" in\n"
        "  *Username*) printf '%s\\n' 'x-access-token' ;;\n"
        f"  *) printf '%s\\n' {shell_single_quote(token)} ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    script.chmod(0o700)
    return script


def shell_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def run(cmd: List[str], *, cwd: Optional[Path] = None, env: Optional[Dict[str, str]] = None) -> None:
    print("+ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def clone_repo(repo_full_name: str, ref: str, token: str, clone_dir: Path) -> None:
    askpass_dir = clone_dir.parent / "askpass"
    askpass_dir.mkdir(mode=0o700, exist_ok=True)
    askpass = git_askpass_script(token, askpass_dir)

    env = os.environ.copy()
    env["GIT_ASKPASS"] = str(askpass)
    env["GIT_TERMINAL_PROMPT"] = "0"

    run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--single-branch",
            "--branch",
            ref,
            git_remote_url(repo_full_name),
            str(clone_dir),
        ],
        env=env,
    )


@contextmanager
def sdk_local_image_materialization():
    container_id_present = "CONTAINER_ID" in os.environ
    previous_container_id = os.environ.pop("CONTAINER_ID", None)
    try:
        yield
    finally:
        if container_id_present and previous_container_id is not None:
            os.environ["CONTAINER_ID"] = previous_container_id


def build_service(repo_dir: Path, config: Dict[str, object]) -> Service:
    working_dir = repo_dir / str(config["working_dir"])
    dockerfile_path = str(config["dockerfile_path"])
    env_vars = config["env_vars"]
    assert isinstance(env_vars, dict)

    with sdk_local_image_materialization():
        service = Service.from_dockerfile(
            dockerfile=dockerfile_path,
            context_dir=".",
            app=str(config["app_name"]),
            name=str(config["app_name"]),
            ports=[int(config["port"])],
            cpu=config["cpu"],
            memory=str(config["memory"]),
            gpu=service_gpu(config),
            env=env_vars,
            keep_warm_seconds=int(config["keep_warm_seconds"]),
            min_replicas=int(config["min_replicas"]),
            max_replicas=int(config["max_replicas"]),
            pool=str(config["pool"]) if config["pool"] else None,
        )

    service.image.dockerfile_path = dockerfile_path
    service.concurrent_requests = int(config["concurrent_requests"])
    return service


def build_model_service(config: Dict[str, object]) -> Service:
    model_id = str(config["model_id"])
    engine = str(config["engine"] or "vllm")
    port = int(config["port"])
    served_model_name = str(config["served_model_name"] or model_id)
    context_length = int(config["context_length"])
    base_image = str(config["base_image"] or DEFAULT_LLM_BASE_IMAGE)

    if engine != "vllm":
        raise BuilderError("Only vLLM model deploys are supported by this builder")

    command = [
        "python3",
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--model",
        model_id,
        "--served-model-name",
        served_model_name,
    ]
    if context_length > 0:
        command.extend(["--max-model-len", str(context_length)])

    image = Image.from_registry(base_image)

    with sdk_local_image_materialization():
        service = Service(
            app=str(config["app_name"]),
            name=str(config["app_name"]),
            entrypoint=command,
            ports=[port],
            cpu=config["cpu"],
            memory=str(config["memory"]),
            gpu=service_gpu(config),
            image=image,
            env=config["env_vars"],
            keep_warm_seconds=int(config["keep_warm_seconds"]),
            min_replicas=int(config["min_replicas"]),
            max_replicas=int(config["max_replicas"]),
            pool=str(config["pool"]) if config["pool"] else None,
            app_kind="llm_model",
            serving_protocol="openai",
            llm=LLMConfig(
                model_id=model_id,
                engine=engine,
                served_model_name=served_model_name,
                context_length=context_length,
                tokenizer=str(config["tokenizer"]),
                metrics_path=str(config["metrics_path"]),
                slo_tier=str(config["slo_tier"]),
            ),
        )
    service.concurrent_requests = int(config["concurrent_requests"])
    return service


def load_common_deploy_config() -> Dict[str, object]:
    port_raw = optional_env("INTERNAL_PORT") or optional_env("PORT") or "8000"
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise BuilderError("INTERNAL_PORT must be an integer") from exc
    if port < 1:
        raise BuilderError("INTERNAL_PORT must be greater than or equal to 1")
    if port > 65535:
        raise BuilderError("INTERNAL_PORT must be less than or equal to 65535")

    min_replicas = parse_int("MIN_REPLICAS", default=0, minimum=0)
    max_replicas = parse_int("MAX_REPLICAS", default=max(1, min_replicas), minimum=0)
    if max_replicas < min_replicas:
        raise BuilderError("MAX_REPLICAS must be greater than or equal to MIN_REPLICAS")

    return {
        "app_name": required_env("APP_NAME"),
        "port": port,
        "cpu": parse_cpu(),
        "memory": required_env("MEMORY"),
        "gpu": optional_env("GPU"),
        "pool": optional_env("POOL"),
        "min_replicas": min_replicas,
        "max_replicas": max_replicas,
        "keep_warm_seconds": parse_int("KEEP_WARM_SECONDS", default=0, minimum=-1),
        "concurrent_requests": parse_int("CONCURRENT_REQUESTS", default=1, minimum=1),
        "env_vars": parse_env_vars(optional_env("ENV_VARS_JSON")),
    }


def load_config() -> Dict[str, object]:
    mode = optional_env("DEPLOY_MODE", "repo") or "repo"
    config = load_common_deploy_config()
    config["mode"] = mode

    if mode == "model":
        model_id = required_env("MODEL_ID")
        config.update(
            {
                "model_id": model_id,
                "engine": optional_env("LLM_ENGINE", "vllm") or "vllm",
                "served_model_name": optional_env("SERVED_MODEL_NAME") or model_id,
                "context_length": parse_int("CONTEXT_LENGTH", default=0, minimum=0),
                "tokenizer": optional_env("TOKENIZER"),
                "metrics_path": optional_env("METRICS_PATH", "/metrics"),
                "slo_tier": optional_env("SLO_TIER", "standard"),
                "base_image": optional_env("LLM_BASE_IMAGE", DEFAULT_LLM_BASE_IMAGE),
            }
        )
        return config

    repo_full_name = validate_repo_full_name(
        optional_env("GIT_REPO_FULL_NAME") or required_env("GIT_REPO_URL")
    )
    working_dir = validate_relative_path(optional_env("WORKING_DIR") or optional_env("WORKDIR"))
    dockerfile_path = validate_relative_path(
        optional_env("DOCKERFILE_PATH", "Dockerfile"), "Dockerfile"
    )

    config.update({
        "repo_full_name": repo_full_name,
        "ref": validate_git_ref(required_env("GIT_REF")),
        "token": required_env("GIT_TOKEN"),
        "working_dir": working_dir,
        "dockerfile_path": dockerfile_path,
    })
    return config


def result_payload(config: Dict[str, object], deploy_result: Dict[str, object]) -> Dict[str, object]:
    endpoint_url = str(deploy_result.get("invoke_url") or "")
    return {
        "app_id": str(deploy_result.get("deployment_id") or ""),
        "app_name": str(config["app_name"]),
        "deployment_id": str(deploy_result.get("deployment_id") or ""),
        "deployment_name": str(deploy_result.get("deployment_name") or config["app_name"]),
        "endpoint_url": endpoint_url,
        "version": deploy_result.get("version"),
    }


def main() -> int:
    temp_dir = Path(tempfile.mkdtemp(prefix="beta9-github-deploy-"))
    try:
        config = load_config()
        if config.get("mode") == "model":
            print(f"Deploying model {config['model_id']}", flush=True)
            service = build_model_service(config)
        else:
            repo_dir = temp_dir / "repo"
            print(f"Cloning {config['repo_full_name']}@{config['ref']}", flush=True)
            clone_repo(str(config["repo_full_name"]), str(config["ref"]), str(config["token"]), repo_dir)

            working_dir = repo_dir / str(config["working_dir"])
            if not working_dir.is_dir():
                raise BuilderError("WORKING_DIR does not exist")
            os.chdir(working_dir)

            if not Path(str(config["dockerfile_path"])).is_file():
                raise BuilderError("DOCKERFILE_PATH does not exist")

            print("Deploying service", flush=True)
            service = build_service(repo_dir, config)
        deploy_result, ok = service.deploy(name=str(config["app_name"]))
        if not ok:
            raise BuilderError("Service deploy failed")

        print(RESULT_PREFIX + json.dumps(result_payload(config, deploy_result), sort_keys=True), flush=True)
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
