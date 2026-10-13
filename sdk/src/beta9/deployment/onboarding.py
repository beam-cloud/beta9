"""Create and verify onboarding workloads."""

import fcntl
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any, Callable, Dict

from ..abstractions.vm import VM
from ..channel import GatewayHTTPError, ServiceClient

AppLookup = Callable[[str, Dict[str, Any]], Dict[str, Any]]


def prepare_vm(
    service: ServiceClient, state_directory: Path, name: str, *, desktop: bool = False
) -> Dict[str, Any]:
    """Create or resume a setup-owned VM and verify its Linux guest."""
    workspace = service.http.workspace_id
    directory = state_directory / hashlib.sha256(workspace.encode()).hexdigest()[:24]
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / (hashlib.sha256(name.encode()).hexdigest()[:24] + ".json")

    with open(path.with_suffix(".lock"), "a", opener=_private_file) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = json.loads(path.read_text()) if path.exists() else {"request_id": str(uuid.uuid4())}
        marker = {"beam.onboarding": state["request_id"]}

        try:
            vm = VM.get(name, _service=service)
        except GatewayHTTPError as exc:
            if exc.status != 404:
                raise
            vm = VM(
                name,
                cpu=1,
                memory=2048,
                disk_size="10Gi",
                desktop=desktop,
                docker_enabled=True,
                ttl=900,
                auto_resume=True,
                metadata=marker,
                request_id=state["request_id"],
                _service=service,
            )
            if state.get("creation_body"):
                vm._creation_body = state["creation_body"]
            else:
                vm.prepare()
                state["creation_body"] = vm._creation_body
                _save_state(path, state)
            vm.create(wait=False)
        else:
            if (vm.info.get("metadata") or {}).get("beam.onboarding") != marker["beam.onboarding"]:
                raise ValueError(f"VM {name!r} already exists; choose a different --name")
            if vm.info.get("desired_state") != "running":
                vm.start(wait=False)

        vm.wait(timeout=240, services=True)
        probe = vm.process.exec("sh", "-c", "test -d /workspace && uname -s", cwd="/")
        if probe.wait(30) != 0 or probe.stdout.read().strip() != "Linux":
            raise RuntimeError(
                "VM guest readiness check failed; inspect vm get/logs and retry setup"
            )
        return vm.refresh().info


def _private_file(path: str, flags: int) -> int:
    return os.open(path, flags, 0o600)


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    with open(temporary, "w", opener=_private_file) as output:
        json.dump(state, output)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def prepare_example(
    service: ServiceClient, state_directory: Path, name: str, kind: str, call: AppLookup
) -> Dict[str, Any]:
    """Run a verified sample in a setup-owned app."""
    import importlib.util
    import sys

    import requests

    from ..abstractions import base

    if kind not in ("inference", "functions", "sandboxes"):
        raise ValueError("Choose inference, functions or sandboxes")

    workspace = service.http.workspace_id
    key = hashlib.sha256(f"{workspace}:{kind}:{name}".encode()).hexdigest()[:24]
    # Canonicalize symlinks so handler paths stay relative to the upload root.
    directory = (state_directory / "examples" / key).resolve()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    state_path = directory / "request.json"

    with open(directory / "request.lock", "a", opener=_private_file) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if state_path.exists():
            state = json.loads(state_path.read_text())
        else:
            state = {"request_id": str(uuid.uuid4())}
            _save_state(state_path, state)

        marker = f"ONBOARDING_RUN={state['request_id']}"
        matches = call("list_apps", {"name": name}).get("items", [])
        if kind == "sandboxes":
            if matches and state.get("app_id") != matches[0].get("app_id"):
                raise ValueError(f"App {name!r} already exists; choose a different --name")
            return _run_sandbox_example(service, directory, name, state, call)

        existing = None
        if matches:
            existing = call("get_app", {"name": name})
            if marker not in existing.get("config", {}).get("env", []):
                raise ValueError(f"App {name!r} already exists; choose a different --name")

        decorator = "endpoint" if kind == "inference" else "function"
        scaling = "keep_warm_seconds=0, " if kind == "inference" else ""
        source = (
            f"from beta9 import {decorator}\n\n"
            f"@{decorator}(name={name!r}, app={name!r}, cpu=1, memory=256, "
            f"{scaling}env={{'ONBOARDING_RUN': {state['request_id']!r}}})\n"
            "def hello(**inputs):\n"
            f"    return {{'message': 'Hello from the cloud', 'example': {kind!r}}}\n"
        )
        path = directory / "app.py"
        path.write_text(source)

        previous_directory = Path.cwd()
        previous_module = sys.modules.get("app")
        previous_channel = base._channel
        try:
            os.chdir(directory)
            base.set_channel(service.channel)
            spec = importlib.util.spec_from_file_location("app", path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["app"] = module
            spec.loader.exec_module(module)
            if existing is None:
                _, accepted = module.hello.deploy(
                    name=name, context=service._config, invocation_details_func=lambda **_: None
                )
                if not accepted:
                    raise RuntimeError(
                        "Sample deployment was not accepted; inspect logs and resume setup"
                    )
                existing = call("get_app", {"name": name})

            if kind == "inference":
                url = existing.get("url") or existing.get("latest_url")
                if not url:
                    raise RuntimeError("The sample endpoint has no invocation URL")
                response = requests.post(
                    url,
                    json={},
                    headers={"Authorization": "Bearer " + service._config.token},
                    timeout=180,
                )
                response.raise_for_status()
                result = response.json()
            else:
                result = module.hello.remote()

            if not isinstance(result, dict) or result.get("message") != "Hello from the cloud":
                raise RuntimeError("The sample ran but its hello-world result was not verified")
            return {"name": name, "example": kind, "result": result, **existing}
        finally:
            os.chdir(previous_directory)
            if previous_module is None:
                sys.modules.pop("app", None)
            else:
                sys.modules["app"] = previous_module
            if previous_channel is None:
                base.unset_channel()
            else:
                base.set_channel(previous_channel)


def _run_sandbox_example(
    service: ServiceClient, directory: Path, name: str, state: Dict[str, Any], call: AppLookup
) -> Dict[str, Any]:
    """Run the sandbox sample and always stop its container."""
    from ..abstractions import base
    from ..abstractions.sandbox import Sandbox

    previous_directory = Path.cwd()
    previous_channel = base._channel
    instance = None
    try:
        os.chdir(directory)
        base.set_channel(service.channel)
        sandbox = Sandbox(
            name=name,
            cpu=1,
            memory=256,
            keep_warm_seconds=60,
            env={"ONBOARDING_RUN": state["request_id"]},
        )
        sandbox.config_context = service._config
        instance = sandbox.create()
        if not instance.ok:
            raise RuntimeError(instance.error_msg or "Sandbox creation failed")

        apps = call("list_apps", {"name": name}).get("items", [])
        if len(apps) != 1 or not apps[0].get("app_id"):
            raise RuntimeError("Sandbox app could not be found; inspect logs before retrying")
        state["app_id"] = apps[0]["app_id"]
        _save_state(directory / "request.json", state)

        process = instance.process.exec("python", "-c", "print('Hello from the cloud')")
        if process.wait(30) != 0 or process.stdout.read().strip() != "Hello from the cloud":
            raise RuntimeError("Sandbox hello-world result was not verified; inspect logs")
        return {
            "name": name,
            "example": "sandboxes",
            "app_id": state["app_id"],
            "container_id": instance.container_id,
            "result": {"message": "Hello from the cloud", "example": "sandboxes"},
        }
    finally:
        try:
            if instance is not None and instance.ok and not instance.terminate():
                raise RuntimeError("Sandbox did not stop; inspect container status")
        finally:
            os.chdir(previous_directory)
            if previous_channel is None:
                base.unset_channel()
            else:
                base.set_channel(previous_channel)
