"""Create and verify onboarding workloads."""

import fcntl
import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from ..abstractions.vm import VM
from ..channel import GatewayHTTPError, ServiceClient

AppLookup = Callable[[str, Dict[str, Any]], Dict[str, Any]]

SANDBOX_CODE = """import hashlib

records = ["beam", "cloud", "agents"]
print(f"Hashing {len(records)} records", flush=True)
for record in records:
    digest = hashlib.sha256(record.encode()).hexdigest()[:8]
    print(f"{record}: {digest}", flush=True)
print("Hello from the cloud", flush=True)
"""


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
            vm = _create_vm(service, path, state, name, desktop)
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


def _create_vm(service, path, state, name, desktop):
    for attempt in range(2):
        vm = VM(
            name,
            desktop=desktop,
            docker_enabled=True,
            auto_resume=True,
            metadata={"beam.onboarding": state["request_id"]},
            request_id=state["request_id"],
            _service=service,
        )
        if state.get("creation_body"):
            vm._creation_body = state["creation_body"]
        else:
            vm.prepare()
            state["creation_body"] = vm._creation_body
            _save_state(path, state)
        try:
            vm.create(wait=False)
            return vm
        except GatewayHTTPError as exc:
            if exc.status != 410:
                raise
            if attempt:
                raise GatewayHTTPError(
                    410,
                    "The VM was deleted during setup. Run this command again to create a new VM.",
                ) from exc
            state = {"request_id": str(uuid.uuid4())}
            _save_state(path, state)


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
    service: ServiceClient,
    state_directory: Path,
    name: str,
    kind: str,
    call: AppLookup,
    *,
    on_source: Optional[Callable[[Path], None]] = None,
    on_output: Optional[Callable[[str], None]] = None,
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
    state_directory = state_directory.resolve()
    directory = state_directory / "examples" / key
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
        path = directory / "app.py"
        if kind == "sandboxes":
            if matches and state.get("app_id") != matches[0].get("app_id"):
                raise ValueError(f"App {name!r} already exists; choose a different --name")
            path.write_text(
                "from beta9 import Sandbox\n\n"
                f"code = '''{SANDBOX_CODE}'''\n\n"
                f"sandbox = Sandbox(name={name!r}, cpu=1, memory=256).create()\n"
                "if not sandbox.ok:\n"
                "    raise RuntimeError(sandbox.error_msg)\n\n"
                "try:\n"
                "    process = sandbox.process.exec('python', '-u', '-c', code)\n"
                "    for line in process.stdout:\n"
                "        print(line, end='', flush=True)\n"
                "    if process.wait(30) != 0:\n"
                "        raise RuntimeError(process.stderr.read())\n"
                "finally:\n"
                "    sandbox.terminate()\n"
            )
            if on_source:
                on_source(path)
            result = _run_sandbox_example(service, directory, name, state, call, on_output)
            return {**result, "source_path": str(path)}

        existing = None
        if matches:
            existing = call("get_app", {"name": name})
            if marker not in existing.get("config", {}).get("env", []):
                raise ValueError(f"App {name!r} already exists; choose a different --name")

        decorator = "endpoint" if kind == "inference" else "function"
        scaling = "    keep_warm_seconds=0,\n" if kind == "inference" else ""
        output = (
            "    records = ['beam', 'cloud', 'agents']\n"
            "    print(f'Processing {len(records)} records', flush=True)\n"
            "    print(', '.join(record.upper() for record in records), flush=True)\n"
            if kind == "functions"
            else ""
        )
        source = (
            f"from beta9 import {decorator}\n\n\n"
            f"@{decorator}(\n"
            f"    name={name!r},\n"
            f"    app={name!r},\n"
            "    cpu=1,\n"
            "    memory=256,\n"
            f"{scaling}"
            f"    env={{'ONBOARDING_RUN': {state['request_id']!r}}},\n"
            ")\n"
            "def hello(**inputs):\n"
            f"{output}"
            f"    return {{'message': 'Hello from the cloud', 'example': {kind!r}}}\n"
        )
        path.write_text(source)
        if on_source:
            on_source(path)

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
            example = {
                "name": name,
                "example": kind,
                "result": result,
                "source_path": str(path),
                **existing,
            }
            if kind == "inference":
                curl_config = (state_directory / "requests" / (key + ".curl")).resolve()
                curl_config.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                with open(curl_config, "w", opener=_private_file) as config:
                    header = "Authorization: Bearer " + service._config.token
                    config.write("header = " + json.dumps(header) + "\n")
                example["curl_config"] = str(curl_config)
            return example
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
    service: ServiceClient,
    directory: Path,
    name: str,
    state: Dict[str, Any],
    call: AppLookup,
    on_output: Optional[Callable[[str], None]],
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

        process = instance.process.exec("python", "-u", "-c", SANDBOX_CODE)
        output = []
        deadline = time.monotonic() + 30
        while True:
            chunk = process.stdout.read()
            if chunk:
                output.append(chunk)
                if on_output:
                    on_output(chunk)
            exit_code, _ = process.status()
            if exit_code >= 0:
                chunk = process.stdout.read()
                if chunk:
                    output.append(chunk)
                    if on_output:
                        on_output(chunk)
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Sandbox sample did not finish within 30 seconds")
            time.sleep(0.1)
        if exit_code != 0 or not "".join(output).strip().endswith("Hello from the cloud"):
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
