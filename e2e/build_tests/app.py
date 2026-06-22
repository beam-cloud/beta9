import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from beta9 import Image, Pod


HERE = Path(__file__).resolve().parent
BASE_IMAGE = os.getenv("BETA9_BUILD_TEST_BASE_IMAGE", "docker.io/library/python:3.10-slim")
RUN_ID = os.getenv("BETA9_BUILD_TEST_RUN_ID", str(uuid.uuid4()))
APP_NAME = f"build-tests-{RUN_ID[:8]}"
MARKER_DIR = "/opt/beta9-build-tests"
USER_PACKAGE = "pyfiglet==1.0.2"
USER_MODULE = "pyfiglet"
RUNTIME_MODULES = ["rich", "cloudpickle", "betterproto"]
DOCKERFILE = HERE / "Dockerfile.python"
DOCKERFILE_SOURCE_MARKER = f"{MARKER_DIR}/source.txt"
DOCKERFILE_SOURCE_CONTENT = "dockerfile-base"
IMAGE_REPOSITORY = os.getenv(
    "BETA9_BUILD_TEST_IMAGE_REPOSITORY", "registry.localhost:5000/beta9-users"
)
K8S_NAMESPACE = os.getenv("BETA9_BUILD_TEST_K8S_NAMESPACE", "beta9")
K8S_TIMEOUT_SECONDS = int(os.getenv("BETA9_BUILD_TEST_K8S_TIMEOUT_SECONDS", "300"))

# Keep SDK runtime file sync scoped to this e2e fixture directory.
os.chdir(HERE)


def marker_path(case_name: str) -> str:
    return f"{MARKER_DIR}/{case_name}.txt"


def marker_command(case_name: str) -> str:
    return (
        f"mkdir -p {MARKER_DIR} && "
        f"printf %s {shlex.quote(RUN_ID)} > {marker_path(case_name)}"
    )


def runtime_modules_expected() -> Dict[str, bool]:
    return {name: True for name in RUNTIME_MODULES}


def runtime_modules_absent() -> Dict[str, bool]:
    return {name: False for name in RUNTIME_MODULES}


def assert_runtime(
    case_name: str,
    actual: Dict[str, Dict[str, Any]],
    expected_modules: Dict[str, bool],
    expected_files: Dict[str, Optional[str]],
) -> None:
    errors = []

    for module, expected in expected_modules.items():
        got = actual.get("modules", {}).get(module)
        if got is not expected:
            errors.append(f"module {module}: expected {expected}, got {got}")

    for path, expected in expected_files.items():
        got = actual.get("files", {}).get(path)
        if got != expected:
            errors.append(f"file {path}: expected {expected!r}, got {got!r}")

    if errors:
        raise AssertionError(
            f"{case_name} failed:\n"
            + "\n".join(f"- {error}" for error in errors)
            + "\nactual="
            + json.dumps(actual, sort_keys=True)
        )


def function_case_expectations(
    case_name: str,
    *,
    user_package: bool = False,
    dockerfile: bool = False,
) -> Dict[str, Any]:
    modules = runtime_modules_expected()
    modules[USER_MODULE] = user_package
    files: Dict[str, Optional[str]] = {marker_path(case_name): RUN_ID}

    if dockerfile:
        files[DOCKERFILE_SOURCE_MARKER] = DOCKERFILE_SOURCE_CONTENT

    return {"modules": modules, "files": files}


def pod_case_expectations(
    case_name: str,
    *,
    runtime_deps: bool = False,
    user_package: bool = False,
    dockerfile: bool = False,
) -> Dict[str, Any]:
    modules = runtime_modules_expected() if runtime_deps else runtime_modules_absent()
    modules[USER_MODULE] = user_package
    files: Dict[str, Optional[str]] = {marker_path(case_name): RUN_ID}

    if dockerfile:
        files[DOCKERFILE_SOURCE_MARKER] = DOCKERFILE_SOURCE_CONTENT

    return {"modules": modules, "files": files}


default_function_image = Image(
    python_version="python3.10",
    python_packages=[USER_PACKAGE],
).add_commands([marker_command("function_default_runner")])

registry_function_image = Image.from_registry(BASE_IMAGE).add_commands(
    [marker_command("function_from_registry")]
)

registry_function_with_package_image = Image(
    python_version="python3.10",
    python_packages=[USER_PACKAGE],
    base_image=BASE_IMAGE,
).add_commands([marker_command("function_registry_constructor_package")])


def pod_validation_entrypoint(
    case_name: str,
    expected_modules: Dict[str, bool],
    expected_files: Dict[str, Optional[str]],
) -> List[str]:
    payload = json.dumps(
        {
            "case": case_name,
            "modules": expected_modules,
            "files": expected_files,
        },
        sort_keys=True,
    )
    script = f"""
import importlib.util
import json
import sys
from pathlib import Path

expected = json.loads({payload!r})
actual = {{
    "modules": {{
        name: importlib.util.find_spec(name) is not None
        for name in expected["modules"]
    }},
    "files": {{
        path: Path(path).read_text().strip() if Path(path).exists() else None
        for path in expected["files"]
    }},
}}
print("BETA9_BUILD_TEST_RESULT " + json.dumps(actual, sort_keys=True), flush=True)

errors = []
for module, want in expected["modules"].items():
    got = actual["modules"].get(module)
    if got is not want:
        errors.append(f"module {{module}}: expected {{want}}, got {{got}}")

for path, want in expected["files"].items():
    got = actual["files"].get(path)
    if got != want:
        errors.append(f"file {{path}}: expected {{want!r}}, got {{got!r}}")

if errors:
    print("BETA9_BUILD_TEST_FAILURE " + json.dumps(errors, sort_keys=True), flush=True)
    sys.exit(1)
"""
    return ["python3.10", "-c", script]


def result_from_pod_output(case_name: str, output: str) -> Dict[str, Dict[str, Any]]:
    prefix = "BETA9_BUILD_TEST_RESULT "
    result_lines = [line for line in output.splitlines() if line.startswith(prefix)]
    if not result_lines:
        raise AssertionError(f"{case_name} did not emit a runtime inspection result\n{output}")
    return json.loads(result_lines[-1][len(prefix) :])


def kubernetes_name(case_name: str) -> str:
    safe_run_id = re.sub(r"[^a-z0-9-]+", "-", RUN_ID.lower()).strip("-")
    safe_case = re.sub(r"[^a-z0-9-]+", "-", case_name.lower()).strip("-")
    suffix = safe_run_id[:12] or uuid.uuid4().hex[:12]
    return f"build-test-{safe_case[:36]}-{suffix}"[:63].strip("-")


def run_kubectl(args: List[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl", "-n", K8S_NAMESPACE, *args],
        check=check,
        text=True,
        capture_output=True,
    )


def pod_phase(pod_name: str) -> str:
    result = run_kubectl(
        ["get", "pod", pod_name, "-o", "jsonpath={.status.phase}"],
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


def wait_for_inspection_pod(case_name: str, pod_name: str) -> str:
    deadline = time.time() + K8S_TIMEOUT_SECONDS
    while time.time() < deadline:
        phase = pod_phase(pod_name)
        if phase in ("Succeeded", "Failed"):
            return phase
        time.sleep(2)

    describe = run_kubectl(["describe", "pod", pod_name], check=False)
    raise AssertionError(
        f"{case_name} inspection pod timed out after {K8S_TIMEOUT_SECONDS}s\n"
        + describe.stdout
        + describe.stderr
    )


def inspect_image_with_kubernetes(
    case_name: str,
    image_id: str,
    expectations: Dict[str, Any],
) -> None:
    pod_name = kubernetes_name(case_name)
    image_ref = f"{IMAGE_REPOSITORY}:{image_id}"
    command = pod_validation_entrypoint(
        case_name, expectations["modules"], expectations["files"]
    )

    run_kubectl(["delete", "pod", pod_name, "--ignore-not-found=true"], check=False)
    try:
        run_kubectl(
            [
                "run",
                pod_name,
                "--restart=Never",
                "--image",
                image_ref,
                "--image-pull-policy=IfNotPresent",
                "--command",
                "--",
                *command,
            ]
        )
        phase = wait_for_inspection_pod(case_name, pod_name)
        logs = run_kubectl(["logs", pod_name], check=False)
        output = logs.stdout + logs.stderr

        if phase != "Succeeded":
            describe = run_kubectl(["describe", "pod", pod_name], check=False)
            raise AssertionError(
                f"{case_name} inspection pod ended in phase {phase}\n"
                + output
                + describe.stdout
                + describe.stderr
            )

        actual = result_from_pod_output(case_name, output)
        assert_runtime(case_name, actual, expectations["modules"], expectations["files"])
    finally:
        run_kubectl(["delete", "pod", pod_name, "--ignore-not-found=true"], check=False)


def run_image_case(case_name: str, image: Image, expectations: Dict[str, Any]) -> None:
    print(f"==> {case_name}")
    result = image.build()
    if not result.success:
        raise AssertionError(f"{case_name} failed to build image")

    inspect_image_with_kubernetes(case_name, result.image_id, expectations)


def run_pod_case(
    case_name: str,
    image: Image,
    expectations: Dict[str, Any],
) -> None:
    print(f"==> {case_name}")
    pod = Pod(
        app=APP_NAME,
        name=case_name.replace("_", "-"),
        image=image,
        memory=512,
        keep_warm_seconds=1,
    )
    result = pod.image.build()
    if not result.success:
        raise AssertionError(f"{case_name} failed to build pod image")

    inspect_image_with_kubernetes(case_name, result.image_id, expectations)


def wants_case(selected: set, case_name: str) -> bool:
    return not selected or case_name in selected


def image_from_dockerfile() -> Image:
    image = Image()
    image.dockerfile = DOCKERFILE.read_text()
    return image


def function_cases(selected: set) -> List[Dict[str, Any]]:
    cases = []

    if wants_case(selected, "function_default_runner"):
        cases.append(
            {
                "name": "function_default_runner",
                "image": default_function_image,
                "expect": function_case_expectations(
                    "function_default_runner", user_package=True
                ),
            }
        )

    if wants_case(selected, "function_from_registry"):
        cases.append(
            {
                "name": "function_from_registry",
                "image": registry_function_image,
                "expect": function_case_expectations("function_from_registry"),
            }
        )

    if wants_case(selected, "function_registry_constructor_package"):
        cases.append(
            {
                "name": "function_registry_constructor_package",
                "image": registry_function_with_package_image,
                "expect": function_case_expectations(
                    "function_registry_constructor_package", user_package=True
                ),
            }
        )

    if wants_case(selected, "function_from_dockerfile"):
        image = image_from_dockerfile().add_python_packages([USER_PACKAGE]).add_commands(
            [marker_command("function_from_dockerfile")]
        )
        cases.append(
            {
                "name": "function_from_dockerfile",
                "image": image,
                "expect": function_case_expectations(
                    "function_from_dockerfile", user_package=True, dockerfile=True
                ),
            }
        )

    return cases


def pod_cases(selected: set) -> List[Dict[str, Any]]:
    cases = []

    if wants_case(selected, "pod_registry_commands_no_python"):
        cases.append(
            {
                "name": "pod_registry_commands_no_python",
                "image": Image.from_registry(BASE_IMAGE).add_commands(
                    [marker_command("pod_registry_commands_no_python")]
                ),
                "expect": pod_case_expectations("pod_registry_commands_no_python"),
            }
        )

    if wants_case(selected, "pod_registry_constructor_package"):
        cases.append(
            {
                "name": "pod_registry_constructor_package",
                "image": Image(
                    python_version="python3.10",
                    python_packages=[USER_PACKAGE],
                    base_image=BASE_IMAGE,
                ).add_commands([marker_command("pod_registry_constructor_package")]),
                "expect": pod_case_expectations(
                    "pod_registry_constructor_package", user_package=True
                ),
            }
        )

    if wants_case(selected, "pod_registry_build_step_package"):
        cases.append(
            {
                "name": "pod_registry_build_step_package",
                "image": Image.from_registry(BASE_IMAGE)
                .add_python_packages([USER_PACKAGE])
                .add_commands([marker_command("pod_registry_build_step_package")]),
                "expect": pod_case_expectations(
                    "pod_registry_build_step_package", user_package=True
                ),
            }
        )

    if wants_case(selected, "pod_from_dockerfile_no_python"):
        cases.append(
            {
                "name": "pod_from_dockerfile_no_python",
                "image": image_from_dockerfile().add_commands(
                    [marker_command("pod_from_dockerfile_no_python")]
                ),
                "expect": pod_case_expectations(
                    "pod_from_dockerfile_no_python", dockerfile=True
                ),
            }
        )

    if wants_case(selected, "pod_from_dockerfile_constructor_package"):
        cases.append(
            {
                "name": "pod_from_dockerfile_constructor_package",
                "image": image_from_dockerfile()
                .add_python_packages([USER_PACKAGE])
                .add_commands([marker_command("pod_from_dockerfile_constructor_package")]),
                "expect": pod_case_expectations(
                    "pod_from_dockerfile_constructor_package",
                    user_package=True,
                    dockerfile=True,
                ),
            }
        )

    if wants_case(selected, "pod_registry_explicit_python_runtime"):
        cases.append(
            {
                "name": "pod_registry_explicit_python_runtime",
                "image": Image.from_registry(BASE_IMAGE)
                .add_python_version("python3.10")
                .add_commands([marker_command("pod_registry_explicit_python_runtime")]),
                "expect": pod_case_expectations(
                    "pod_registry_explicit_python_runtime", runtime_deps=True
                ),
            }
        )

    return cases


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Beam/Beta9 image build e2e tests.")
    parser.add_argument(
        "mode",
        nargs="?",
        default="all",
        choices=["all", "functions", "pods", "local"],
        help="'local' is accepted as an alias for 'all' for compatibility with the old target.",
    )
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Run only the named case. Can be passed multiple times.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mode = "all" if args.mode == "local" else args.mode
    selected = set(args.case)

    cases: List[Dict[str, Any]] = []
    if mode in ("all", "functions"):
        cases.extend({"kind": "function", **case} for case in function_cases(selected))
    if mode in ("all", "pods"):
        cases.extend({"kind": "pod", **case} for case in pod_cases(selected))

    if selected:
        cases = [case for case in cases if case["name"] in selected]
        missing = selected - {case["name"] for case in cases}
        if missing:
            raise SystemExit(f"unknown build test case(s): {', '.join(sorted(missing))}")

    print(f"run_id={RUN_ID}")
    print(f"base_image={BASE_IMAGE}")
    print(f"app={APP_NAME}")

    for case in cases:
        if case["kind"] == "function":
            run_image_case(case["name"], case["image"], case["expect"])
        else:
            run_pod_case(case["name"], case["image"], case["expect"])

    print("all build image e2e cases passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"build image e2e failed: {exc}", file=sys.stderr)
        raise
