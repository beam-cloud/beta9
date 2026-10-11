import json
from types import SimpleNamespace

import pytest

from beta9.abstractions.sandbox import DockerResult, SandboxDockerManager
from beta9.exceptions import DockerCommandError


def docker_manager_with_output(output: str) -> SandboxDockerManager:
    manager = SandboxDockerManager.__new__(SandboxDockerManager)
    manager._run = lambda *args: SimpleNamespace(stdout=output)
    return manager


def test_docker_list_helpers_return_stdout_strings():
    manager = docker_manager_with_output("CONTAINER ID   IMAGE\nabc            busybox")

    assert manager.ps() == "CONTAINER ID   IMAGE\nabc            busybox"


def test_docker_logs_include_stderr():
    manager = SandboxDockerManager.__new__(SandboxDockerManager)
    manager._run = lambda *args: SimpleNamespace(stdout="out", stderr="error")

    assert manager.logs("app") == "out\nerror"


def test_docker_list_helpers_split_quiet_stdout():
    manager = docker_manager_with_output("abc\ndef")

    assert manager.ps(quiet=True) == ["abc", "def"]
    assert manager.images(quiet=True) == ["abc", "def"]
    assert manager.volume_ls(quiet=True) == ["abc", "def"]


def test_docker_run_uses_host_network_and_pid_namespaces():
    captured = {}
    manager = SandboxDockerManager.__new__(SandboxDockerManager)

    def fake_result(*cmd, extract_output=None):
        captured["cmd"] = cmd
        captured["extract_output"] = extract_output
        return SimpleNamespace()

    manager._result = fake_result

    manager.run("busybox:1.36", command=["sleep", "1"], detach=True)

    assert captured["cmd"] == (
        "docker",
        "run",
        "--network",
        "host",
        "--pid",
        "host",
        "-d",
        "busybox:1.36",
        "sleep",
        "1",
    )
    assert captured["extract_output"] is not None


@pytest.fixture
def compose_manager():
    manager = SandboxDockerManager.__new__(SandboxDockerManager)
    manager._authenticated = True
    manager._ensure_ready = lambda: None
    manager.sandbox_instance = SimpleNamespace(
        fs=SimpleNamespace(write_text=lambda *args, **kwargs: None)
    )
    return manager


def test_compose_config_resolves_services_in_guest_and_isolates_projects(compose_manager):
    uploaded = {}
    commands = []
    services = {
        "included-web": {
            "build": {"context": "/workspace/src"},
            "networks": {"custom": None},
            "extra_hosts": {"external.example": "192.0.2.5"},
        },
        "db": {"image": "busybox"},
    }

    def run(*cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(stdout=json.dumps({"services": services}))

    compose_manager._run = run
    compose_manager.sandbox_instance.fs.write_text = lambda path, text, **kwargs: uploaded.update(
        {path: text}
    )
    first = compose_manager._compose_config("compose.yml", cwd="/workspace/one")
    second = compose_manager._compose_config("compose.yml", cwd="/workspace/two")
    assert first.override_path != second.override_path
    assert commands[0] == ("docker-compose", "-f", "compose.yml", "config", "--format", "json")
    assert list(first.services) == list(services)
    project = json.loads(uploaded[first.override_path])
    override = project["services"]
    assert "networks" not in project and "networks" not in override["included-web"]
    assert override["included-web"] == {
        "network_mode": "host",
        "pid": "host",
        "extra_hosts": {
            "external.example": "192.0.2.5",
            "included-web": "127.0.0.1",
            "db": "127.0.0.1",
        },
        "build": {"context": "/workspace/src", "network": "host", "entitlements": ["network.host"]},
    }
    assert "build" not in override["db"]


def test_compose_invalid_config_surfaces_original_error(compose_manager):
    def run(*cmd, **kwargs):
        raise DockerCommandError("docker compose config", 1, "invalid service")

    compose_manager._run = run
    with pytest.raises(DockerCommandError, match="invalid service"):
        compose_manager.compose_up()


def test_compose_build_and_up_share_resolved_plan_and_workspace(compose_manager):
    uploaded = {}
    commands = []
    working_dirs = []
    plan = '{"target":{"web":{"tags":["project-web"]}}}'

    def run(*cmd, **kwargs):
        commands.append(cmd)
        output = (
            json.dumps({"services": {"web": {"build": {}}, "db": {"image": "busybox"}}})
            if "--format" in cmd
            else plan
        )
        return SimpleNamespace(stdout=output)

    def exec_process(*cmd, **kwargs):
        commands.append(cmd)
        working_dirs.append(kwargs["cwd"])
        return SimpleNamespace(wait=lambda: 0)

    compose_manager.sandbox_instance.fs.write_text = lambda path, text, **kwargs: uploaded.update(
        {path: text}
    )
    compose_manager._run = run
    compose_manager.sandbox_instance.process = SimpleNamespace(exec=exec_process)
    compose_manager.compose_build(file="compose.yml", no_cache=True, pull=True)
    compose_manager.compose_up(file="compose.yml", build=True)
    builds = [cmd for cmd in commands if cmd[:3] == ("docker", "buildx", "bake")]
    assert len(builds) == 2
    assert all("--allow=network.host" in cmd and "--load" in cmd for cmd in builds)
    assert "--no-cache" in builds[0] and "--pull" in builds[0]
    assert uploaded[builds[0][4]] == plan
    assert working_dirs == ["/workspace", "/workspace"]
    assert commands[-1][-3:] == ("up", "--no-build", "-d")
    # One Compose config resolution per operation, including build=True.
    assert sum("--format" in cmd for cmd in commands) == 2


@pytest.mark.parametrize("cached", [True, False])
def test_compose_up_builds_only_missing_images_by_default(compose_manager, cached):
    builds = []

    def run(*cmd, **kwargs):
        if "--format" in cmd:
            return SimpleNamespace(stdout=json.dumps({"services": {"web": {"build": {}}}}))
        if cmd[:3] == ("docker", "image", "inspect") and not cached:
            raise DockerCommandError("docker image inspect", 1, "No such image")
        return SimpleNamespace(stdout="project-web")

    def build(*args, **kwargs):
        builds.append(args)
        return SimpleNamespace(wait=lambda: 0)

    compose_manager._run = run
    compose_manager._compose_build = build
    compose_manager.compose_up(file="compose.yml", cwd="/project")
    assert len(builds) == (0 if cached else 1)


class ConsumptiveStream:
    def __init__(self, value: str):
        self.value = value

    def read(self):
        value = self.value
        self.value = ""
        return value


class FakeDockerProcess:
    def __init__(self):
        self.stdout = ConsumptiveStream("stdout-once")
        self.stderr = ConsumptiveStream("stderr-once")

    def wait(self):
        return 0


def test_docker_result_caches_consumptive_output_streams():
    result = DockerResult(FakeDockerProcess())

    assert result.success is True
    assert result.stdout == "stdout-once"
    assert result.stdout == "stdout-once"
    assert result.stderr == "stderr-once"
    assert result.stderr == "stderr-once"
