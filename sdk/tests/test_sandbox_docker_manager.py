from types import SimpleNamespace

import pytest
import yaml

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


def test_compose_override_uses_host_network_and_pid_namespaces():
    uploaded = {}

    class FakeFS:
        def download_file(self, _remote_path, local_path):
            with open(local_path, "w") as f:
                f.write("services:\n  web:\n    image: busybox\n")

        def upload_file(self, local_path, remote_path):
            with open(local_path) as f:
                uploaded["content"] = f.read()
            uploaded["remote_path"] = remote_path

    manager = SandboxDockerManager.__new__(SandboxDockerManager)
    manager.sandbox_instance = SimpleNamespace(fs=FakeFS())

    override_path = manager._create_compose_override("compose.yml", cwd="/workspace")
    override = yaml.safe_load(uploaded["content"])
    service = override["services"]["web"]

    assert override_path == "/tmp/.docker-compose-override.yml"
    assert uploaded["remote_path"] == "/tmp/.docker-compose-override.yml"
    assert service["network_mode"] == "host"
    assert service["pid"] == "host"


def test_compose_build_and_up_share_build_network_entitlements():
    uploaded = {}
    commands = []
    plan = '{"target":{"web":{"tags":["project-web"]}}}'
    manager = SandboxDockerManager.__new__(SandboxDockerManager)
    manager._read_compose_services = lambda *args: (["web", "db"], {"web"})
    manager._authenticated = True

    def upload(content, path, **kwargs):
        uploaded[path] = content

    def run(*cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(stdout=plan)

    def exec_process(*cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(wait=lambda: 0)

    manager._upload_text = upload
    manager._run = run
    manager.sandbox_instance = SimpleNamespace(process=SimpleNamespace(exec=exec_process))
    manager.compose_build(file="compose.yml", no_cache=True)
    manager.compose_up(file="compose.yml", build=True)

    override = yaml.safe_load(uploaded["/tmp/.docker-compose-override.yml"])
    assert override["services"]["web"]["build"] == {
        "network": "host", "entitlements": ["network.host"]
    }
    assert "build" not in override["services"]["db"]
    assert uploaded["/tmp/.docker-compose-override.yml.bake.json"] == plan
    builds = [cmd for cmd in commands if cmd[:3] == ("docker", "buildx", "bake")]
    assert len(builds) == 2
    assert all("--allow=network.host" in cmd and "--load" in cmd for cmd in builds)
    assert "--no-cache" in builds[0]
    assert commands[-1] == (
        "docker-compose", "-f", "compose.yml", "-f", "/tmp/.docker-compose-override.yml",
        "up", "--no-build", "-d",
    )


@pytest.mark.parametrize("cached", [True, False])
def test_compose_up_builds_only_missing_images_by_default(cached):
    builds = []
    manager = SandboxDockerManager.__new__(SandboxDockerManager)
    manager._authenticated = True
    manager._read_compose_services = lambda *args: (["web"], {"web"})
    manager._create_compose_override = lambda *args: "/tmp/override.yml"

    def run(*cmd, **kwargs):
        if cmd[:3] == ("docker", "image", "inspect") and not cached:
            raise DockerCommandError("docker image inspect", 1, "No such image")
        return SimpleNamespace(stdout="project-web")

    def build(**kwargs):
        builds.append(kwargs)
        return SimpleNamespace(wait=lambda: 0)

    manager._run = run
    manager.compose_build = build
    manager.compose_up(file="compose.yml", cwd="/project")

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
