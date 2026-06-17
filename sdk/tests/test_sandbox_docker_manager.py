from types import SimpleNamespace

import yaml

from beta9.abstractions.sandbox import DockerResult, SandboxDockerManager


def docker_manager_with_output(output: str) -> SandboxDockerManager:
    manager = SandboxDockerManager.__new__(SandboxDockerManager)
    manager._run = lambda *args: SimpleNamespace(stdout=output)
    return manager


def test_docker_list_helpers_return_stdout_strings():
    manager = docker_manager_with_output("CONTAINER ID   IMAGE\nabc            busybox")

    assert manager.ps() == "CONTAINER ID   IMAGE\nabc            busybox"


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
