from types import SimpleNamespace

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
