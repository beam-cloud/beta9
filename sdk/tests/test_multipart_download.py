from pathlib import Path
from types import SimpleNamespace

import pytest

from beta9 import multipart
from beta9.multipart import RemotePath


class FakeVolumeService:
    def stat_path(self, request):
        path_info = SimpleNamespace(is_dir=False)
        return SimpleNamespace(ok=True, err_msg="", path_info=path_info)


class FakeProgress:
    def __init__(self):
        self.task_names = []

    def add_task(self, task_name):
        self.task_names.append(task_name)
        return len(self.task_names)


@pytest.fixture
def downloads(monkeypatch):
    """Record the files beta9_download would write instead of downloading them."""
    file_paths = []
    monkeypatch.setattr(
        multipart, "beta9_download", lambda **kwargs: file_paths.append(kwargs["file_path"])
    )
    return file_paths


def test_download_to_directory_outside_cwd(tmp_path, monkeypatch, downloads):
    project = tmp_path / "project"
    project.mkdir()
    destination = tmp_path / "downloads"
    destination.mkdir()
    monkeypatch.chdir(project)
    progress = FakeProgress()

    handler = multipart.Beta9Handler(service=FakeVolumeService(), progress=progress)
    handler.download(RemotePath.parse("beam://my-volume/runs/metrics.csv"), destination)

    assert downloads == [destination / "metrics.csv"]
    assert progress.task_names == [destination / "metrics.csv"]


def test_download_inside_cwd_shows_relative_path(tmp_path, monkeypatch, downloads):
    destination = tmp_path / "downloads"
    destination.mkdir()
    monkeypatch.chdir(tmp_path)
    progress = FakeProgress()

    handler = multipart.Beta9Handler(service=FakeVolumeService(), progress=progress)
    handler.download(RemotePath.parse("beam://my-volume/runs/metrics.csv"), destination.resolve())

    assert downloads == [destination.resolve() / "metrics.csv"]
    assert progress.task_names == [Path("downloads") / "metrics.csv"]
