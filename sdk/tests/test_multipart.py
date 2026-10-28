import ntpath
import os
from textwrap import dedent
from types import SimpleNamespace

import pytest

from beta9 import multipart
from beta9.multipart import RemotePath


def test_calculate_chunk_size():
    output = dedent("""
        File:     2.0 MiB → Chunk:   2.0 MiB, Workers: 1
        File:     4.0 MiB → Chunk:   4.0 MiB, Workers: 1
        File:     8.0 MiB → Chunk:   5.0 MiB, Workers: 1
        File:    16.0 MiB → Chunk:   5.0 MiB, Workers: 1
        File:    32.0 MiB → Chunk:   5.0 MiB, Workers: 2
        File:    64.0 MiB → Chunk:   5.0 MiB, Workers: 3
        File:   128.0 MiB → Chunk:   8.0 MiB, Workers: 4
        File:   256.0 MiB → Chunk:  16.0 MiB, Workers: 5
        File:   512.0 MiB → Chunk:  32.0 MiB, Workers: 6
        File:  1024.0 MiB → Chunk:  64.0 MiB, Workers: 7
        File:  2048.0 MiB → Chunk: 128.0 MiB, Workers: 8
        File:  4096.0 MiB → Chunk: 256.0 MiB, Workers: 9
        File:  8192.0 MiB → Chunk: 500.0 MiB, Workers: 10
        File: 16384.0 MiB → Chunk: 500.0 MiB, Workers: 11
        File: 32768.0 MiB → Chunk: 500.0 MiB, Workers: 12
    """)

    mib = 1024 * 1024
    lines = [""]
    sizes = {(2**i) * mib: multipart._calculate_chunk_size((2**i) * mib) for i in range(1, 16)}
    for size, (chunk, workers) in sizes.items():
        line = f"File: {(size/mib):7.1f} MiB → Chunk: {(chunk/mib):5.1f} MiB, Workers: {workers}"
        lines.append(line)
        print(line)

    assert output.splitlines() == lines


@pytest.fixture
def windows_os_path(monkeypatch):
    """Make os.path behave the way it does on Windows."""
    monkeypatch.setattr(os, "path", ntpath)


def test_remote_path_uses_forward_slashes_on_windows(windows_os_path):
    remote = RemotePath.parse("beam://my-volume/uploads/data.tar")

    assert remote.path == "my-volume/uploads/data.tar"
    assert remote.name == "data.tar"
    assert RemotePath.parse("beam://my-volume").path == "my-volume/"


def test_joined_remote_path_uses_forward_slashes_on_windows(windows_os_path):
    runs = RemotePath.parse("beam://my-volume/runs")

    joined = runs / "exp/metrics.csv"
    assert joined.volume_path == "runs/exp/metrics.csv"
    assert joined.path == "my-volume/runs/exp/metrics.csv"

    child = runs / RemotePath("beam", "my-volume", "model.pth", is_dir=False)
    assert child.volume_path == "runs/model.pth"
    assert child.is_dir is False


def test_stat_path_request_uses_forward_slashes_on_windows(windows_os_path):
    requested_paths = []

    class FakeVolumeService:
        def stat_path(self, request):
            requested_paths.append(request.path)
            path_info = SimpleNamespace(is_dir=False)
            return SimpleNamespace(ok=True, err_msg="", path_info=path_info)

    handler = multipart.Beta9Handler(service=FakeVolumeService())
    handler.is_dir(RemotePath.parse("beam://my-volume/runs/metrics.csv"))

    assert requested_paths == ["my-volume/runs/metrics.csv"]
