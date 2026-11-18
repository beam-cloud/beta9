import io
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from beta9.abstractions.output import Output, OutputCannotRunLocallyError, OutputTaskIdError
from beta9.clients.output import OutputSaveResponse


@pytest.fixture
def output_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTAINER_ID", "container")
    monkeypatch.setenv("TASK_ID", "task")
    root = tmp_path / "outputs"
    # Isolate file ownership from prepare_tmp_dir's existing directory-mode behavior.
    root.mkdir(mode=0o700)
    monkeypatch.setattr(Output, "_tmp_dir", root)
    return root


@pytest.mark.parametrize("contents", [b"", b"generated audio" * 100])
def test_from_file_owns_and_cleans_temporary_copy(output_env, contents):
    source = io.BytesIO(contents)
    output = Output.from_file(source)
    saved_path = output.path
    try:
        assert saved_path.read_bytes() == contents
        assert saved_path.parent == output_env / str(id(output))
    finally:
        # Exercise the actual cleanup method without relying on GC timing.
        output.__del__()
    assert not saved_path.exists()
    assert source.getvalue() == contents
    assert not source.closed


def test_from_file_cleans_partial_copy_on_read_failure(output_env):
    class BrokenReader:
        calls = 0

        def read(self, size=-1):
            self.calls += 1
            if self.calls == 1:
                return b"partial output"
            raise OSError("source read failed")

    with pytest.raises(OSError, match="source read failed"):
        Output.from_file(BrokenReader())
    assert list(output_env.iterdir()) == []


@pytest.mark.parametrize("rejection", ["local", "missing_task"])
def test_from_file_cleans_copy_if_output_creation_fails(output_env, monkeypatch, rejection):
    if rejection == "local":
        monkeypatch.delenv("CONTAINER_ID")
        error = OutputCannotRunLocallyError
    else:
        monkeypatch.delenv("TASK_ID")
        error = OutputTaskIdError
    with pytest.raises(error):
        Output.from_file(io.BytesIO(b"generated output"))
    assert list(output_env.iterdir()) == []


def test_direct_output_does_not_remove_callers_file(output_env, tmp_path):
    source = tmp_path / "user-owned.txt"
    source.write_bytes(b"keep this")
    output = Output(path=source)
    output.__del__()
    assert source.read_bytes() == b"keep this"


def test_from_file_preserves_source_cursor(output_env):
    source = io.BytesIO(b"skipkeep")
    source.seek(4)
    output = Output.from_file(source)
    try:
        assert output.path.read_bytes() == b"keep"
    finally:
        output.__del__()


def test_from_file_cleans_copy_on_rename_failure(output_env, monkeypatch):
    def fail_rename(self, target):
        raise OSError("cannot adopt output")

    monkeypatch.setattr(Path, "rename", fail_rename)
    with pytest.raises(OSError, match="cannot adopt output"):
        Output.from_file(io.BytesIO(b"generated output"))
    assert list(output_env.iterdir()) == []


def test_from_file_remains_available_for_save(output_env):
    output = Output.from_file(io.BytesIO(b"generated output"))
    requests = []

    def save(stream):
        requests.extend(stream)
        return OutputSaveResponse(ok=True, id="saved-output")

    output.stub = MagicMock()
    output.stub.output_save_stream.side_effect = save
    try:
        assert output.save() is output
        assert output.id == "saved-output"
        assert b"".join(request.content for request in requests) == b"generated output"
        assert all(request.filename == output.path.name for request in requests)
    finally:
        output.__del__()
