import io
import zipfile
from unittest.mock import MagicMock

import grpc
import pytest

from beta9.abstractions.output import Output, OutputSaveError
from beta9.clients.output import OutputSaveResponse


@pytest.fixture
def output_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTAINER_ID", "container")
    monkeypatch.setenv("TASK_ID", "task")
    monkeypatch.setattr(Output, "_tmp_dir", tmp_path / "tmp")
    # A writable mount can accept files without publishing them to object storage.
    monkeypatch.setattr(
        "beta9.abstractions.output.USER_OUTPUTS_DIR",
        str(tmp_path / "mounted-outputs"),
        raising=False,
    )


@pytest.mark.parametrize("storage_available", ["true", "false"])
@pytest.mark.parametrize("contents", [b"", b"transcription" * 10])
def test_save_publishes_before_returning(
    output_env, monkeypatch, tmp_path, storage_available, contents
):
    monkeypatch.setenv("STORAGE_AVAILABLE", storage_available)
    monkeypatch.setattr("beta9.abstractions.output.CHUNK_SIZE", 7)
    source = tmp_path / "transcript.txt"
    source.write_bytes(contents)
    output = Output(path=source)
    uploaded = {}

    def save(stream):
        assert output.id is None
        requests = list(stream)
        assert requests
        assert all(r.task_id == "task" and r.filename == source.name for r in requests)
        uploaded["saved-output"] = b"".join(r.content for r in requests)
        return OutputSaveResponse(ok=True, id="saved-output")

    output.stub = MagicMock()
    output.stub.output_save_stream.side_effect = save

    assert output.save() is output
    assert output.id == "saved-output"
    assert uploaded[output.id] == contents
    assert not (tmp_path / "mounted-outputs").exists()


@pytest.mark.parametrize("storage_available", ["true", "false"])
def test_save_reports_upload_failure(output_env, monkeypatch, tmp_path, storage_available):
    monkeypatch.setenv("STORAGE_AVAILABLE", storage_available)
    source = tmp_path / "transcript.txt"
    source.write_text("transcription")
    output = Output(path=source)
    output.stub = MagicMock()
    output.stub.output_save_stream.return_value = OutputSaveResponse(err_msg="upload failed")

    with pytest.raises(OutputSaveError, match="upload failed"):
        output.save()
    assert output.id is None


def test_save_propagates_transport_failure(output_env, monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_AVAILABLE", "true")
    source = tmp_path / "transcript.txt"
    source.write_text("transcription")
    output = Output(path=source)
    output.stub = MagicMock()
    output.stub.output_save_stream.side_effect = grpc.RpcError("connection lost")

    with pytest.raises(grpc.RpcError, match="connection lost"):
        output.save()
    assert output.id is None


def test_save_uploads_directory_as_zip(output_env, monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_AVAILABLE", "true")
    source = tmp_path / "transcripts"
    source.mkdir()
    (source / "transcript.txt").write_text("transcription")
    output = Output(path=source)
    requests = []

    def save(stream):
        requests.extend(stream)
        return OutputSaveResponse(ok=True, id="saved-output")

    output.stub = MagicMock()
    output.stub.output_save_stream.side_effect = save
    output.save()
    output.zipped_path.parent.chmod(0o755)

    assert requests
    assert all(r.task_id == "task" and r.filename == "transcripts.zip" for r in requests)
    with zipfile.ZipFile(io.BytesIO(b"".join(r.content for r in requests))) as archive:
        assert archive.read("transcript.txt") == b"transcription"
