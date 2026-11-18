import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

import pytest
import requests

from beta9.client import client as client_module


@pytest.fixture
def upload_client(monkeypatch, tmp_path):
    signed_requests = []
    uploads = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(client_module, "is_remote", lambda: False)
    monkeypatch.setattr(client_module, "get_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(client_module.uuid, "uuid4", lambda: "fixed-id")

    def response(value):
        result = requests.Response()
        result.status_code = 200
        result._content = json.dumps(value).encode()
        return result

    def request(method, url, **kwargs):
        # Exercise Requests' actual URL preparation without sending traffic.
        prepared = requests.Request(method, url, headers=kwargs.get("headers")).prepare()
        if "/workspace/current" in prepared.path_url:
            return response({"external_id": "workspace"})
        signed_requests.append(prepared)
        return response("https://storage.example.test/signed")

    def put(url, data):
        uploads.append(data.read())
        return response(None)

    monkeypatch.setattr(client_module.requests, "request", request)
    monkeypatch.setattr(client_module.requests, "put", put)
    client = client_module.Client(
        token="test-token",
        gateway_host="gateway.example.test",
        gateway_port=443,
        tls=True,
        api_url="https://gateway.example.test",
    )
    return client, signed_requests, uploads


@pytest.mark.parametrize(
    "name",
    [
        "ordinary.wav",
        "literal%.wav",
        "literal%2F.wav",
        "literal%25.wav",
        "literal%23.wav",
        "literal%252F.wav",
        "literal+plus.wav",
        "literal%2B.wav",
        "你好.wav",
        "media%25/clip+one.wav",
        "clip#final.wav",
        "clip?take=1.wav",
        "media#one/clip.wav",
        "media?one/clip.wav",
        "café take.wav",
    ],
)
@pytest.mark.parametrize("absolute", [False, True])
def test_upload_paths_are_not_query_or_fragment(upload_client, name, absolute):
    client, signed_requests, uploads = upload_client
    path = Path(name)
    if absolute:
        path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"audio payload")
    expected = str(path.parent / f"{path.stem}_fixed-id{path.suffix}")

    assert client.upload_file(str(path)) == "https://storage.example.test/signed"
    assert uploads == [b"audio payload"]
    assert len(signed_requests) == 2
    for prepared, operation in zip(signed_requests, ["upload", "download"]):
        transport = urlsplit(prepared.path_url)
        assert not transport.query
        assert not urlsplit(prepared.url).fragment
        assert transport.path.startswith(f"/volume/workspace/generate-{operation}-url/uploads%2F")
        assert unquote(transport.path) == (
            f"/volume/workspace/generate-{operation}-url/uploads/{expected}"
        )


def test_hash_suffixes_do_not_collapse_signed_paths(upload_client):
    client, signed_requests, uploads = upload_client
    for name in ["clip#first.wav", "clip#second.wav"]:
        Path(name).write_bytes(name.encode())
        client.upload_file(name)
    upload_paths = [request.path_url for request in signed_requests[::2]]
    assert len(set(upload_paths)) == 2
    assert len(uploads) == 2
