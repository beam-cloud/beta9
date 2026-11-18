import io
import json
from unittest.mock import Mock

import pytest
import requests
from requests.utils import get_encoding_from_headers

from beta9.client import task as task_module


@pytest.mark.parametrize("chunk_size", [1, 2, 7, 1024])
@pytest.mark.parametrize("name", ["plain.txt", "中文😀.txt", "café.txt"])
def test_subscribe_decodes_utf8(monkeypatch, name, chunk_size):
    payload = {
        "status": "COMPLETE",
        "result": None,
        "outputs": [
            {
                "name": name,
                "url": "https://storage.example.test/signed",
                "expires_in": 3600,
            }
        ],
    }
    data = ("event: status\ndata: " + json.dumps(payload, ensure_ascii=False) + "\n\n").encode(
        "utf-8"
    )
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "text/event-stream"
    response.encoding = get_encoding_from_headers(response.headers)
    assert response.encoding == "ISO-8859-1"
    response.raw = io.BytesIO(data)
    real_iter_content = response.iter_content
    response.iter_content = lambda **kwargs: real_iter_content(
        chunk_size=chunk_size, decode_unicode=kwargs["decode_unicode"]
    )
    # Bypass only the constructor's initial unrelated status HTTP request.
    task = object.__new__(task_module.Task)
    task.id = "local-test"
    task.url = "https://gateway.example.test/task/local-test"
    task._Task__token = "test-token"
    monkeypatch.setattr(task_module.requests, "get", Mock(return_value=response))
    seen = []
    assert task.subscribe(event_handler=seen.append) is None
    assert seen == [payload]
    assert task.outputs() == payload["outputs"]
    assert response.raw.closed
