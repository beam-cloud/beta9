import json
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest

from beta9.harness_client import HarnessClient, _decode


def test_patch_preserves_repository_seed_before_first_revision():
    client = HarnessClient("https://gateway.invalid", "test-token")
    client.replica = Mock(
        return_value=_decode(
            {
                "config": None,
                "engine_metrics_json": json.dumps(
                    {"effective": {"max_num_seqs": 32, "instrumentation": "metrics"}}
                ),
            }
        )
    )
    client.set = Mock()
    client.patch("replica", {"max_num_seqs": 16}, author="test-agent")
    client.set.assert_called_once_with(
        "replica", {"max_num_seqs": 16, "instrumentation": "metrics"}, author="test-agent"
    )


def test_current_prefers_acknowledged_effective_config_over_heartbeat():
    client = HarnessClient("https://gateway.invalid", "test-token")
    client.replica = Mock(
        return_value=_decode(
            {
                "config": {
                    "applied": True,
                    "revision": "3",
                    "acked_revision": "3",
                    "effective_json": '{"max_num_seqs":16}',
                },
                "engine_metrics_json": '{"effective":{"max_num_seqs":32}}',
            }
        )
    )
    assert client.current("replica") == {"max_num_seqs": 16}
    client.replica.return_value["config"]["applied"] = False
    assert client.current("replica") == {"max_num_seqs": 32}
    client.replica.return_value["engine_metrics"] = {}
    with pytest.raises(ValueError, match="not available"):
        client.current("replica")


def test_revision_filter_and_model_path_reach_http_request(monkeypatch):
    requests = []
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read.return_value = b'{"ok":true}'

    def open_request(request, **kwargs):
        requests.append(request)
        return response

    monkeypatch.setattr("beta9.harness_client.urlopen", open_request)
    client = HarnessClient("https://gateway.invalid", "test-token")
    client.metrics("qwen/qwen3-8b", replica_id="replica", config_revision=0)
    parsed = urlsplit(requests[0].full_url)
    assert parsed.path == "/api/v1/endpoints/qwen%2Fqwen3-8b/metrics"
    assert parse_qs(parsed.query)["config_revision"] == ["0"]
    assert requests[0].get_header("Authorization") == "Bearer test-token"
