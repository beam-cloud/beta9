"""Engine-independent hosted endpoint control client; no inference runtime required."""

import json
from typing import Any, Dict, Optional
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


def _decode(record: Dict[str, Any]) -> Dict[str, Any]:
    for name in ("capabilities", "engine_metrics", "spec", "placements", "effective", "config"):
        raw = record.get(name + "_json")
        if isinstance(raw, str):
            record[name] = json.loads(raw) if raw else {}
    if isinstance(record.get("config"), dict):
        _decode(record["config"])
    return record


class HarnessClient:
    """Use a cluster-admin token with the gateway URL (not an inference replica URL).

    ``current`` returns the effective settings to save before an experiment.
    ``set`` applies a full config; ``patch`` preserves settings not in the change.
    A rollback is another audited ``set`` of the saved effective settings.
    """

    def __init__(self, gateway_url: str, token: str, timeout: float = 30):
        self.gateway_url = gateway_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _request(self, method, path, body=None, query=None, timeout=None):
        url = self.gateway_url + "/api/v1" + path
        if query:
            url += "?" + urlencode({k: v for k, v in query.items() if v is not None})
        request = Request(
            url,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=timeout or self.timeout) as response:
                payload = json.load(response)
        except HTTPError as exc:
            try:
                payload = json.loads(exc.read())
            except ValueError:
                payload = {}
            raise ValueError(
                f"{method} {path}: HTTP {exc.code}: {payload.get('err_msg') or payload.get('message') or exc.reason}"
            ) from None
        if payload.get("ok") is False:
            raise ValueError(f"{method} {path}: {payload.get('err_msg')}")
        return payload

    def endpoint(self, endpoint_id: str) -> Dict[str, Any]:
        result = self._request("GET", "/endpoints/" + quote(endpoint_id, safe=""))
        result["endpoint"] = _decode(result["endpoint"])
        result["replicas"] = [_decode(r) for r in result.get("replicas") or []]
        return result

    def replicas(self, endpoint_id: Optional[str] = None):
        path = (
            "/endpoints" + ("/" + quote(endpoint_id, safe="") if endpoint_id else "") + "/replicas"
        )
        return [_decode(r) for r in self._request("GET", path).get("replicas") or []]

    def replica(self, replica_id: str):
        for replica in self.replicas():
            if replica.get("id") == replica_id:
                return replica
        raise ValueError("Replica not found: " + replica_id)

    def current(self, replica_id: str) -> Dict[str, Any]:
        replica = self.replica(replica_id)
        config = replica.get("config") or {}
        # Only an acknowledged successful revision is authoritative. An
        # unacknowledged/rejected update must not become the rollback baseline.
        if config.get("applied") and int(config.get("acked_revision") or 0) == int(
            config.get("revision") or 0
        ):
            effective = config.get("effective")
            if isinstance(effective, dict):
                return effective
        effective = (replica.get("engine_metrics") or {}).get("effective")
        if not isinstance(effective, dict):
            raise ValueError("Effective configuration is not available yet")
        return effective

    def set(
        self,
        replica_id: str,
        config: Dict[str, Any],
        wait: int = 30,
        author: str = "beta9.harness_client",
    ):
        result = self._request(
            "POST",
            "/endpoints/replicas/" + quote(replica_id, safe="") + "/config",
            body={"config_json": json.dumps(config), "author": author, "wait_seconds": wait},
            timeout=self.timeout + wait,
        )
        replica = _decode(result["replica"])
        ack = replica.get("config") or {}
        if int(ack.get("acked_revision") or 0) < int(ack.get("revision") or 0):
            raise TimeoutError("Configuration was not acknowledged within the wait window")
        if not ack.get("applied") or ack.get("error"):
            raise ValueError(ack.get("error") or "Configuration was not applied")
        return replica

    def patch(self, replica_id: str, changes: Dict[str, Any], **kwargs):
        return self.set(replica_id, {**self.current(replica_id), **changes}, **kwargs)

    def metrics(
        self,
        endpoint_id: str,
        replica_id: Optional[str] = None,
        config_revision: Optional[int] = None,
        window_seconds: int = 60,
    ):
        return self._request(
            "GET",
            "/endpoints/" + quote(endpoint_id, safe="") + "/metrics",
            query={
                "replica_id": replica_id,
                "config_revision": config_revision,
                "window_seconds": window_seconds,
            },
        )

    def history(self, workspace_id: str, start_time: str, container_id: Optional[str] = None):
        return self._request(
            "GET",
            "/events/" + quote(workspace_id, safe="") + "/history",
            query={
                "event_types": "endpoint.config",
                "start_time": start_time,
                "container_id": container_id,
            },
        )
