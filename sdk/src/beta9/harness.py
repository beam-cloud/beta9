"""
Replica harness: the in-container client of the gateway's EndpointHarnessService.

An inference engine running as a managed endpoint replica hands ``Harness`` an
object implementing ``HarnessEngine`` and calls ``start()``. The harness then
registers the replica, applies the seed config from the repo, streams live
config set by tuning agents (acknowledging each), heartbeats liveness and capacity, honours
drain requests from the control plane and forwards engine events. It is
engine-agnostic: vLLM, the fake engine and any custom server use the same class.

The gateway provisions the contract through the environment: BETA9_GATEWAY_HOST
and BETA9_GATEWAY_PORT, BEAM_REPLICA_SECRET (the replica's only credential),
BEAM_HARNESS_ENABLED and an optional BEAM_HARNESS_CONFIG seed.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, Mapping, Optional, Protocol

from .channel import Channel
from .clients import managedendpoint as pb

REPLICA_SECRET_HEADER = "x-beam-replica-secret"
MAX_EVENTS_PER_PUBLISH = 256  # the gateway accepts this many per call
CAPACITY_FIELDS = frozenset(
    "in_flight max_concurrency running waiting kv_cache_free_milli decode_tokens_per_sec "
    "prompt_tokens_per_sec ttft_ms tpot_ms prefix_cache_hit_milli".split()
)


class HarnessEngine(Protocol):
    """What the harness needs from an engine.

    ``apply`` returns the effective config after the change and raises when the
    config is rejected. Engines that can only reconfigure on their own thread
    return ``None`` (the revision is now pending) and later call
    ``Harness.ack(revision, effective=...)`` or ``Harness.ack(revision, error=...)``.
    ``drain`` must stop admitting work and exit the process once in-flight work
    is done. Optional ``ready()`` reports whether the engine can serve (default
    True; return False while reloading a model or config so the gateway routes
    around the replica). Optional ``metrics()`` is forwarded with every heartbeat."""

    engine: str
    engine_version: str

    def capabilities(self) -> Dict[str, Any]: ...

    def apply(self, config: Dict[str, Any], revision: int) -> Optional[Dict[str, Any]]: ...

    def capacity(self) -> Dict[str, Any]: ...

    def draining(self) -> bool: ...

    def drain(self, seconds: int) -> None: ...


class Harness:
    def __init__(
        self,
        engine: HarnessEngine,
        env: Optional[Mapping[str, str]] = None,
        log=lambda msg: print(f"[harness] {msg}", flush=True),
        max_pending_events: int = 1000,
    ) -> None:
        self.env = env if env is not None else os.environ
        self.engine = engine
        self.log = log
        self.stop = threading.Event()
        self.replica_id = ""
        self.interval = 5
        self.applied_revision = 0
        self.seen_revision = 0  # last revision delivered, applied or rejected
        self._events: Deque[pb.Event] = deque(maxlen=max_pending_events)
        self._dropped = 0
        self._lock = threading.Lock()
        channel = Channel(
            addr=f"{self.env['BETA9_GATEWAY_HOST']}:{self.env['BETA9_GATEWAY_PORT']}",
            metadata=[(REPLICA_SECRET_HEADER, self.env["BEAM_REPLICA_SECRET"])],
        )
        self.stub = pb.EndpointHarnessServiceStub(channel)

    @staticmethod
    def enabled(env: Optional[Mapping[str, str]] = None) -> bool:
        env = env if env is not None else os.environ
        return env.get("BEAM_HARNESS_ENABLED", "").lower() == "true"

    # --- lifecycle -----------------------------------------------------------

    def start(self, register_attempts: int = 60) -> None:
        """Apply the seed config (revision 0, never acked; a bad seed is logged
        and skipped so a typo in app.py cannot keep a replica from serving),
        register (retrying while the gateway is unreachable) and start the
        watch and heartbeat threads."""
        seed = self.env.get("BEAM_HARNESS_CONFIG")
        if seed:
            try:
                self.engine.apply(json.loads(seed), 0)
            except Exception as exc:  # noqa: BLE001
                self.log(f"ignoring invalid BEAM_HARNESS_CONFIG seed: {exc}")
        self.register(register_attempts)
        threading.Thread(target=self._watch, name="harness-watch", daemon=True).start()
        threading.Thread(target=self._heartbeat, name="harness-heartbeat", daemon=True).start()

    def close(self) -> None:
        self.stop.set()

    def register(self, attempts: int) -> None:
        req = pb.HarnessRegisterRequest(
            container_id=self.env.get("CONTAINER_ID", ""),
            engine=self.engine.engine,
            engine_version=self.engine.engine_version,
            capabilities_json=json.dumps(self.engine.capabilities()),
            limits_json=json.dumps(
                {"max_concurrency": self.engine.capacity().get("max_concurrency", 0)}
            ),
        )
        for attempt in range(attempts):
            try:
                resp = self.stub.register(req)
                if not resp.ok:
                    raise RuntimeError(f"rejected: {resp.err_msg}")
            except Exception as exc:  # noqa: BLE001
                self.log(f"register failed ({exc}); retrying")
                time.sleep(min(10, 1 + attempt))
                continue
            self.replica_id = resp.replica_id
            self.interval = max(1, resp.heartbeat_interval_seconds or 5)
            if resp.current and resp.current.config_json:
                self.apply(resp.current.revision, resp.current.config_json)
            self.log(f"registered as {self.replica_id} ({resp.endpoint_id} on {resp.gpu})")
            return
        raise RuntimeError("could not register with the control plane")

    # --- config ----------------------------------------------------------------

    def apply(self, revision: int, config_json: str) -> None:
        self.seen_revision = max(self.seen_revision, revision)
        try:
            effective = self.engine.apply(json.loads(config_json or "{}"), revision)
        except Exception as exc:  # noqa: BLE001
            self.ack(revision, error=str(exc))
            return
        if effective is not None:
            self.ack(revision, effective=effective)

    def ack(
        self, revision: int, effective: Optional[Dict[str, Any]] = None, error: Optional[str] = None
    ) -> None:
        """Report the outcome of a revision. Called by ``apply`` for synchronous
        engines and by the engine itself after a deferred apply."""
        ack = pb.HarnessAckConfigRequest(replica_id=self.replica_id, revision=revision)
        if error:
            ack.error = error
            self.log(f"revision {revision} rejected: {error}")
        else:
            self.applied_revision = revision
            ack.applied, ack.effective_json = True, json.dumps(effective or {})
            self.log(f"applied revision {revision}")
        try:
            resp = self.stub.ack_config(ack)
            if not resp.ok:
                self.log(f"ack refused: {resp.err_msg}")
        except Exception as exc:  # noqa: BLE001
            self.log(f"ack failed: {exc}")

    def _watch(self) -> None:
        while not self.stop.is_set():
            try:
                # Resume after the last revision seen, not the last applied, so
                # a rejected revision is not re-delivered on every reconnect.
                req = pb.HarnessWatchConfigRequest(
                    replica_id=self.replica_id, after_revision=self.seen_revision
                )
                for rev in self.stub.watch_config(req):
                    if self.stop.is_set():
                        return
                    self.apply(rev.revision, rev.config_json)
            except Exception as exc:  # noqa: BLE001
                if self.stop.is_set():
                    return
                self.log(f"watch disconnected ({exc}); reconnecting")
                self.stop.wait(2)

    # --- liveness and events -------------------------------------------------

    def publish(self, name: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """Queue an engine event; batches are forwarded with the next heartbeat."""
        event = pb.Event(
            name=name, payload_json=json.dumps(payload or {}), at_unix_ms=int(time.time() * 1000)
        )
        with self._lock:
            if len(self._events) == self._events.maxlen:
                self._dropped += 1
            self._events.append(event)

    def heartbeat(self) -> None:
        """One heartbeat: report status and capacity, pick up drain requests, flush events."""
        capacity = {k: int(v) for k, v in self.engine.capacity().items() if k in CAPACITY_FIELDS}
        metrics = getattr(self.engine, "metrics", None)
        ready = getattr(self.engine, "ready", None)
        if self.engine.draining():
            status = "draining"
        elif callable(ready) and not ready():
            status = "loading"
        else:
            status = "ready"
        req = pb.HarnessHeartbeatRequest(
            replica_id=self.replica_id,
            status=status,
            capacity=pb.ReplicaCapacity(**capacity),
            applied_revision=self.applied_revision,
            metrics_json=json.dumps(metrics()) if callable(metrics) else "",
        )
        if dropped := self._take_dropped():
            self.log(f"{dropped} events dropped since last heartbeat")
        resp = self.stub.heartbeat(req)
        if resp.heartbeat_interval_seconds:
            self.interval = max(1, resp.heartbeat_interval_seconds)
        if resp.drain and not self.engine.draining():
            self.log(f"drain requested ({resp.drain_seconds}s)")
            self.engine.drain(resp.drain_seconds or 5)
        self._flush_events()

    def _heartbeat(self) -> None:
        while not self.stop.is_set():
            try:
                self.heartbeat()
            except Exception as exc:  # noqa: BLE001
                self.log(f"heartbeat failed: {exc}")
            self.stop.wait(self.interval)

    def _take_dropped(self) -> int:
        with self._lock:
            dropped, self._dropped = self._dropped, 0
            return dropped

    def _flush_events(self) -> None:
        """Publish queued events in gateway-sized batches; a failed batch stays queued."""
        while True:
            with self._lock:
                batch = [self._events.popleft() for _ in range(min(len(self._events), MAX_EVENTS_PER_PUBLISH))]
            if not batch:
                return
            try:
                self.stub.publish_events(
                    pb.HarnessPublishEventsRequest(replica_id=self.replica_id, events=batch)
                )
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._events.extendleft(reversed(batch))  # keep order; a full deque drops the newest
                self.log(f"publish failed ({exc}); {len(batch)} events kept for the next heartbeat")
                return
