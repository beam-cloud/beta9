import json
from unittest.mock import MagicMock, patch

from beta9.clients import managedendpoint as pb
from beta9.harness import Harness

ENV = {
    "BETA9_GATEWAY_HOST": "gw",
    "BETA9_GATEWAY_PORT": "1993",
    "BEAM_REPLICA_SECRET": "s",
    "BEAM_HARNESS_ENABLED": "true",
    "BEAM_HARNESS_CONFIG": json.dumps({"tokens_per_sec": 50}),
}


class SyncEngine:
    engine, engine_version = "fake", "1"

    def __init__(self):
        self.config = {"tokens_per_sec": 10}
        self.drained = None

    def capabilities(self):
        return {"knobs": ["tokens_per_sec"]}

    def apply(self, config, revision):
        if "bad" in config:
            raise ValueError("unknown knob 'bad'")
        self.config.update(config)
        return dict(self.config)

    def capacity(self):
        return {"in_flight": 1, "max_concurrency": 4, "unknown": 9}

    def draining(self):
        return self.drained is not None

    def drain(self, seconds):
        self.drained = seconds


class DeferredEngine(SyncEngine):
    def apply(self, config, revision):
        self.pending = (config, revision)
        return None


def harness_with_stub(engine):
    with patch("beta9.harness.Channel"):
        harness = Harness(engine, env=ENV, log=lambda _: None)
    harness.stub = MagicMock()
    harness.stub.register.return_value = pb.HarnessRegisterResponse(
        ok=True,
        replica_id="r1",
        endpoint_id="acme/m",
        heartbeat_interval_seconds=7,
        current=pb.ReplicaConfig(revision=3, config_json=json.dumps({"tokens_per_sec": 20})),
    )
    return harness


def test_register_applies_seed_then_current_revision_and_acks():
    engine = SyncEngine()
    harness = harness_with_stub(engine)
    with patch("beta9.harness.threading.Thread"):
        harness.start()
    assert harness.replica_id == "r1"
    assert harness.interval == 7
    assert harness.applied_revision == 3
    assert engine.config["tokens_per_sec"] == 20
    ack = harness.stub.ack_config.call_args.args[0]
    assert (
        ack.revision == 3 and ack.applied and json.loads(ack.effective_json)["tokens_per_sec"] == 20
    )


def test_rejected_config_acks_with_error():
    engine = SyncEngine()
    harness = harness_with_stub(engine)
    harness.replica_id = "r1"
    harness.apply(5, json.dumps({"bad": 1}))
    ack = harness.stub.ack_config.call_args.args[0]
    assert ack.revision == 5 and not ack.applied and "bad" in ack.error
    assert harness.applied_revision == 0


def test_deferred_engine_acks_later():
    engine = DeferredEngine()
    harness = harness_with_stub(engine)
    harness.replica_id = "r1"
    harness.apply(8, json.dumps({"tokens_per_sec": 1}))
    harness.stub.ack_config.assert_not_called()
    assert engine.pending == ({"tokens_per_sec": 1}, 8)
    harness.ack(8, effective={"tokens_per_sec": 1})
    assert harness.applied_revision == 8
    assert harness.stub.ack_config.call_args.args[0].applied


def test_heartbeat_filters_capacity_honours_drain_and_flushes_events():
    engine = SyncEngine()
    harness = harness_with_stub(engine)
    harness.replica_id = "r1"
    harness.stub.heartbeat.return_value = pb.HarnessHeartbeatResponse(
        ok=True, drain=True, drain_seconds=9
    )
    harness.publish("engine.metrics", {"tps": 1})
    harness.heartbeat()
    req = harness.stub.heartbeat.call_args.args[0]
    assert req.capacity.in_flight == 1 and req.capacity.max_concurrency == 4
    assert req.status == "ready"
    assert engine.drained == 9
    published = harness.stub.publish_events.call_args.args[0]
    assert published.replica_id == "r1" and published.events[0].name == "engine.metrics"
