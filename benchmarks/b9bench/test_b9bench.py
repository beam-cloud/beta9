import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.b9bench.model import Measurement, ScenarioSpec
from benchmarks.b9bench.payload import deterministic_payload_range, deterministic_sha256
from benchmarks.b9bench.reports import MetricSink
from benchmarks.b9bench.suites import SuiteLoader
from benchmarks.b9bench.validators import Validator


class B9BenchTests(unittest.TestCase):
    def test_deterministic_payload_is_stable_across_ranges(self):
        nonce = "nonce"
        label = "volume_mount:sequential:size:32mb"
        full = deterministic_payload_range(nonce, label, 0, 8192)
        split = deterministic_payload_range(nonce, label, 0, 4096) + deterministic_payload_range(
            nonce, label, 4096, 4096
        )
        self.assertEqual(full, split)
        self.assertEqual(
            deterministic_sha256(nonce, label, 8192),
            deterministic_sha256(nonce, label, 4096 + 4096),
        )

    def test_suite_loader_expands_cache_file_plan_entries(self):
        root = Path(__file__).resolve().parents[2]
        suite = SuiteLoader(root).load("cache-smoke")
        self.assertEqual(suite.kind, "cache")
        self.assertIn("volume_mount:sequential:32", suite.file_plan)
        self.assertIn("workspace_fuse:sequential:32", suite.file_plan)

    def test_validator_requires_cache_hit_when_requested(self):
        measurement = Measurement(
            run_id="run",
            suite="cache",
            scenario="s",
            measurement="sandbox_hot_read",
            timestamp="now",
            status="ok",
            tags={"requires_cache_hit": True, "requires_sha": True},
            evidence={"sha_ok": True, "cache_hit": False},
        )
        failures = Validator().validate([measurement])
        self.assertEqual(len(failures), 1)
        self.assertIn("missing embedded-cache hit proof", failures[0])

    def test_metric_sink_writes_jsonl_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink = MetricSink(Path(tmp))
            sink.open()
            sink.emit(
                Measurement(
                    run_id="run",
                    suite="cache",
                    scenario="s",
                    measurement="m",
                    timestamp="now",
                    mbps=123.4,
                    evidence={"sha_ok": True},
                )
            )
            sink.write_summary([])
            lines = sink.metrics_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])["mbps"], 123.4)
            self.assertEqual(json.loads(sink.summary_path.read_text())["status"], "ok")

    def test_scenario_tags_include_core_dimensions(self):
        scenario = ScenarioSpec(
            name="x",
            access="volume_mount",
            pattern="sequential",
            size_mib=1024,
            cache_state="strict_disk",
        )
        self.assertEqual(
            scenario.metric_tags,
            {
                "access": "volume_mount",
                "operation": "read",
                "pattern": "sequential",
                "size_mib": 1024,
                "cache_state": "strict_disk",
            },
        )


if __name__ == "__main__":
    unittest.main()
