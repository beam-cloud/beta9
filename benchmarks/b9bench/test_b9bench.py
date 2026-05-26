import json
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path

from benchmarks.b9bench.config import resolve_run_config
from benchmarks.b9bench.config import http_url_from_gateway
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

    def test_default_output_dir_is_timestamped_under_benchmarks_runs(self):
        args = SimpleNamespace(
            command="sandbox",
            suite="sandbox-default",
            profile=None,
            config=None,
            token=None,
            gateway_url=None,
            grpc_addr=None,
            namespace=None,
            out_dir=None,
            dry_run=True,
            param=[],
            script_arg=[],
        )

        config = resolve_run_config(args)

        self.assertEqual(config.out_dir.parents[1], config.root / "benchmarks")
        self.assertEqual(config.out_dir.parent.name, "runs")
        self.assertIn("sandbox", config.out_dir.name)
        self.assertIn("sandbox-default", config.out_dir.name)

    def test_stage_gateway_profile_maps_to_stage_http_host(self):
        self.assertEqual(
            http_url_from_gateway("gateway.stage.beam.cloud", "443"),
            "https://app.stage.beam.cloud",
        )

    def test_profile_can_define_explicit_http_url(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.ini"
            config_path.write_text(
                "\n".join(
                    (
                        "[staging]",
                        "token = token-1",
                        "gateway_host = gateway.stage.beam.cloud",
                        "gateway_port = 443",
                        "gateway_http_url = https://custom.stage.example.com",
                        "",
                    )
                ),
                encoding="utf-8",
            )
            args = SimpleNamespace(
                command="sandbox",
                suite="sandbox-default",
                profile="staging",
                config=str(config_path),
                token=None,
                gateway_url=None,
                grpc_addr=None,
                namespace=None,
                out_dir=None,
                dry_run=True,
                param=[],
                script_arg=[],
            )

            config = resolve_run_config(args)

            self.assertEqual(config.gateway_url, "https://custom.stage.example.com")
            self.assertEqual(config.grpc_addr, "gateway.stage.beam.cloud:443")

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
