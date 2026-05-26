import unittest

from benchmarks.sandbox_startup_report import (
    build_startup_report,
    container_event_targets,
    event_batch_score,
    first_slowest_phase,
    render_markdown,
    select_primary_bottleneck,
    summarize_values,
)


class SandboxStartupReportTests(unittest.TestCase):
    def test_summarize_values_uses_expected_percentiles(self):
        summary = summarize_values([10, 20, 30, 40, 50])

        self.assertEqual(summary["count"], 5)
        self.assertEqual(summary["p50"], 30)
        self.assertEqual(summary["p95"], 48)
        self.assertEqual(summary["max"], 50)

    def test_time_to_interactive_excludes_warmup(self):
        benchmark = {
            "summary": {},
            "samples": [
                {"warmup": True, "ok": True, "execCompleteMs": 10, "execExitCode": 0},
                {"warmup": False, "ok": True, "execCompleteMs": 100, "execExitCode": 0},
                {"warmup": False, "ok": True, "execCompleteMs": 200, "execExitCode": 0},
            ],
        }

        report = build_startup_report(benchmark)

        self.assertEqual(report["verdict"]["interactive"], 2)
        self.assertEqual(report["timeToInteractive"]["count"], 2)
        self.assertEqual(report["timeToInteractive"]["p50"], 150)

    def test_primary_bottleneck_ignores_low_coverage_outlier(self):
        phases = [
            {
                "eventId": "rare.outlier",
                "count": 1,
                "coverage": 0.01,
                "coverageStatus": "low",
                "p95Ms": 100_000,
                "rollup": False,
            },
            {
                "eventId": "scheduler.backlog_wait",
                "count": 90,
                "coverage": 0.9,
                "coverageStatus": "partial",
                "p95Ms": 2_000,
                "rollup": False,
            },
        ]

        bottleneck = select_primary_bottleneck(phases, 100)

        self.assertEqual(bottleneck["eventId"], "scheduler.backlog_wait")

    def test_markdown_includes_verdict_bottleneck_and_data_quality(self):
        benchmark = {
            "summary": {
                "execCompleteMs": {
                    "count": 1,
                    "min": 100,
                    "p50": 100,
                    "p90": 100,
                    "p95": 100,
                    "max": 100,
                },
                "batch": {
                    "okCount": 1,
                    "count": 1,
                    "wallMs": 150,
                    "throughputPerSecond": 6.6,
                },
            },
            "samples": [
                {
                    "warmup": False,
                    "ok": True,
                    "containerId": "container-1",
                    "execCompleteMs": 100,
                    "runningObservedMs": 80,
                    "execExitCode": 0,
                }
            ],
        }
        events = {
            "items": [
                {
                    "container_id": "container-1",
                    "event_count": 4,
                    "missing": [],
                    "slowest_lifecycle": [
                        {"event_id": "scheduler.backlog_wait", "duration_ms": 50}
                    ],
                    "summary": {"scheduler_backlog_ms": 50},
                }
            ],
            "summary": {
                "scheduler_backlog_ms": {
                    "count": 1,
                    "min_ms": 50,
                    "p50_ms": 50,
                    "p90_ms": 50,
                    "p95_ms": 50,
                    "max_ms": 50,
                    "total_ms": 50,
                }
            },
        }

        markdown = render_markdown(build_startup_report(benchmark, events=events))

        self.assertIn("Verdict: 1/1 sandboxes interactive", markdown)
        self.assertIn("Primary bottleneck: scheduler.backlog_wait", markdown)
        self.assertIn("Event coverage: 1/1 containers", markdown)

    def test_image_drilldown_aggregates_clip_accesses(self):
        benchmark = {
            "summary": {},
            "samples": [
                {
                    "warmup": False,
                    "ok": True,
                    "containerId": "container-1",
                    "execCompleteMs": 100,
                    "runningObservedMs": 80,
                    "execExitCode": 0,
                },
                {
                    "warmup": False,
                    "ok": True,
                    "containerId": "container-2",
                    "execCompleteMs": 120,
                    "runningObservedMs": 90,
                    "execExitCode": 0,
                },
            ],
        }
        events = {
            "items": [
                {
                    "container_id": "container-1",
                    "event_count": 3,
                    "missing": [],
                    "clip_accesses": [
                        {
                            "operation": "clip.oci_read",
                            "path": "/usr/local/lib/python.py",
                            "source": "content_cache",
                            "count": 2,
                            "total_us": 2500,
                            "max_us": 1500,
                            "bytes_read": 4096,
                        }
                    ],
                    "summary": {"clip_oci_read_ms": 3, "image_ms": 1},
                },
                {
                    "container_id": "container-2",
                    "event_count": 3,
                    "missing": [],
                    "clip_accesses": [
                        {
                            "operation": "clip.oci_read",
                            "path": "/usr/local/lib/python.py",
                            "source": "content_cache",
                            "count": 1,
                            "total_us": 1000,
                            "max_us": 1000,
                            "bytes_read": 2048,
                        }
                    ],
                    "summary": {"clip_oci_read_ms": 1, "image_ms": 1},
                },
            ],
            "summary": {
                "clip_oci_read_ms": {
                    "count": 2,
                    "min_ms": 1,
                    "p50_ms": 2,
                    "p90_ms": 2.8,
                    "p95_ms": 2.9,
                    "max_ms": 3,
                    "total_ms": 4,
                }
            },
        }

        report = build_startup_report(benchmark, events=events)
        access = report["imageDrilldown"]["clipAccesses"][0]
        markdown = render_markdown(report)

        self.assertEqual(report["imageDrilldown"]["containersWithClipAccesses"], 2)
        self.assertEqual(access["count"], 3)
        self.assertEqual(access["containerCount"], 2)
        self.assertEqual(access["totalMs"], 4)
        self.assertIn("## Image And CLIP Drilldown", markdown)
        self.assertIn("/usr/local/lib/python.py", markdown)

    def test_event_batch_score_prefers_richer_complete_payload(self):
        early = {
            "items": [
                {
                    "container_id": "container-1",
                    "event_count": 3,
                    "missing": [],
                    "summary": {"image_ms": 1},
                }
            ]
        }
        later = {
            "items": [
                {
                    "container_id": "container-1",
                    "event_count": 8,
                    "missing": [],
                    "summary": {"image_ms": 1, "scheduler_backlog_ms": 10},
                }
            ]
        }

        self.assertGreater(event_batch_score(later), event_batch_score(early))

    def test_report_prefers_api_phase_and_coverage_contract(self):
        benchmark = {
            "summary": {},
            "samples": [
                {
                    "warmup": False,
                    "ok": True,
                    "containerId": "container-1",
                    "execCompleteMs": 100,
                    "execExitCode": 0,
                }
            ],
        }
        events = {
            "items": [{"container_id": "container-1", "event_count": 4, "summary": {}}],
            "coverage": {
                "requested_containers": 1,
                "items": 1,
                "containers_with_events": 1,
                "required_lifecycle_present": 3,
                "required_lifecycle_total": 3,
                "required_lifecycle_missing": {},
                "required_metric_present": 7,
                "required_metric_total": 7,
                "required_metric_missing": {},
            },
            "phases": [
                {
                    "metric_key": "network_setup_ms",
                    "event_id": "network.setup",
                    "label": "Network setup",
                    "count": 1,
                    "coverage": 1,
                    "coverage_status": "full",
                    "p50_ms": 10,
                    "p95_ms": 20,
                    "max_ms": 30,
                },
                {
                    "metric_key": "clip_read_total_us",
                    "event_id": "clip.read",
                    "count": 1,
                    "coverage": 1,
                    "p95_ms": 5000,
                }
            ],
            "primary_bottleneck": {
                "metric_key": "network_setup_ms",
                "event_id": "network.setup",
                "label": "Network setup",
                "count": 1,
                "coverage": 1,
                "coverage_status": "full",
                "p95_ms": 20,
            },
        }

        report = build_startup_report(benchmark, events=events)

        self.assertEqual(report["primaryBottleneck"]["eventId"], "network.setup")
        self.assertEqual(report["eventCoverage"]["requiredMetricPresent"], 7)
        self.assertEqual(report["serverPhases"][0]["metricKey"], "network_setup_ms")
        self.assertEqual(len(report["serverPhases"]), 1)

    def test_slowest_phase_uses_per_container_derived_summary(self):
        phase = first_slowest_phase(
            {
                "summary": {
                    "scheduler_queue_to_worker_receive_ms": 50_000,
                    "sandbox_process_manager_ready_ms": 2_000,
                },
                "slowest_lifecycle": [
                    {"event_id": "sandbox.process_manager_ready", "duration_ms": 2_000}
                ],
            }
        )

        self.assertEqual(phase["eventId"], "scheduler.queue_to_worker_receive")
        self.assertEqual(phase["durationMs"], 50_000)

    def test_container_event_targets_include_stub_ids(self):
        targets = container_event_targets(
            [
                {"containerId": "container-1", "stubId": "stub-1"},
                {"containerId": "container-1", "stubId": "stub-1"},
                {"containerId": "container-2"},
            ]
        )

        self.assertEqual(
            targets,
            [
                {"container_id": "container-1", "stub_id": "stub-1"},
                {"container_id": "container-2"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
