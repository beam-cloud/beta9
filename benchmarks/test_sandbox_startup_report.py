import unittest

from benchmarks.sandbox_startup_report import (
    build_startup_report,
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


if __name__ == "__main__":
    unittest.main()
