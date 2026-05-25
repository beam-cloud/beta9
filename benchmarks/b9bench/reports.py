from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from .model import Measurement


class MetricSink:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.artifact_dir = out_dir / "artifacts"
        self.metrics_path = out_dir / "metrics.jsonl"
        self.summary_path = out_dir / "summary.json"
        self.markdown_path = out_dir / "summary.md"
        self.measurements: list[Measurement] = []

    def open(self) -> None:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_path.write_text("", encoding="utf-8")

    def emit(self, measurement: Measurement) -> None:
        self.measurements.append(measurement)
        with self.metrics_path.open("a", encoding="utf-8") as handle:
            handle.write(measurement.to_json() + "\n")

    def write_summary(self, failures: list[str]) -> None:
        summary = {
            "metrics": [measurement.as_dict() for measurement in self.measurements],
            "failures": failures,
            "status": "failed" if failures else "ok",
            "paths": {
                "metrics": str(self.metrics_path),
                "summary": str(self.summary_path),
                "report": str(self.markdown_path),
                "artifacts": str(self.artifact_dir),
            },
        }
        self.summary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        self.markdown_path.write_text(self._markdown(summary), encoding="utf-8")

    def _markdown(self, summary: dict) -> str:
        lines = [
            "# beta9 Benchmark Report",
            "",
            f"- Status: `{summary['status']}`",
            f"- Metrics: `{self.metrics_path}`",
            f"- Artifacts: `{self.artifact_dir}`",
            "",
        ]
        lines.extend(self._sandbox_startup_reports())
        lines.extend(
            [
                "| Suite | Scenario | Measurement | Size | MB/s | Status | Evidence |",
                "| --- | --- | --- | ---: | ---: | --- | --- |",
            ]
        )
        for measurement in self.measurements:
            evidence_bits = self._evidence_bits(measurement.evidence)
            size = measurement.tags.get("size_mib") or (
                round(measurement.bytes / 1024 / 1024, 2) if measurement.bytes else ""
            )
            lines.append(
                "| "
                f"{measurement.suite} | {measurement.scenario} | {measurement.measurement} | "
                f"{size} | {measurement.mbps:.2f} | `{measurement.status}` | "
                f"{evidence_bits} |"
            )
        lines.extend(["", "## Failures", ""])
        if summary["failures"]:
            lines.extend(f"- {failure}" for failure in summary["failures"])
        else:
            lines.append("- none")
        lines.append("")
        lines.extend(self._suite_rollup())
        return "\n".join(lines) + "\n"

    def _sandbox_startup_reports(self) -> list[str]:
        reports = [
            measurement
            for measurement in self.measurements
            if measurement.evidence.get("startup_report")
        ]
        if not reports:
            return []

        lines = ["## Sandbox Startup", ""]
        for measurement in reports:
            bottleneck = measurement.evidence.get("primary_bottleneck") or {}
            tti = measurement.evidence.get("time_to_interactive") or {}
            coverage = measurement.evidence.get("event_coverage") or {}
            lines.append(f"- Report: `{measurement.evidence['startup_report']}`")
            lines.append(f"- Verdict: {measurement.evidence.get('startup_verdict') or 'unavailable'}")
            lines.append(
                "- Time to interactive: "
                f"p50={self._format_ms(tti.get('p50'))}ms "
                f"p95={self._format_ms(tti.get('p95'))}ms "
                f"max={self._format_ms(tti.get('max'))}ms"
            )
            if bottleneck:
                lines.append(
                    "- Primary bottleneck: "
                    f"`{bottleneck.get('eventId')}` "
                    f"p95={self._format_ms(bottleneck.get('p95Ms'))}ms "
                    f"count={bottleneck.get('count', 0)}"
                )
            else:
                lines.append("- Primary bottleneck: unavailable")
            lines.append(
                "- Event coverage: "
                f"{coverage.get('containersWithEvents', 0)}/{coverage.get('requestedContainers', 0)} containers, "
                "required lifecycle spans "
                f"{coverage.get('requiredLifecyclePresent', 0)}/{coverage.get('requiredLifecycleTotal', 0)}"
            )
            lines.append("")
        return lines

    def _suite_rollup(self) -> list[str]:
        grouped: dict[tuple[str, str], list[Measurement]] = defaultdict(list)
        for measurement in self.measurements:
            grouped[(measurement.suite, measurement.measurement)].append(measurement)
        if not grouped:
            return []
        lines = ["## Rollup", ""]
        lines.append("| Suite | Measurement | Count | Best MB/s | Worst MB/s |")
        lines.append("| --- | --- | ---: | ---: | ---: |")
        for (suite, name), values in sorted(grouped.items()):
            mbps = [item.mbps for item in values if item.mbps]
            lines.append(
                f"| {suite} | {name} | {len(values)} | "
                f"{(max(mbps) if mbps else 0):.2f} | {(min(mbps) if mbps else 0):.2f} |"
            )
        lines.append("")
        return lines

    @staticmethod
    def _evidence_bits(evidence: dict) -> str:
        bits = []
        for key in (
            "sha_ok",
            "cache_hit",
            "cache_source",
            "remote_worker",
            "cloud_read",
            "artifact_output",
        ):
            if key in evidence:
                bits.append(f"{key}={evidence[key]}")
        return "`" + " ".join(bits) + "`" if bits else ""

    @staticmethod
    def _format_ms(value) -> str:
        if value is None:
            return "-"
        try:
            return f"{float(value):.1f}"
        except (TypeError, ValueError):
            return "-"
