from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from typing import Any

from .model import Measurement, RunConfig, ScenarioSpec, SuiteSpec, slug, utc_now
from .reports import MetricSink
from .suites import SuiteLoader, merge_args
from .validators import Validator


class BenchmarkRunner:
    def __init__(self, config: RunConfig) -> None:
        self.config = config
        self.loader = SuiteLoader(config.root)
        self.run_id = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{slug(config.profile)}"
        self.sink = MetricSink(config.out_dir)
        self.validator = Validator()

    def run(self) -> int:
        self.sink.open()
        suite = self.loader.load(self.config.suite_name)
        self._run_suite(suite)
        failures = self.validator.validate(self.sink.measurements)
        self.sink.write_summary(failures)
        print(f"metrics: {self.sink.metrics_path}")
        print(f"summary: {self.sink.summary_path}")
        print(f"report:  {self.sink.markdown_path}")
        return 1 if failures else 0

    def _run_suite(self, suite: SuiteSpec) -> None:
        if suite.includes:
            for include in suite.includes:
                self._run_suite(self.loader.load(include))
            return

        if self.config.dry_run:
            self._emit_dry_run(suite)
            return

        runner = suite.runner
        if runner == "cache":
            CacheSuiteProbe(self.config, self.run_id, self.sink).run(suite)
        elif runner == "startup":
            ScriptSuiteProbe(
                self.config, self.run_id, self.sink, "startup", "benchmarks/startup.py"
            ).run(suite)
        elif runner == "sandbox":
            ScriptSuiteProbe(
                self.config,
                self.run_id,
                self.sink,
                "sandbox",
                "benchmarks/sandbox_parallel.py",
            ).run(suite)
        else:
            raise SystemExit(f"suite {suite.name} uses unsupported runner {runner!r}")

    def _emit_dry_run(self, suite: SuiteSpec) -> None:
        scenarios = suite.scenarios or (ScenarioSpec(name=suite.name),)
        for scenario in scenarios:
            self.sink.emit(
                Measurement(
                    run_id=self.run_id,
                    suite=suite.name,
                    scenario=scenario.name,
                    measurement="dry_run",
                    timestamp=utc_now(),
                    status="ok",
                    tags=scenario.metric_tags,
                    evidence={
                        "runner": suite.runner,
                        "suite_kind": suite.kind,
                        "description": suite.description,
                    },
                )
            )


class ScriptProbeBase:
    def __init__(self, config: RunConfig, run_id: str, sink: MetricSink) -> None:
        self.config = config
        self.run_id = run_id
        self.sink = sink

    def _common_args(self) -> dict[str, Any]:
        return {
            "namespace": self.config.namespace,
            "gateway_url": self.config.gateway_url,
            "grpc_addr": self.config.grpc_addr,
            "token": self.config.token,
        }

    def _run_python_script(
        self, suite: SuiteSpec, script: str, args: dict[str, Any]
    ) -> subprocess.CompletedProcess[str]:
        cmd = [sys.executable, str(self.config.root / script)]
        cmd.extend(self._args_to_cli(args))
        cmd.extend(self.config.passthrough_args)

        log_path = self.sink.artifact_dir / f"{slug(suite.name)}.log"
        start = time.perf_counter()
        proc = subprocess.run(
            cmd,
            cwd=self.config.root,
            text=True,
            capture_output=True,
            timeout=float(args.get("timeout_seconds", 2400)) + 120,
            env=self._env(),
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        log_path.write_text(
            "$ " + " ".join(cmd) + "\n\n"
            + "## stdout\n"
            + proc.stdout
            + "\n## stderr\n"
            + proc.stderr,
            encoding="utf-8",
        )
        if proc.returncode != 0:
            self.sink.emit(
                Measurement(
                    run_id=self.run_id,
                    suite=suite.name,
                    scenario=suite.name,
                    measurement="script",
                    timestamp=utc_now(),
                    duration_ms=elapsed_ms,
                    status="failed",
                    error=(proc.stderr or proc.stdout)[-1000:],
                    evidence={"log": str(log_path), "script": script},
                )
            )
        return proc

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        python_path = [
            str(self.config.root),
            str(self.config.root / "benchmarks"),
            str(self.config.root / "sdk" / "src"),
        ]
        if env.get("PYTHONPATH"):
            python_path.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = ":".join(python_path)
        return env

    @staticmethod
    def _args_to_cli(args: dict[str, Any]) -> list[str]:
        out: list[str] = []
        for key, value in args.items():
            if value is None:
                continue
            name = "--" + key.replace("_", "-")
            if isinstance(value, bool):
                out.append(name if value else "--no-" + key.replace("_", "-"))
            else:
                out.extend([name, str(value)])
        return out


class CacheSuiteProbe(ScriptProbeBase):
    def run(self, suite: SuiteSpec) -> None:
        suite_json = self.sink.artifact_dir / f"{slug(suite.name)}.json"
        suite_md = self.sink.artifact_dir / f"{slug(suite.name)}.md"
        args = merge_args(
            self._common_args(),
            suite.defaults,
            suite.args,
            self.config.extra_args,
            {"output": str(suite_json), "report": str(suite_md)},
        )
        if not args.get("file_plan"):
            file_plan = suite.file_plan
            if file_plan:
                args["file_plan"] = file_plan

        self._run_python_script(suite, "benchmarks/b9bench/cache_suite.py", args)
        if suite_json.exists():
            report = json.loads(suite_json.read_text(encoding="utf-8"))
            self._emit_report_metrics(suite, report)

    def _emit_report_metrics(self, suite: SuiteSpec, report: dict[str, Any]) -> None:
        scenario_by_key = {
            (scenario.access, scenario.pattern, scenario.size_mib): scenario
            for scenario in suite.scenarios
        }
        direct = report.get("directDiskProbe") or {}
        if direct:
            self.sink.emit(
                Measurement(
                    run_id=self.run_id,
                    suite=suite.name,
                    scenario="cache-disk",
                    measurement="direct_cache_disk_read",
                    timestamp=utc_now(),
                    duration_ms=float(direct.get("durationMs") or 0),
                    bytes=int(direct.get("bytes") or direct.get("size") or 0),
                    mbps=float(direct.get("mbps") or 0),
                    status="ok" if direct.get("ok") else "failed",
                    error=str(direct.get("error") or ""),
                    evidence={
                        "cache_source": "embedded_disk",
                        "artifact_output": str(self.sink.artifact_dir / f"{slug(suite.name)}.json"),
                    },
                )
            )
        for row in report.get("rows", []):
            key = (
                row.get("accessType", ""),
                row.get("pattern", "sequential"),
                int(row.get("sizeMiB", 0) or 0),
            )
            scenario = scenario_by_key.get(key) or ScenarioSpec(
                name=f"{key[0]}-{key[1]}-{key[2]}mib",
                access=key[0],
                pattern=key[1],
                size_mib=key[2],
                cache_state="hot",
            )
            hot_read_name = (
                "worker_hot_read"
                if row.get("accessType") == "workspace_fuse"
                else "sandbox_hot_read"
            )
            self._emit_row_metric(suite, scenario, row, "hotRead", hot_read_name)
            self._emit_row_metric(suite, scenario, row, "workerDD", "worker_pod_dd")
            self._emit_row_metric(suite, scenario, row, "sandboxDD", "sandbox_dd")
            self._emit_row_metric(
                suite,
                scenario,
                row,
                "remoteRead",
                "remote_cache_socket_read",
                requires_remote=True,
            )
            hot = row.get("hotRead") or {}
            if hot.get("fileReadMBps"):
                self.sink.emit(
                    self._measurement(
                        suite,
                        scenario,
                        row,
                        "python_file_read",
                        mbps=float(hot.get("fileReadMBps") or 0),
                        duration_ms=float(hot.get("timing", {}).get("fileReadMs") or 0),
                    )
                )
        for failure in report.get("validationFailures", []):
            self.sink.emit(
                Measurement(
                    run_id=self.run_id,
                    suite=suite.name,
                    scenario=suite.name,
                    measurement="suite_validation",
                    timestamp=utc_now(),
                    status="failed",
                    error=str(failure),
                    evidence={"artifact_output": str(self.sink.artifact_dir)},
                )
            )

    def _emit_row_metric(
        self,
        suite: SuiteSpec,
        scenario: ScenarioSpec,
        row: dict[str, Any],
        source_key: str,
        name: str,
        requires_remote: bool = False,
    ) -> None:
        payload = row.get(source_key)
        if not payload:
            return
        if (
            source_key == "workerDD"
            and payload.get("ok") is False
            and "no worker dd probe succeeded" in str(payload.get("error") or "")
        ):
            return
        self.sink.emit(
            self._measurement(
                suite,
                scenario,
                row,
                name,
                mbps=float(payload.get("mbps") or payload.get("fileReadMBps") or 0),
                duration_ms=float(payload.get("elapsedMs") or payload.get("durationMs") or 0),
                status="ok" if payload.get("ok", True) else "failed",
                error=str(payload.get("error") or ""),
                extra_evidence={
                    "remote_worker": bool(payload.get("differentWorkerPod"))
                    if requires_remote
                    else None,
                    "source_worker": payload.get("sourcePod"),
                    "target_worker": payload.get("targetPod"),
                },
                requires_remote=requires_remote,
            )
        )

    def _measurement(
        self,
        suite: SuiteSpec,
        scenario: ScenarioSpec,
        row: dict[str, Any],
        name: str,
        mbps: float,
        duration_ms: float = 0.0,
        status: str = "ok",
        error: str = "",
        extra_evidence: dict[str, Any] | None = None,
        requires_remote: bool = False,
    ) -> Measurement:
        read_path = row.get("readPathProof") or {}
        summary = read_path.get("geesefsSummary") or {}
        cloud_req = int(summary.get("cloudReq") or 0)
        external_page_hits = int(read_path.get("externalPageHitLines") or 0)
        cache_hit = bool(
            summary.get("mmapHits", 0)
            or summary.get("readIntoHits", 0)
            or external_page_hits
        )
        evidence = {
            "sha_ok": bool(row.get("shaOK")),
            "cache_hit": cache_hit,
            "cache_source": "embedded_disk" if cache_hit else "unknown",
            "cloud_read": cloud_req > 0,
            "external_page_hits": external_page_hits,
            "hash": row.get("hash"),
            "artifact_output": str(self.sink.artifact_dir / f"{slug(suite.name)}.json"),
        }
        if extra_evidence:
            evidence.update({key: value for key, value in extra_evidence.items() if value is not None})
        tags = scenario.metric_tags
        tags.update(
            {
                "requires_sha": True,
                "requires_cache_hit": name
                in {"sandbox_hot_read", "worker_hot_read", "python_file_read"}
                and scenario.cache_state in {"hot", "strict_disk"},
                "reject_cloud_read": scenario.cache_state in {"hot", "strict_disk"},
                "requires_remote_worker": requires_remote,
                "min_mbps": scenario.thresholds.get(f"{name}_min_mbps"),
            }
        )
        return Measurement(
            run_id=self.run_id,
            suite=suite.name,
            scenario=scenario.name,
            measurement=name,
            timestamp=utc_now(),
            duration_ms=duration_ms,
            bytes=int(row.get("sizeMiB", 0) or 0) * 1024 * 1024,
            mbps=mbps,
            status=status,
            tags=tags,
            evidence=evidence,
            error=error,
        )


class ScriptSuiteProbe(ScriptProbeBase):
    def __init__(
        self,
        config: RunConfig,
        run_id: str,
        sink: MetricSink,
        measurement_name: str,
        script: str,
    ) -> None:
        super().__init__(config, run_id, sink)
        self.measurement_name = measurement_name
        self.script = script

    def run(self, suite: SuiteSpec) -> None:
        output = self.sink.artifact_dir / f"{slug(suite.name)}.json"
        args = merge_args(
            self._common_args(),
            suite.defaults,
            suite.args,
            self.config.extra_args,
            {"output": str(output)},
        )
        start = time.perf_counter()
        proc = self._run_python_script(suite, self.script, args)
        elapsed_ms = (time.perf_counter() - start) * 1000
        evidence: dict[str, Any] = {"artifact_output": str(output), "script": self.script}
        if output.exists():
            try:
                payload = json.loads(output.read_text(encoding="utf-8"))
                evidence["summary"] = payload.get("summary") or payload.get("aggregate") or {}
            except json.JSONDecodeError as exc:
                evidence["parse_error"] = str(exc)
        self.sink.emit(
            Measurement(
                run_id=self.run_id,
                suite=suite.name,
                scenario=suite.name,
                measurement=self.measurement_name,
                timestamp=utc_now(),
                duration_ms=elapsed_ms,
                status="ok" if proc.returncode == 0 else "failed",
                error=(proc.stderr or proc.stdout)[-1000:] if proc.returncode != 0 else "",
                tags={"suite_kind": suite.kind},
                evidence=evidence,
            )
        )
