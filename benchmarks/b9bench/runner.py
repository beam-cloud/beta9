from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
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
        elif runner == "clip_layer":
            ClipLayerSuiteProbe(self.config, self.run_id, self.sink).run(suite)
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
        env = self._env()
        env["PYTHONUNBUFFERED"] = "1"
        popen_kwargs: dict[str, Any] = {}
        if sys.platform == "darwin":
            popen_kwargs["close_fds"] = False
        proc = subprocess.Popen(
            cmd,
            cwd=self.config.root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            env=env,
            **popen_kwargs,
        )
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []

        def stream(pipe, parts: list[str], dest) -> None:
            if pipe is None:
                return
            try:
                for line in pipe:
                    parts.append(line)
                    print(line, end="", file=dest, flush=True)
            finally:
                pipe.close()

        stdout_thread = threading.Thread(
            target=stream, args=(proc.stdout, stdout_parts, sys.stdout), daemon=True
        )
        stderr_thread = threading.Thread(
            target=stream, args=(proc.stderr, stderr_parts, sys.stderr), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()
        timeout = float(args.get("timeout_seconds", 2400)) + 120
        timed_out = False
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            proc.kill()
            returncode = proc.wait()
            stderr_parts.append(f"\nscript timed out after {timeout:.0f}s\n")
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        elapsed_ms = (time.perf_counter() - start) * 1000
        stdout = "".join(stdout_parts)
        stderr = "".join(stderr_parts)
        completed = subprocess.CompletedProcess(
            cmd,
            returncode,
            stdout=stdout,
            stderr=stderr,
        )
        log_path.write_text(
            "$ " + " ".join(cmd) + "\n\n"
            + "## stdout\n"
            + stdout
            + "\n## stderr\n"
            + stderr,
            encoding="utf-8",
        )
        if timed_out or completed.returncode != 0:
            self.sink.emit(
                Measurement(
                    run_id=self.run_id,
                    suite=suite.name,
                    scenario=suite.name,
                    measurement="script",
                    timestamp=utc_now(),
                    duration_ms=elapsed_ms,
                    status="failed",
                    error=(completed.stderr or completed.stdout)[-1000:],
                    evidence={
                        "log": str(log_path),
                        "script": script,
                        "timed_out": timed_out,
                    },
                )
            )
        return completed

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
            {
                "profile": self.config.profile,
                "beta9_config": str(self.config.config_path),
                "output": str(suite_json),
                "report": str(suite_md),
            },
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
            remote = row.get("remoteRead") or {}
            network = remote.get("networkProbe") or {}
            if network:
                self.sink.emit(
                    self._measurement(
                        suite,
                        scenario,
                        row,
                        "remote_network_read",
                        mbps=float(network.get("mbps") or 0),
                        duration_ms=float(network.get("durationMs") or 0),
                        status="ok" if network.get("ok") else "failed",
                        error=str(network.get("error") or ""),
                        extra_evidence={
                            "source_worker": network.get("sourcePod"),
                            "source_node": network.get("sourceNode"),
                            "target_worker": network.get("targetPod"),
                            "target_node": network.get("targetNode"),
                        },
                        source_key="remoteNetwork",
                    )
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
                    "remote_node": bool(payload.get("differentNode"))
                    if requires_remote
                    else None,
                    "source_worker": payload.get("sourcePod"),
                    "source_node": payload.get("sourceNode"),
                    "target_worker": payload.get("targetPod"),
                    "target_node": payload.get("targetNode"),
                },
                requires_remote=requires_remote,
                source_key=source_key,
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
        source_key: str = "",
    ) -> Measurement:
        read_path = (
            row.get("ddReadPathProof")
            if source_key in {"sandboxDD", "workerDD"} and row.get("ddReadPathProof")
            else row.get("readPathProof")
        ) or {}
        summary = read_path.get("geesefsSummary") or {}
        cache_summary = read_path.get("cacheSummary") or {}
        cloud_req = int(summary.get("cloudReq") or 0)
        external_page_hits = int(read_path.get("externalPageHitLines") or 0)
        raw_hits = int(cache_summary.get("clientRawHits") or 0)
        grpc_hits = int(cache_summary.get("clientGRPCHits") or 0)
        local_hits = int(cache_summary.get("clientLocalHits") or 0)
        embedded_disk_hit = bool(
            summary.get("mmapHits", 0)
            or summary.get("readIntoHits", 0)
            or external_page_hits
            or local_hits
        )
        buffer_hit = bool(summary.get("bufferHits", 0))
        cache_hit = embedded_disk_hit or buffer_hit or raw_hits > 0 or grpc_hits > 0
        cache_source = "unknown"
        if embedded_disk_hit:
            cache_source = "embedded_disk"
        elif raw_hits > 0:
            cache_source = "raw_remote"
        elif grpc_hits > 0:
            cache_source = "grpc_remote"
        elif buffer_hit:
            cache_source = "geesefs_buffer"
        elif cloud_req > 0:
            cache_source = "cloud"
        evidence = {
            "sha_ok": bool(row.get("shaOK")),
            "cache_hit": cache_hit,
            "cache_source": cache_source,
            "cloud_read": cloud_req > 0,
            "external_page_hits": external_page_hits,
            "buffer_hits": int(summary.get("bufferHits") or 0),
            "raw_remote_hits": raw_hits,
            "grpc_remote_hits": grpc_hits,
            "local_cache_hits": local_hits,
            "hash": row.get("hash"),
            "artifact_output": str(self.sink.artifact_dir / f"{slug(suite.name)}.json"),
        }
        hrw_route = (((row.get("hrwRoutingProof") or {}).get("routes") or []) + [{}])[0]
        if hrw_route:
            evidence.update(
                {
                    "hrw_ok": bool(hrw_route.get("ok")),
                    "hrw_selected_host": hrw_route.get("selectedHostId"),
                    "hrw_selected_registration": hrw_route.get("selectedRegistrationId"),
                    "hrw_selected_node": hrw_route.get("selectedNode"),
                    "hrw_pages_on_selected_node": hrw_route.get("pagesOnSelectedNode"),
                    "remote_target_matches_hrw": hrw_route.get("remoteTargetMatchesHRW"),
                    "remote_target_registration_matches_hrw": hrw_route.get("remoteTargetRegistrationMatchesHRW"),
                }
            )
        if extra_evidence:
            evidence.update({key: value for key, value in extra_evidence.items() if value is not None})
        if source_key == "remoteRead":
            remote = row.get("remoteRead") or {}
            evidence.update(
                {
                    "source_worker": remote.get("sourcePod"),
                    "source_node": remote.get("sourceNode"),
                    "target_worker": remote.get("targetPod"),
                    "target_node": remote.get("targetNode"),
                    "remote_worker": remote.get("differentWorkerPod"),
                    "remote_node": remote.get("differentNode"),
                }
            )
            network = (row.get("remoteRead") or {}).get("networkProbe") or {}
            if network.get("ok") and network.get("mbps"):
                evidence["network_ceiling_mbps"] = float(network.get("mbps") or 0)
        min_mbps = scenario.thresholds.get(f"{name}_min_mbps")
        if name in {"sandbox_hot_read", "worker_hot_read", "python_file_read"}:
            if "min_hot_file_read_mbps" in self.config.extra_args:
                min_mbps = self.config.extra_args["min_hot_file_read_mbps"]
        elif name == "remote_cache_socket_read":
            if "min_remote_cache_socket_mbps" in self.config.extra_args:
                min_mbps = self.config.extra_args["min_remote_cache_socket_mbps"]
        if min_mbps is not None and float(min_mbps) <= 0:
            min_mbps = None

        tags = scenario.metric_tags
        tags.update(
            {
                "requires_sha": True,
                "requires_cache_hit": name
                in {"sandbox_hot_read", "worker_hot_read", "python_file_read"}
                and scenario.cache_state in {"hot", "strict_disk"},
                "reject_cloud_read": scenario.cache_state in {"hot", "strict_disk"},
                "requires_remote_worker": requires_remote,
                "min_mbps": min_mbps,
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
            {"install": False},
            suite.defaults,
            suite.args,
            self.config.extra_args,
            {"output": str(output)},
        )
        start = time.perf_counter()
        proc = self._run_python_script(suite, self.script, args)
        elapsed_ms = (time.perf_counter() - start) * 1000
        effective_output = self._effective_output_path(output)
        evidence: dict[str, Any] = {"artifact_output": str(effective_output), "script": self.script}
        if effective_output.exists():
            try:
                payload = json.loads(effective_output.read_text(encoding="utf-8"))
                evidence["summary"] = payload.get("summary") or payload.get("aggregate") or {}
                startup_report = payload.get("startupReport") or {}
                if startup_report:
                    evidence["startup_report"] = startup_report.get("markdownPath")
                    evidence["startup_verdict"] = startup_report.get("verdict", {}).get("text")
                    evidence["time_to_interactive"] = startup_report.get("timeToInteractive")
                    evidence["primary_bottleneck"] = startup_report.get("primaryBottleneck")
                    evidence["event_coverage"] = startup_report.get("eventCoverage")
                    evidence["image_drilldown"] = startup_report.get("imageDrilldown")
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

    def _effective_output_path(self, default: os.PathLike[str] | str) -> Path:
        output = str(default)
        args = list(self.config.passthrough_args)
        index = 0
        while index < len(args):
            arg = args[index]
            if arg == "--output" and index + 1 < len(args):
                output = args[index + 1]
                index += 2
                continue
            if arg.startswith("--output="):
                output = arg.split("=", 1)[1]
            index += 1

        path = Path(output)
        if path.is_absolute():
            return path
        return self.config.root / path


class ClipLayerSuiteProbe(ScriptProbeBase):
    def run(self, suite: SuiteSpec) -> None:
        output = self.sink.artifact_dir / f"{slug(suite.name)}.json"
        args = merge_args(
            self._common_args(),
            suite.defaults,
            suite.args,
            self.config.extra_args,
            {
                "profile": self.config.profile,
                "beta9_config": str(self.config.config_path),
                "output": str(output),
            },
        )
        proc = self._run_python_script(
            suite, "benchmarks/b9bench/clip_layer_suite.py", args
        )
        if output.exists():
            report = json.loads(output.read_text(encoding="utf-8"))
            self._emit_report_metrics(suite, report, proc.returncode)

    def _emit_report_metrics(
        self, suite: SuiteSpec, report: dict[str, Any], returncode: int
    ) -> None:
        iterations = report.get("iterations") or []
        for item in iterations:
            read = item.get("read") or {}
            logs = item.get("logSummary") or {}
            page_proof = item.get("embeddedCachePageProof") or {}
            hrw_proof = item.get("hrwRoutingProof") or {}
            clip_disk = item.get("clipDiskCachePresence") or {}
            name = str(item.get("name") or "read")
            is_after_kill = name == "after_worker_kill"
            is_second_read = is_after_kill or name == "stable_second_read"
            cloud_read = logs.get("ociCacheMisses", 0) > 0 or logs.get("layerDecompressed", 0) > 0
            cache_hit = bool(page_proof.get("ok") and hrw_proof.get("ok") and not clip_disk.get("present") and not cloud_read)
            self.sink.emit(
                Measurement(
                    run_id=self.run_id,
                    suite=suite.name,
                    scenario=f"clip-layer-{name}",
                    measurement="clip_layer_read",
                    timestamp=utc_now(),
                    duration_ms=float(read.get("durationSeconds") or 0) * 1000,
                    bytes=int(read.get("size") or 0),
                    mbps=float(read.get("mbps") or 0),
                    status="ok" if read.get("ok") else "failed",
                    tags={
                        "suite_kind": suite.kind,
                        "requires_sha": True,
                        "requires_cache_hit": is_second_read,
                        "reject_cloud_read": is_second_read,
                        "requires_remote_worker": is_after_kill,
                        "cache_state": name if is_second_read else "cold",
                    },
                    evidence={
                        "sha_ok": bool(read.get("ok")),
                        "cache_hit": cache_hit,
                        "cache_source": "embedded_disk"
                        if page_proof.get("ok") and hrw_proof.get("ok")
                        else "unknown",
                        "hrw_routed": bool(hrw_proof.get("ok")),
                        "hrw_routes": hrw_proof.get("routes") or [],
                        "cloud_read": cloud_read,
                        "remote_worker": self._worker_changed(iterations)
                        if is_after_kill
                        else False,
                        "worker": ((item.get("worker") or {}).get("name") or ""),
                        "decompressed_hashes": logs.get("decompressedHashes")
                        or page_proof.get("hashes")
                        or report.get("decompressedHashes")
                        or [],
                        "artifact_output": str(
                            self.sink.artifact_dir / f"{slug(suite.name)}.json"
                        ),
                    },
                    error="" if read.get("ok") else str(read),
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
        if returncode != 0 and not report.get("validationFailures"):
            self.sink.emit(
                Measurement(
                    run_id=self.run_id,
                    suite=suite.name,
                    scenario=suite.name,
                    measurement="suite_validation",
                    timestamp=utc_now(),
                    status="failed",
                    error=str(report.get("error") or "clip layer suite failed"),
                    evidence={"artifact_output": str(self.sink.artifact_dir)},
                )
            )

    @staticmethod
    def _worker_changed(iterations: list[dict[str, Any]]) -> bool:
        if len(iterations) < 2:
            return False
        first = ((iterations[0].get("worker") or {}).get("name") or "")
        second = ((iterations[1].get("worker") or {}).get("name") or "")
        return bool(first and second and first != second)
