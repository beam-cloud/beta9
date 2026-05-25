from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def slug(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in text).strip("-")


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    access: str = ""
    operation: str = "read"
    pattern: str = "sequential"
    size_mib: int = 0
    cache_state: str = ""
    probes: tuple[str, ...] = ()
    args: dict[str, Any] = field(default_factory=dict)
    thresholds: dict[str, Any] = field(default_factory=dict)
    tags: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScenarioSpec":
        return cls(
            name=str(data["name"]),
            access=str(data.get("access", "")),
            operation=str(data.get("operation", "read")),
            pattern=str(data.get("pattern", "sequential")),
            size_mib=int(data.get("size_mib", 0) or 0),
            cache_state=str(data.get("cache_state", "")),
            probes=tuple(str(probe) for probe in data.get("probes", ())),
            args=dict(data.get("args") or {}),
            thresholds=dict(data.get("thresholds") or {}),
            tags=dict(data.get("tags") or {}),
        )

    @property
    def file_plan_entry(self) -> str:
        if not self.access or not self.pattern or not self.size_mib:
            return ""
        return f"{self.access}:{self.pattern}:{self.size_mib}"

    @property
    def metric_tags(self) -> dict[str, Any]:
        tags = dict(self.tags)
        if self.access:
            tags["access"] = self.access
        if self.operation:
            tags["operation"] = self.operation
        if self.pattern:
            tags["pattern"] = self.pattern
        if self.size_mib:
            tags["size_mib"] = self.size_mib
        if self.cache_state:
            tags["cache_state"] = self.cache_state
        return tags


@dataclass(frozen=True)
class SuiteSpec:
    name: str
    kind: str
    runner: str
    description: str = ""
    includes: tuple[str, ...] = ()
    defaults: dict[str, Any] = field(default_factory=dict)
    args: dict[str, Any] = field(default_factory=dict)
    scenarios: tuple[ScenarioSpec, ...] = ()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SuiteSpec":
        name = str(data["name"])
        kind = str(data.get("kind") or name.split("-", 1)[0])
        return cls(
            name=name,
            kind=kind,
            runner=str(data.get("runner") or kind),
            description=str(data.get("description", "")),
            includes=tuple(str(item) for item in data.get("includes", ())),
            defaults=dict(data.get("defaults") or {}),
            args=dict(data.get("args") or {}),
            scenarios=tuple(
                ScenarioSpec.from_dict(item) for item in data.get("scenarios", ())
            ),
        )

    @property
    def file_plan(self) -> str:
        entries = [scenario.file_plan_entry for scenario in self.scenarios]
        entries = [entry for entry in entries if entry]
        return ",".join(entries)


@dataclass
class RunConfig:
    root: Path
    command: str
    suite_name: str
    profile: str
    config_path: Path
    namespace: str
    gateway_url: str
    grpc_addr: str
    token: str
    out_dir: Path
    dry_run: bool
    extra_args: dict[str, Any] = field(default_factory=dict)
    passthrough_args: tuple[str, ...] = ()


@dataclass
class Measurement:
    run_id: str
    suite: str
    scenario: str
    measurement: str
    timestamp: str
    duration_ms: float = 0.0
    bytes: int = 0
    mbps: float = 0.0
    status: str = "ok"
    tags: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    error: str = ""

    def as_dict(self) -> dict[str, Any]:
        data = {
            "run_id": self.run_id,
            "suite": self.suite,
            "scenario": self.scenario,
            "measurement": self.measurement,
            "timestamp": self.timestamp,
            "duration_ms": self.duration_ms,
            "bytes": self.bytes,
            "mbps": self.mbps,
            "status": self.status,
            "tags": self.tags,
            "evidence": self.evidence,
        }
        if self.error:
            data["error"] = self.error
        return data

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True)
