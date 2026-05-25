from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .model import SuiteSpec


class SuiteLoader:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.suite_dir = root / "benchmarks" / "b9bench" / "suite_defs"

    def load(self, name: str) -> SuiteSpec:
        path = self.resolve(name)
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise SystemExit(f"suite file {path} did not contain a mapping")
        return SuiteSpec.from_dict(data)

    def resolve(self, name: str) -> Path:
        raw = Path(name)
        candidates: list[Path] = []
        if raw.is_absolute() or raw.parent != Path("."):
            candidates.append(raw)
        else:
            candidates.append(self.suite_dir / raw)
        if not str(candidates[0]).endswith((".yaml", ".yml")):
            candidates.extend(Path(str(path) + ".yaml") for path in tuple(candidates))
            candidates.extend(Path(str(path) + ".yml") for path in tuple(candidates[:1]))
        for path in candidates:
            if path.exists():
                return path
        searched = ", ".join(str(path) for path in candidates)
        raise SystemExit(f"suite {name!r} not found; searched {searched}")


def merge_args(*mappings: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for mapping in mappings:
        out.update({key: value for key, value in mapping.items() if value is not None})
    return out
