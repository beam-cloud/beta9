#!/usr/bin/env python3
"""Archive only this module graph's cached private dependencies, never credentials."""

import argparse
import json
from pathlib import Path
import subprocess
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    cache = Path(subprocess.check_output(["go", "env", "GOMODCACHE"], cwd=root, text=True).strip())
    graph = subprocess.check_output(["go", "list", "-mod=readonly", "-m", "-json", "all"], cwd=root, text=True)
    decoder = json.JSONDecoder()
    files = set()
    while graph.strip():
        module, length = decoder.raw_decode(graph.lstrip())
        graph = graph.lstrip()[length:]
        module = module.get("Replace", module)
        if not module["Path"].startswith("github.com/beam-cloud/") or "Version" not in module:
            continue
        escaped_path = "".join("!" + ch.lower() if ch.isupper() else ch for ch in module["Path"])
        prefix = cache / "cache/download" / escaped_path / "@v"
        for suffix in ("info", "mod", "zip", "ziphash"):
            path = prefix / f"{module['Version']}.{suffix}"
            if not path.is_file():
                raise SystemExit(f"Missing cached dependency {module['Path']}@{module['Version']}; run go mod download locally first.")
            files.add(path)
    with tarfile.open(args.output, "w") as archive:
        for path in sorted(files):
            archive.add(path, arcname=path.relative_to(cache))
    print(f"Archived {len(files)} private module cache files in {args.output}; no credentials included.")


if __name__ == "__main__":
    main()
