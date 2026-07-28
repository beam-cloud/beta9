from __future__ import annotations

import json
import os
import shutil
from pathlib import Path


def main() -> None:
    workspace = Path(os.environ.get("WORKSPACE", "/workspace"))
    destination = Path(os.environ.get("ARTIFACT_ROOT", "/artifacts"))
    result = json.loads((workspace / "result.json").read_text())
    if result.get("status") != "complete":
        raise RuntimeError(f"Training result is not complete: {result.get('status')}")

    destination.mkdir(parents=True, exist_ok=True)
    for filename in ("result.json", "ai-toolkit.log"):
        shutil.copy2(workspace / filename, destination / filename)

    run_dir = Path(result["artifacts"]["run_dir"])
    exported_run = destination / "runs" / run_dir.name
    shutil.rmtree(exported_run, ignore_errors=True)
    shutil.copytree(run_dir, exported_run)

    exported_config = destination / "config"
    shutil.rmtree(exported_config, ignore_errors=True)
    shutil.copytree(workspace / "config", exported_config)

    exported_dataset = destination / "dataset"
    exported_dataset.mkdir(parents=True, exist_ok=True)
    shutil.copy2(workspace / "dataset" / "sources.json", exported_dataset / "sources.json")

    print(
        json.dumps(
            {
                "status": "complete",
                "source": str(workspace),
                "destination": str(destination),
                "models": len(result["artifacts"]["models"]),
                "samples": len(result["artifacts"]["samples"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
