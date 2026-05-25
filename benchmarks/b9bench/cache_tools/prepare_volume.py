import argparse
import json
import os
import time
from pathlib import Path

FSYNC_FILE_THRESHOLD = 1024 * 1024 * 1024

try:
    deterministic_payload_range
except NameError:  # pragma: no cover - used when this helper runs standalone.
    from payload import deterministic_payload_range


def fsync_path(path):
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_generated_file(path, nonce, label, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    written = 0
    fsynced = size <= FSYNC_FILE_THRESHOLD
    with path.open("wb", buffering=0) as handle:
        while written < size:
            length = min(4 * 1024 * 1024, size - written)
            handle.write(deterministic_payload_range(nonce, label, written, length))
            written += length
        handle.flush()
        if fsynced:
            os.fsync(handle.fileno())
    return {
        "writeMs": (time.monotonic_ns() - started) / 1_000_000,
        "fsynced": fsynced,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dest", required=True)
    parser.add_argument("--manifest-json", required=True)
    args = parser.parse_args()

    root = Path(args.dest)
    manifest = json.loads(args.manifest_json)
    started = time.monotonic_ns()
    bytes_written = 0
    files = []
    for entry in manifest["files"]:
        path = root / entry["path"]
        write_result = write_generated_file(
            path, manifest["nonce"], entry["label"], int(entry["size"])
        )
        bytes_written += int(entry["size"])
        files.append(
            {
                "path": entry["path"],
                "bytes": int(entry["size"]),
                **write_result,
            }
        )

    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
    if all(file["fsynced"] for file in files):
        fsync_path(root)
    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    print(
        json.dumps(
            {
                "ok": True,
                "files": len(files),
                "bytes": bytes_written,
                "durationMs": duration_ms,
                "mbps": (bytes_written / 1048576) / (duration_ms / 1000)
                if duration_ms > 0
                else 0,
                "manifest": manifest,
                "writes": files,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
