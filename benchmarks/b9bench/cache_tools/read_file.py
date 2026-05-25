import argparse
import hashlib
import json
import time
from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024


def manifest_entry(root, rel_path):
    manifest = json.loads((root / "manifest.json").read_text())
    for entry in manifest["files"]:
        if entry["path"] == rel_path:
            return entry
    raise SystemExit(f"file {rel_path!r} not found in manifest")


def read_sequential(path, expected, verify, read_method):
    hasher = hashlib.sha256() if verify else None
    size = 0
    read_ns = 0
    verify_ns = 0
    open_started = time.monotonic_ns()
    with path.open("rb") as handle:
        open_ns = time.monotonic_ns() - open_started
        if read_method == "readinto":
            buf = bytearray(CHUNK_SIZE)
            view = memoryview(buf)
            while True:
                read_started = time.monotonic_ns()
                n = handle.readinto(buf)
                read_ns += time.monotonic_ns() - read_started
                if not n:
                    break
                if hasher is not None:
                    verify_started = time.monotonic_ns()
                    hasher.update(view[:n])
                    verify_ns += time.monotonic_ns() - verify_started
                size += n
        else:
            while True:
                read_started = time.monotonic_ns()
                chunk = handle.read(CHUNK_SIZE)
                read_ns += time.monotonic_ns() - read_started
                if not chunk:
                    break
                if hasher is not None:
                    verify_started = time.monotonic_ns()
                    hasher.update(chunk)
                    verify_ns += time.monotonic_ns() - verify_started
                size += len(chunk)
    digest = hasher.hexdigest() if hasher is not None else ""
    if size != int(expected["size"]) or (verify and digest != expected["sha256"]):
        raise SystemExit(f"sequential mismatch size={size} sha256={digest}")
    return (
        size,
        digest,
        {
            "openMs": open_ns / 1_000_000,
            "fileReadMs": read_ns / 1_000_000,
            "verifyMs": verify_ns / 1_000_000,
            "readMethod": read_method,
        },
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--expected-size", type=int, default=0)
    parser.add_argument("--expected-sha256", default="")
    parser.add_argument(
        "--read-method", choices=("read", "readinto"), default="readinto"
    )
    parser.add_argument("--skip-verify", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    if args.expected_size > 0 and args.expected_sha256:
        entry = {
            "path": args.file,
            "size": args.expected_size,
            "sha256": args.expected_sha256,
        }
    else:
        entry = manifest_entry(root, args.file)
    started = time.monotonic_ns()
    bytes_read, digest, timing = read_sequential(
        root / args.file, entry, not args.skip_verify, args.read_method
    )
    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    file_read_ms = timing["fileReadMs"]
    print(
        json.dumps(
            {
                "ok": True,
                "path": args.file,
                "bytes": bytes_read,
                "digest": digest,
                "expectedSha256": entry["sha256"],
                "verified": not args.skip_verify,
                "durationMs": duration_ms,
                "mbps": (bytes_read / 1048576) / (duration_ms / 1000)
                if duration_ms > 0
                else 0,
                "fileReadMBps": (bytes_read / 1048576) / (file_read_ms / 1000)
                if file_read_ms > 0
                else 0,
                "timing": timing,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
