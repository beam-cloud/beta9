import argparse
import hashlib
import json
import signal
import sys
import time
from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024


def manifest_entry(root, rel_path):
    manifest = json.loads((root / "manifest.json").read_text())
    for entry in manifest["files"]:
        if entry["path"] == rel_path:
            return entry
    raise SystemExit(f"file {rel_path!r} not found in manifest")


class ReadStallTimeout(TimeoutError):
    pass


def _set_read_alarm(seconds):
    if seconds <= 0 or not hasattr(signal, "setitimer"):
        return
    signal.setitimer(signal.ITIMER_REAL, seconds)


def _clear_read_alarm():
    if hasattr(signal, "setitimer"):
        signal.setitimer(signal.ITIMER_REAL, 0)


def _install_read_alarm_handler():
    if not hasattr(signal, "SIGALRM"):
        return

    def _handle_timeout(signum, frame):
        raise ReadStallTimeout("read stalled")

    signal.signal(signal.SIGALRM, _handle_timeout)


def maybe_progress(path, size, started_ns, last_progress_ns, interval_seconds):
    if interval_seconds <= 0:
        return last_progress_ns
    now = time.monotonic_ns()
    if last_progress_ns and now - last_progress_ns < interval_seconds * 1_000_000_000:
        return last_progress_ns
    elapsed_ms = (now - started_ns) / 1_000_000
    mbps = (size / 1048576) / (elapsed_ms / 1000) if elapsed_ms > 0 else 0
    print(
        json.dumps(
            {
                "event": "read_progress",
                "path": str(path),
                "bytes": size,
                "elapsedMs": elapsed_ms,
                "mbps": mbps,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
        flush=True,
    )
    return now


def read_sequential(path, expected, verify, read_method, progress_interval_seconds, read_stall_timeout_seconds):
    hasher = hashlib.sha256() if verify else None
    size = 0
    read_ns = 0
    verify_ns = 0
    started_ns = time.monotonic_ns()
    last_progress_ns = 0
    open_started = time.monotonic_ns()
    with path.open("rb") as handle:
        open_ns = time.monotonic_ns() - open_started
        if read_method == "readinto":
            buf = bytearray(CHUNK_SIZE)
            view = memoryview(buf)
            while True:
                read_started = time.monotonic_ns()
                try:
                    _set_read_alarm(read_stall_timeout_seconds)
                    n = handle.readinto(buf)
                except ReadStallTimeout as exc:
                    raise SystemExit(
                        f"{exc}: path={path} offset={size} chunk_size={len(buf)} "
                        f"timeout_seconds={read_stall_timeout_seconds}"
                    )
                finally:
                    _clear_read_alarm()
                    read_ns += time.monotonic_ns() - read_started
                if not n:
                    break
                if hasher is not None:
                    verify_started = time.monotonic_ns()
                    hasher.update(view[:n])
                    verify_ns += time.monotonic_ns() - verify_started
                size += n
                last_progress_ns = maybe_progress(
                    path, size, started_ns, last_progress_ns, progress_interval_seconds
                )
        else:
            while True:
                read_started = time.monotonic_ns()
                try:
                    _set_read_alarm(read_stall_timeout_seconds)
                    chunk = handle.read(CHUNK_SIZE)
                except ReadStallTimeout as exc:
                    raise SystemExit(
                        f"{exc}: path={path} offset={size} chunk_size={CHUNK_SIZE} "
                        f"timeout_seconds={read_stall_timeout_seconds}"
                    )
                finally:
                    _clear_read_alarm()
                    read_ns += time.monotonic_ns() - read_started
                if not chunk:
                    break
                if hasher is not None:
                    verify_started = time.monotonic_ns()
                    hasher.update(chunk)
                    verify_ns += time.monotonic_ns() - verify_started
                size += len(chunk)
                last_progress_ns = maybe_progress(
                    path, size, started_ns, last_progress_ns, progress_interval_seconds
                )
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
    parser.add_argument("--progress-interval-seconds", type=float, default=10)
    parser.add_argument("--read-stall-timeout-seconds", type=float, default=120)
    args = parser.parse_args()
    _install_read_alarm_handler()

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
        root / args.file,
        entry,
        not args.skip_verify,
        args.read_method,
        args.progress_interval_seconds,
        args.read_stall_timeout_seconds,
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
