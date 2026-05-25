import argparse
import json
import os
import subprocess
import time


def write_probe_file(directory, size):
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(
        directory, f".beta9-cache-disk-probe-{os.getpid()}-{int(time.time() * 1000)}"
    )
    chunk = b"\0" * min(8 * 1024 * 1024, size)
    remaining = size
    started = time.monotonic_ns()
    with open(path, "wb", buffering=0) as handle:
        while remaining > 0:
            n = min(len(chunk), remaining)
            handle.write(memoryview(chunk)[:n])
            remaining -= n
        handle.flush()
        os.fsync(handle.fileno())
        advise = getattr(os, "posix_fadvise", None)
        if advise is not None:
            try:
                advise(handle.fileno(), 0, 0, getattr(os, "POSIX_FADV_DONTNEED", 4))
            except OSError:
                pass
    return path, (time.monotonic_ns() - started) / 1_000_000


def run_dd(path, block_size):
    attempts = [
        [
            "dd",
            f"if={path}",
            "of=/dev/null",
            f"bs={block_size}",
            "iflag=direct,fullblock",
            "status=none",
        ],
        [
            "dd",
            f"if={path}",
            "of=/dev/null",
            f"bs={block_size}",
            "iflag=fullblock",
            "status=none",
        ],
    ]
    last = None
    for command in attempts:
        started = time.monotonic_ns()
        proc = subprocess.run(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        elapsed = (time.monotonic_ns() - started) / 1_000_000
        if proc.returncode == 0:
            return command, elapsed, proc
        last = proc
    raise SystemExit(
        (last.stderr or last.stdout or f"dd exited {last.returncode}")[-1000:]
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path")
    parser.add_argument("--expected-size", type=int)
    parser.add_argument("--probe-dir")
    parser.add_argument("--probe-size", type=int)
    parser.add_argument("--cleanup-probe", action="store_true")
    parser.add_argument("--block-size", default="4M")
    args = parser.parse_args()

    created_probe = False
    write_ms = None
    if args.probe_dir:
        args.path, write_ms = write_probe_file(args.probe_dir, args.probe_size)
        args.expected_size = args.probe_size
        created_probe = True
    if not args.path or args.expected_size is None:
        raise SystemExit(
            "either --path/--expected-size or --probe-dir/--probe-size is required"
        )

    actual = os.stat(args.path).st_size
    if actual != args.expected_size:
        raise SystemExit(
            f"size mismatch path={args.path} got={actual} want={args.expected_size}"
        )

    cleanup_ok = None
    try:
        command, duration_ms, proc = run_dd(args.path, args.block_size)
        mb = args.expected_size / 1048576
        result = {
            "ok": True,
            "path": args.path,
            "bytes": args.expected_size,
            "durationMs": duration_ms,
            "mbps": mb / (duration_ms / 1000) if duration_ms > 0 else 0,
            "blockSize": args.block_size,
            "directIO": any(flag.startswith("iflag=direct") for flag in command),
            "command": command,
            "writeDurationMs": write_ms,
            "writeMBps": mb / (write_ms / 1000) if write_ms and write_ms > 0 else None,
            "stdout": proc.stdout[-1000:],
            "stderr": proc.stderr[-1000:],
        }
    finally:
        if created_probe and args.cleanup_probe:
            try:
                os.unlink(args.path)
                cleanup_ok = True
            except OSError:
                cleanup_ok = False
    if cleanup_ok is not None:
        result["cleanupOk"] = cleanup_ok
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
