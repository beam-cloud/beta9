#!/usr/bin/env python3
import argparse
import atexit
import base64
import hashlib
import json
import os
import random
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from sandbox_parallel import (
    cleanup_sandbox,
    create_sandbox_with_retries,
    import_sdk,
    parse_sdk_cpu,
    parse_sdk_memory,
    wait_running,
    write_sdk_config,
)
from startup import (
    api_url,
    authorize,
    cluster_snapshot,
    delete_workers,
    env_bool,
    env_float,
    env_int,
    find_redis_pod,
    format_ms,
    gateway_host_is_local,
    grpc_addr,
    http_json,
    install_overlay,
    load_cached_token,
    log,
    redis_cli,
    require_tools,
    run,
    save_cached_token,
    start_grpc_port_forward_if_needed,
    start_http_port_forward_if_needed,
    wait_http,
)


HEALTH_PATH = "/api/v1/health"
DEFAULT_CACHE_MOUNT_PATH = "/var/lib/beta9/cache"
DEFAULT_IMAGE_CACHE_PATH = "/images/cache"
IMAGE_READ_ROOT = "/bench-cache"
VOLUME_CONTAINER_PREFIX = "/volumes"

DETERMINISTIC_PAYLOAD_SOURCE = r"""
DETERMINISTIC_BLOCK_SIZE = 4096


def deterministic_payload_range(nonce, label, offset, length):
    out = bytearray()
    position = offset
    while len(out) < length:
        block_index = position // DETERMINISTIC_BLOCK_SIZE
        seed = hashlib.sha256(f"{nonce}:{label}:{block_index}".encode()).digest()
        block = (seed * ((DETERMINISTIC_BLOCK_SIZE // len(seed)) + 1))[:DETERMINISTIC_BLOCK_SIZE]
        block_offset = position % DETERMINISTIC_BLOCK_SIZE
        take = min(length - len(out), len(block) - block_offset)
        out.extend(block[block_offset:block_offset + take])
        position += take
    return bytes(out)
"""

exec(DETERMINISTIC_PAYLOAD_SOURCE, globals())

READ_HARNESS = r"""
import argparse
import hashlib
import json
import random
import time
from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024
""" + DETERMINISTIC_PAYLOAD_SOURCE + r"""

def manifest_entry(root, rel_path):
    manifest = json.loads((root / "manifest.json").read_text())
    for entry in manifest["files"]:
        if entry["path"] == rel_path:
            return manifest, entry
    raise SystemExit(f"file {rel_path!r} not found in manifest")


def sequential(path, expected, verify):
    h = hashlib.sha256() if verify else None
    size = 0
    read_ns = 0
    verify_ns = 0
    open_started = time.monotonic_ns()
    with path.open("rb") as f:
        open_ns = time.monotonic_ns() - open_started
        while True:
            read_started = time.monotonic_ns()
            chunk = f.read(CHUNK_SIZE)
            read_ns += time.monotonic_ns() - read_started
            if not chunk:
                break
            if h is not None:
                verify_started = time.monotonic_ns()
                h.update(chunk)
                verify_ns += time.monotonic_ns() - verify_started
            size += len(chunk)
    digest = h.hexdigest() if h is not None else ""
    if size != expected["size"] or (verify and digest != expected["sha256"]):
        raise SystemExit(f"sequential mismatch size={size} sha256={digest}")
    return size, 1, digest, {
        "openMs": open_ns / 1_000_000,
        "fileReadMs": read_ns / 1_000_000,
        "verifyMs": verify_ns / 1_000_000,
    }


def random_reads(path, manifest, expected, block_size, total_bytes, seed, verify):
    total_bytes = total_bytes or expected["size"]
    rng = random.Random(seed)
    aggregate = hashlib.sha256()
    bytes_read = 0
    ops = 0
    read_ns = 0
    verify_ns = 0
    seek_ns = 0

    open_started = time.monotonic_ns()
    with path.open("rb") as f:
        open_ns = time.monotonic_ns() - open_started
        while bytes_read < total_bytes:
            length = min(block_size, total_bytes - bytes_read, expected["size"])
            max_offset = expected["size"] - length
            if max_offset <= 0:
                offset = 0
            else:
                max_block = max_offset // block_size
                offset = rng.randint(0, max_block) * block_size

            seek_started = time.monotonic_ns()
            f.seek(offset)
            seek_ns += time.monotonic_ns() - seek_started
            read_started = time.monotonic_ns()
            chunk = f.read(length)
            read_ns += time.monotonic_ns() - read_started
            if len(chunk) != length:
                raise SystemExit(f"short random read offset={offset} expected={length} got={len(chunk)}")
            if verify:
                verify_started = time.monotonic_ns()
                expected_chunk = deterministic_payload_range(manifest["nonce"], expected["label"], offset, length)
                if chunk != expected_chunk:
                    got = hashlib.sha256(chunk).hexdigest()
                    want = hashlib.sha256(expected_chunk).hexdigest()
                    raise SystemExit(f"random chunk mismatch offset={offset} got={got} want={want}")

                aggregate.update(str(offset).encode())
                aggregate.update(b"\0")
                aggregate.update(chunk)
                verify_ns += time.monotonic_ns() - verify_started
            bytes_read += length
            ops += 1

    return bytes_read, ops, aggregate.hexdigest() if verify else "", {
        "openMs": open_ns / 1_000_000,
        "seekMs": seek_ns / 1_000_000,
        "fileReadMs": read_ns / 1_000_000,
        "verifyMs": verify_ns / 1_000_000,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--pattern", choices=("sequential", "random"), required=True)
    parser.add_argument("--random-block-bytes", type=int, default=4096)
    parser.add_argument("--random-total-bytes", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--skip-verify", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    manifest, entry = manifest_entry(root, args.file)
    path = root / args.file
    started = time.monotonic_ns()
    verify = not args.skip_verify
    if args.pattern == "sequential":
        bytes_read, ops, digest, timing = sequential(path, entry, verify)
    else:
        bytes_read, ops, digest, timing = random_reads(
            path,
            manifest,
            entry,
            args.random_block_bytes,
            args.random_total_bytes,
            args.seed,
            verify,
        )
    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    mb = bytes_read / (1024 * 1024)
    print(json.dumps({
        "ok": True,
        "path": args.file,
        "pattern": args.pattern,
        "fileSizeBytes": entry["size"],
        "bytes": bytes_read,
        "operations": ops,
        "durationMs": duration_ms,
        "timing": timing,
        "mbps": mb / (duration_ms / 1000) if duration_ms > 0 else 0,
        "fileReadMBps": mb / (timing.get("fileReadMs", 0) / 1000) if timing.get("fileReadMs", 0) > 0 else 0,
        "opsPerSecond": ops / (duration_ms / 1000) if duration_ms > 0 else 0,
        "digest": digest,
        "expectedSha256": entry["sha256"],
        "verified": verify,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
"""

REMOTE_RAW_READ_HARNESS = r"""
import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import socket
import struct
import threading
import time

MAGIC = b"B9CR\x01"
VERSION = 1
STATUS_OK = 0
CHUNK_BYTES = 16 * 1024 * 1024
_thread_state = threading.local()


def parse_addr(addr):
    if addr.startswith("["):
        host, _, rest = addr[1:].partition("]")
        return host, int(rest.lstrip(":"))
    host, port = addr.rsplit(":", 1)
    return host, int(port)


def read_exact(sock, length):
    out = bytearray(length)
    view = memoryview(out)
    read_ns = 0
    pos = 0
    while pos < length:
        read_started = time.monotonic_ns()
        n = sock.recv_into(view[pos:], length - pos)
        read_ns += time.monotonic_ns() - read_started
        if not n:
            raise RuntimeError(f"short socket read expected={length} got={pos}")
        pos += n
    return out, read_ns


def close_thread_sock():
    sock = getattr(_thread_state, "sock", None)
    if sock is not None:
        try:
            sock.close()
        finally:
            _thread_state.sock = None


def thread_sock(host, port, connect_timeout):
    sock = getattr(_thread_state, "sock", None)
    if sock is not None:
        return sock
    sock = socket.create_connection((host, port), timeout=connect_timeout)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
    sock.sendall(MAGIC)
    _thread_state.sock = sock
    return sock


def read_chunk_once(sock, hash_value, offset, length):
    read_ns = 0
    request = struct.pack(">B H Q Q", VERSION, len(hash_value), offset, length) + hash_value.encode()
    sock.sendall(request)
    response, response_read_ns = read_exact(sock, 9)
    read_ns += response_read_ns
    status = response[0]
    response_length = struct.unpack(">Q", response[1:9])[0]
    if status != STATUS_OK:
        raise RuntimeError(f"raw read status={status} offset={offset} length={length}")
    if response_length != length:
        raise RuntimeError(
            f"raw read length mismatch offset={offset} expected={length} got={response_length}"
        )
    body, body_read_ns = read_exact(sock, response_length)
    read_ns += body_read_ns
    return offset, body, read_ns


def read_chunk(host, port, hash_value, offset, length, connect_timeout):
    for attempt in range(2):
        try:
            return read_chunk_once(thread_sock(host, port, connect_timeout), hash_value, offset, length)
        except Exception:
            close_thread_sock()
            if attempt:
                raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", required=True)
    parser.add_argument("--hash", required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--chunk-bytes", type=int, default=CHUNK_BYTES)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--connect-timeout", type=float, default=5)
    args = parser.parse_args()

    if args.chunk_bytes <= 0:
        raise SystemExit("--chunk-bytes must be positive")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be positive")

    host, port = parse_addr(args.addr)
    hasher = hashlib.sha256()
    chunks = [
        (offset, min(args.chunk_bytes, args.size - offset))
        for offset in range(0, args.size, args.chunk_bytes)
    ]
    read_ns = 0
    verify_ns = 0
    started = time.monotonic_ns()
    read_started = time.monotonic_ns()
    next_submit = 0
    next_hash_offset = 0
    pending = {}
    in_flight = {}
    max_buffered_chunks = args.concurrency

    def submit_more(executor):
        nonlocal next_submit
        while next_submit < len(chunks) and len(in_flight) + len(pending) < max_buffered_chunks:
            chunk_offset, length = chunks[next_submit]
            future = executor.submit(
                read_chunk,
                host,
                port,
                args.hash,
                chunk_offset,
                length,
                args.connect_timeout,
            )
            in_flight[future] = chunk_offset
            next_submit += 1

    with ThreadPoolExecutor(max_workers=min(args.concurrency, max(1, len(chunks)))) as executor:
        submit_more(executor)
        while in_flight:
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for future in done:
                del in_flight[future]
                chunk_offset, body, chunk_read_ns = future.result()
                read_ns += chunk_read_ns
                pending[chunk_offset] = body

            while (len(in_flight) + len(pending) >= max_buffered_chunks) and next_hash_offset in pending:
                body = pending.pop(next_hash_offset)
                verify_started = time.monotonic_ns()
                hasher.update(body)
                verify_ns += time.monotonic_ns() - verify_started
                next_hash_offset += len(body)

            submit_more(executor)

    read_wall_ns = max(0, time.monotonic_ns() - read_started - verify_ns)

    while next_hash_offset in pending:
        body = pending.pop(next_hash_offset)
        verify_started = time.monotonic_ns()
        hasher.update(body)
        verify_ns += time.monotonic_ns() - verify_started
        next_hash_offset += len(body)

    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    read_ms = read_wall_ns / 1_000_000
    socket_wait_ms = read_ns / 1_000_000
    verify_ms = verify_ns / 1_000_000
    digest = hasher.hexdigest()
    if digest != args.expected_sha256:
        raise RuntimeError(f"raw read digest mismatch got={digest} want={args.expected_sha256}")

    mb = args.size / (1024 * 1024)
    print(json.dumps({
        "ok": True,
        "addr": args.addr,
        "hash": args.hash,
        "bytes": args.size,
        "chunks": len(chunks),
        "chunkBytes": args.chunk_bytes,
        "concurrency": args.concurrency,
        "durationMs": duration_ms,
        "socketReadMs": read_ms,
        "socketWaitMs": socket_wait_ms,
        "verifyMs": verify_ms,
        "mbps": mb / (duration_ms / 1000) if duration_ms > 0 else 0,
        "socketReadMBps": mb / (read_ms / 1000) if read_ms > 0 else 0,
        "sha256": digest,
        "expectedSha256": args.expected_sha256,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
"""

REMOTE_RAW_READ_GO_HARNESS = r"""
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	magic    = "B9CR\x01"
	version  = byte(1)
	statusOK = byte(0)
)

type task struct {
	offset int64
	length int
}

type result struct {
	offset int64
	body   []byte
	readNS int64
	err    error
}

func parseAddr(addr string) (string, string, error) {
	if strings.HasPrefix(addr, "[") {
		i := strings.LastIndex(addr, "]:")
		if i < 0 {
			return "", "", fmt.Errorf("invalid address %q", addr)
		}
		return addr[1:i], addr[i+2:], nil
	}
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return "", "", fmt.Errorf("invalid address %q", addr)
	}
	return addr[:i], addr[i+1:], nil
}

func dial(addr string, timeout time.Duration) (net.Conn, error) {
	host, port, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetReadBuffer(16 * 1024 * 1024)
		_ = tcp.SetWriteBuffer(16 * 1024 * 1024)
	}
	if _, err := conn.Write([]byte(magic)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func writeRequest(w io.Writer, hash string, offset int64, length int) error {
	if len(hash) == 0 || len(hash) > 512 {
		return fmt.Errorf("invalid hash length %d", len(hash))
	}
	var hdr [19]byte
	hdr[0] = version
	binary.BigEndian.PutUint16(hdr[1:3], uint16(len(hash)))
	binary.BigEndian.PutUint64(hdr[3:11], uint64(offset))
	binary.BigEndian.PutUint64(hdr[11:19], uint64(length))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write([]byte(hash))
	return err
}

func readOne(conn net.Conn, hash string, t task) result {
	started := time.Now()
	if err := writeRequest(conn, hash, t.offset, t.length); err != nil {
		return result{offset: t.offset, err: err}
	}
	var rh [9]byte
	if _, err := io.ReadFull(conn, rh[:]); err != nil {
		return result{offset: t.offset, err: err}
	}
	if rh[0] != statusOK {
		return result{offset: t.offset, err: fmt.Errorf("raw status=%d", rh[0])}
	}
	n := int(binary.BigEndian.Uint64(rh[1:9]))
	if n != t.length {
		return result{offset: t.offset, err: fmt.Errorf("length mismatch got=%d want=%d", n, t.length)}
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(conn, body); err != nil {
		return result{offset: t.offset, err: err}
	}
	return result{offset: t.offset, body: body, readNS: time.Since(started).Nanoseconds()}
}

func worker(addr, hash string, timeout time.Duration, tasks <-chan task, results chan<- result, readNS *int64) {
	var conn net.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	for t := range tasks {
		if conn == nil {
			c, err := dial(addr, timeout)
			if err != nil {
				results <- result{offset: t.offset, err: err}
				continue
			}
			conn = c
		}
		res := readOne(conn, hash, t)
		if res.err != nil {
			_ = conn.Close()
			conn = nil
			c, err := dial(addr, timeout)
			if err == nil {
				conn = c
				res = readOne(conn, hash, t)
			}
		}
		atomic.AddInt64(readNS, res.readNS)
		results <- res
	}
}

func main() {
	addr := flag.String("addr", "", "")
	hash := flag.String("hash", "", "")
	size := flag.Int64("size", 0, "")
	expected := flag.String("expected-sha256", "", "")
	chunkBytes := flag.Int("chunk-bytes", 16*1024*1024, "")
	concurrency := flag.Int("concurrency", 16, "")
	connectTimeout := flag.Duration("connect-timeout", 5*time.Second, "")
	flag.Parse()

	if *addr == "" || *hash == "" || *size <= 0 || *expected == "" {
		fmt.Fprintln(os.Stderr, "--addr, --hash, --size, and --expected-sha256 are required")
		os.Exit(2)
	}
	if *chunkBytes <= 0 || *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "--chunk-bytes and --concurrency must be positive")
		os.Exit(2)
	}

	var chunks []task
	for off := int64(0); off < *size; off += int64(*chunkBytes) {
		n := *chunkBytes
		if rem := *size - off; int64(n) > rem {
			n = int(rem)
		}
		chunks = append(chunks, task{offset: off, length: n})
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].offset < chunks[j].offset })

	started := time.Now()
	readStarted := time.Now()
	tasks := make(chan task, *concurrency)
	results := make(chan result, *concurrency*2)
	var readWaitNS int64
	var wg sync.WaitGroup
	workers := *concurrency
	if workers > len(chunks) {
		workers = len(chunks)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(*addr, *hash, *connectTimeout, tasks, results, &readWaitNS)
		}()
	}
	go func() {
		for _, t := range chunks {
			tasks <- t
		}
		close(tasks)
		wg.Wait()
		close(results)
	}()

	hasher := sha256.New()
	pending := make(map[int64][]byte, workers*2)
	next := int64(0)
	verifyNS := int64(0)
	for res := range results {
		if res.err != nil {
			fmt.Fprintln(os.Stderr, res.err)
			os.Exit(1)
		}
		pending[res.offset] = res.body
		for {
			body, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			verifyStarted := time.Now()
			_, _ = hasher.Write(body)
			verifyNS += time.Since(verifyStarted).Nanoseconds()
			next += int64(len(body))
		}
	}
	if next != *size {
		fmt.Fprintf(os.Stderr, "short read got=%d want=%d\n", next, *size)
		os.Exit(1)
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	if digest != *expected {
		fmt.Fprintf(os.Stderr, "digest mismatch got=%s want=%s\n", digest, *expected)
		os.Exit(1)
	}

	durationMs := float64(time.Since(started).Nanoseconds()) / 1_000_000
	readWallNS := time.Since(readStarted).Nanoseconds() - verifyNS
	if readWallNS < 0 {
		readWallNS = 0
	}
	socketReadMs := float64(readWallNS) / 1_000_000
	verifyMs := float64(verifyNS) / 1_000_000
	socketWaitMs := float64(atomic.LoadInt64(&readWaitNS)) / 1_000_000
	mb := float64(*size) / (1024 * 1024)
	out := map[string]any{
		"ok":             true,
		"addr":           *addr,
		"hash":           *hash,
		"bytes":          *size,
		"chunks":         len(chunks),
		"chunkBytes":     *chunkBytes,
		"concurrency":    *concurrency,
		"durationMs":     durationMs,
		"socketReadMs":   socketReadMs,
		"socketWaitMs":   socketWaitMs,
		"verifyMs":       verifyMs,
		"mbps":           mb / (durationMs / 1000),
		"socketReadMBps": mb / (socketReadMs / 1000),
		"sha256":         digest,
		"expectedSha256": *expected,
	}
	_ = json.NewEncoder(os.Stdout).Encode(out)
}
"""

REMOTE_RAW_READ_BINARIES = {}

DD_READ_HARNESS = r"""
import argparse
import json
import os
import subprocess
import time


def run_dd(path, block_size):
    attempts = [
        ["dd", f"if={path}", "of=/dev/null", f"bs={block_size}", "iflag=direct,fullblock", "status=none"],
        ["dd", f"if={path}", "of=/dev/null", f"bs={block_size}", "iflag=fullblock", "status=none"],
        ["dd", f"if={path}", "of=/dev/null", f"bs={block_size}", "status=none"],
        ["dd", f"if={path}", "of=/dev/null", f"bs={block_size}"],
    ]
    last = None
    for command in attempts:
        started = time.monotonic_ns()
        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        finished = time.monotonic_ns()
        last = proc
        if proc.returncode == 0:
            return command, (finished - started) / 1_000_000, proc
        stderr = proc.stderr.lower()
        unsupported = "invalid" in stderr or "unrecognized" in stderr or "not supported" in stderr
        if not unsupported:
            break
    raise SystemExit((last.stderr or last.stdout or f"dd exited {last.returncode}").strip())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument("--expected-size", type=int, required=True)
    parser.add_argument("--block-size", default="4M")
    args = parser.parse_args()

    stat_size = os.stat(args.path).st_size
    if stat_size != args.expected_size:
        raise SystemExit(f"size mismatch path={args.path} got={stat_size} want={args.expected_size}")

    command, duration_ms, proc = run_dd(args.path, args.block_size)
    mb = args.expected_size / (1024 * 1024)
    print(json.dumps({
        "ok": True,
        "path": args.path,
        "bytes": args.expected_size,
        "durationMs": duration_ms,
        "mbps": mb / (duration_ms / 1000) if duration_ms > 0 else 0,
        "blockSize": args.block_size,
        "directIO": any(flag.startswith("iflag=direct") or flag == "iflag=direct,fullblock" for flag in command),
        "command": command,
        "ddStdout": proc.stdout[-1000:],
        "ddStderr": proc.stderr[-1000:],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
"""

EVICT_CACHE_OBJECT_HARNESS = r"""
import json
import os
import sys
import time
from pathlib import Path


def fadvise_dontneed(fd):
    advise = getattr(os, "posix_fadvise", None)
    if advise is None:
        return False, "os.posix_fadvise unavailable"
    flag = getattr(os, "POSIX_FADV_DONTNEED", 4)
    advise(fd, 0, 0, flag)
    return True, ""


def main():
    if len(sys.argv) < 3:
        raise SystemExit("usage: evict_cache_object.py <hash> <candidate-dir>...")
    content_hash = sys.argv[1]
    candidates = [Path(p) for p in sys.argv[2:]]
    started = time.monotonic_ns()
    if hasattr(os, "sync"):
        os.sync()

    found = None
    for candidate in candidates:
        if candidate.is_dir():
            found = candidate
            break

    result = {
        "hash": content_hash,
        "exists": found is not None,
        "evicted": False,
        "path": str(found) if found else "",
        "chunks": 0,
        "bytes": 0,
        "errors": [],
    }
    if found is None:
        print(json.dumps(result, sort_keys=True))
        return

    index = 0
    while True:
        path = found / f"{content_hash}-{index}"
        if not path.exists():
            break
        try:
            size = path.stat().st_size
            fd = os.open(path, os.O_RDONLY)
            try:
                ok, err = fadvise_dontneed(fd)
                if not ok and err:
                    result["errors"].append({"path": str(path), "error": err})
            finally:
                os.close(fd)
            result["chunks"] += 1
            result["bytes"] += size
        except Exception as exc:
            result["errors"].append({"path": str(path), "error": str(exc)})
        index += 1

    result["evicted"] = result["exists"] and result["chunks"] > 0 and not result["errors"]
    result["durationMs"] = (time.monotonic_ns() - started) / 1_000_000
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
"""

EVICT_MOUNTED_FILE_HARNESS = r"""
import json
import os
import sys
import time


def fadvise_dontneed(fd):
    advise = getattr(os, "posix_fadvise", None)
    if advise is None:
        return False, "os.posix_fadvise unavailable"
    flag = getattr(os, "POSIX_FADV_DONTNEED", 4)
    advise(fd, 0, 0, flag)
    return True, ""


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: evict_mounted_file.py <path> <expected-size>")
    path = sys.argv[1]
    expected_size = int(sys.argv[2])
    started = time.monotonic_ns()
    result = {
        "path": path,
        "exists": os.path.exists(path),
        "evicted": False,
        "bytes": 0,
        "errors": [],
    }
    if not result["exists"]:
        print(json.dumps(result, sort_keys=True))
        return
    try:
        stat = os.stat(path)
        result["bytes"] = stat.st_size
        if expected_size and stat.st_size != expected_size:
            result["errors"].append(f"size mismatch got={stat.st_size} want={expected_size}")
        if hasattr(os, "sync"):
            os.sync()
        fd = os.open(path, os.O_RDONLY)
        try:
            ok, err = fadvise_dontneed(fd)
            if not ok and err:
                result["errors"].append(err)
        finally:
            os.close(fd)
    except Exception as exc:
        result["errors"].append(str(exc))
    result["evicted"] = result["exists"] and not result["errors"]
    result["durationMs"] = (time.monotonic_ns() - started) / 1_000_000
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
"""

PAYLOAD_GENERATOR = r"""
import hashlib
import json
import os
from pathlib import Path

ROOT = Path("/bench-cache")
SIZES_MB = [int(part) for part in os.environ["CACHE_BENCH_SIZES_MB"].split(",") if part]
ACCESS_TYPES = [part for part in os.environ["CACHE_BENCH_ACCESS_TYPES"].split(",") if part]
PATTERNS = [part for part in os.environ["CACHE_BENCH_PATTERNS"].split(",") if part]
FILE_PLAN = os.environ.get("CACHE_BENCH_FILE_PLAN", "")
NONCE = os.environ["CACHE_BENCH_NONCE"]
CHUNK_SIZE = 1024 * 1024
""" + DETERMINISTIC_PAYLOAD_SOURCE + r"""

def parse_file_plan(value):
    specs = []
    for part in [part.strip() for part in value.split(",") if part.strip()]:
        fields = part.split(":")
        if len(fields) != 3:
            raise SystemExit(f"invalid CACHE_BENCH_FILE_PLAN entry {part!r}; expected access:pattern:sizeMiB")
        access_type, pattern, size_text = fields
        size_text = size_text.lower().removesuffix("mib").removesuffix("mb")
        specs.append({"accessType": access_type, "pattern": pattern, "sizeMiB": int(size_text)})
    return specs

def write_file(path, label, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256()
    remaining = size
    offset = 0
    with path.open("wb") as f:
        while remaining > 0:
            chunk_len = min(CHUNK_SIZE, remaining)
            chunk = deterministic_payload_range(NONCE, label, offset, chunk_len)
            f.write(chunk)
            h.update(chunk)
            remaining -= chunk_len
            offset += chunk_len
    return h.hexdigest()


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    files = []
    specs = parse_file_plan(FILE_PLAN)
    if not specs:
        specs = [
            {"accessType": access_type, "pattern": pattern, "sizeMiB": size_mb}
            for access_type in ACCESS_TYPES
            for pattern in PATTERNS
            for size_mb in SIZES_MB
        ]

    for spec in specs:
        access_type = spec["accessType"]
        pattern = spec["pattern"]
        size_mb = spec["sizeMiB"]
        size = size_mb * 1024 * 1024
        label = f"{access_type}:{pattern}:size:{size_mb}mb"
        rel = f"files/{access_type}/{pattern}/{size_mb}mb.bin"
        digest = write_file(ROOT / rel, label, size)
        files.append({
            "path": rel,
            "accessType": access_type,
            "pattern": pattern,
            "size": size,
            "sizeMiB": size_mb,
            "sha256": digest,
            "label": label,
        })

    manifest = {
        "version": 3,
        "nonce": NONCE,
        "files": files,
    }
    (ROOT / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
"""

VOLUME_PREPARE_SCRIPT = r"""
import argparse
import hashlib
import json
import os
import shutil
import time
from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024
""" + DETERMINISTIC_PAYLOAD_SOURCE + r"""

def write_generated_file(path, nonce, label, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256()
    remaining = size
    offset = 0
    with path.open("wb") as f:
        while remaining > 0:
            chunk_len = min(CHUNK_SIZE, remaining)
            chunk = deterministic_payload_range(nonce, label, offset, chunk_len)
            f.write(chunk)
            h.update(chunk)
            remaining -= chunk_len
            offset += chunk_len
        f.flush()
        os.fsync(f.fileno())
    return h.hexdigest()


def fsync_path(path):
    with path.open("rb") as f:
        os.fsync(f.fileno())


def sha256_file(path):
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return size, h.hexdigest()


def verify(root, access_types=None):
    manifest = json.loads((root / "manifest.json").read_text())
    total = 0
    files = 0
    for entry in manifest["files"]:
        if access_types and entry.get("accessType") not in access_types:
            continue
        size, digest = sha256_file(root / entry["path"])
        if size != entry["size"] or digest != entry["sha256"]:
            raise SystemExit(f"mismatch path={entry['path']} size={size} sha256={digest}")
        total += size
        files += 1
    return manifest, total, files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="/bench-cache")
    parser.add_argument("--dest", required=True)
    parser.add_argument("--access-types", default="")
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--manifest-json", default="")
    parser.add_argument("--skip-dest-verify", action="store_true")
    args = parser.parse_args()

    source = Path(args.source)
    dest = Path(args.dest)
    access_types = {part for part in args.access_types.split(",") if part}
    started = time.monotonic_ns()
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.mkdir(parents=True, exist_ok=True)
    if args.generate:
        manifest = json.loads(args.manifest_json)
    else:
        manifest = json.loads((source / "manifest.json").read_text())
    manifest_path = dest / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
    fsync_path(manifest_path)
    total = 0
    files = 0
    for entry in manifest["files"]:
        if access_types and entry.get("accessType") not in access_types:
            continue
        dest_path = dest / entry["path"]
        if args.generate:
            digest = write_generated_file(dest_path, manifest["nonce"], entry["label"], entry["size"])
            if digest != entry["sha256"]:
                raise SystemExit(f"generated digest mismatch path={entry['path']} got={digest} want={entry['sha256']}")
            total += entry["size"]
            files += 1
        else:
            source_path = source / entry["path"]
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, dest_path)
            fsync_path(dest_path)
            total += entry["size"]
            files += 1
    os.sync()
    if not args.generate and not args.skip_dest_verify:
        manifest, total, files = verify(dest, access_types)
    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    mb = total / (1024 * 1024)
    print(json.dumps({
        "ok": True,
        "root": str(dest),
        "bytes": total,
        "files": files,
        "manifest": manifest,
        "durationMs": duration_ms,
        "mbps": mb / (duration_ms / 1000) if duration_ms > 0 else 0,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
"""

STORAGE_READY_HARNESS = r"""
import argparse
import json
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--manifest-json", required=True)
    parser.add_argument("--access-types", default="")
    args = parser.parse_args()

    root = Path(args.root)
    expected = json.loads(args.manifest_json)
    access_types = {part for part in args.access_types.split(",") if part}
    selected = [
        entry
        for entry in expected.get("files", [])
        if not access_types or entry.get("accessType") in access_types
    ]
    started = time.monotonic_ns()

    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        print(json.dumps({"ready": False, "missing": "manifest.json"}, sort_keys=True))
        return

    try:
        observed = json.loads(manifest_path.read_text())
    except Exception as exc:
        print(json.dumps({"ready": False, "error": f"manifest read failed: {exc}"}, sort_keys=True))
        return

    if observed != expected:
        print(json.dumps({"ready": False, "error": "manifest mismatch"}, sort_keys=True))
        return

    checked = 0
    bytes_seen = 0
    for entry in selected:
        path = root / entry["path"]
        if not path.exists():
            print(json.dumps({"ready": False, "missing": entry["path"]}, sort_keys=True))
            return
        actual_size = path.stat().st_size
        if actual_size != int(entry["size"]):
            print(json.dumps({
                "ready": False,
                "path": entry["path"],
                "size": int(entry["size"]),
                "actual": actual_size,
            }, sort_keys=True))
            return
        checked += 1
        bytes_seen += actual_size

    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    print(json.dumps({
        "ready": True,
        "checked": checked,
        "bytes": bytes_seen,
        "durationMs": duration_ms,
        "root": str(root),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
"""

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
LOG_SIZE_RE = re.compile(r"size=(\d+)")
GEESEFS_SUMMARY_RE = re.compile(
    r"geesefs read path summary: .*?"
    r"timing\(handler_count=(\d+) handler_avg=([^ ]+) callback_count=(\d+) callback=([0-9.]+)MiB callback_avg=([^)]*)\).*?"
    r"mmap_page\(attempt=(\d+) hit=(\d+) miss=(\d+) mmap_fail=(\d+) ([0-9.]+)MiB[^)]*\).*?"
    r"read_into\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"stream\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"unary\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"cloud\(req=(\d+) ([0-9.]+)MiB\)"
)
GEESEFS_SUMMARY_LEGACY_RE = re.compile(
    r"geesefs read path summary: .*?"
    r"mmap_page\(attempt=(\d+) hit=(\d+) miss=(\d+) mmap_fail=(\d+) ([0-9.]+)MiB\).*?"
    r"read_into\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"stream\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"unary\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"cloud\(req=(\d+) ([0-9.]+)MiB\)"
)
GEESEFS_CACHE_EVENT_RE = re.compile(
    r"cache_event\(queued=(\d+) started=(\d+) ok=(\d+) err=(\d+) mismatch=(\d+) dropped=(\d+) ([0-9.]+)MiB\)"
)
GEESEFS_READ_RESPONSE_RE = re.compile(
    r"geesefs fuse read response complete: .*?"
    r"path=\"([^\"]+)\" hash=\"([^\"]*)\" offset=(\d+) size=(\d+) bytes=(\d+) .*?"
    r"handler_ms=([0-9.]+) response_ms=([0-9.]+)"
)
CACHE_SUMMARY_RE = re.compile(
    r"cache read path summary: .*?"
    r"client\(read_into=(\d+) ([0-9.]+)MiB local_hit=(\d+) local_miss=(\d+) raw_hit=(\d+) raw_miss=(\d+) raw_err=(\d+) grpc_hit=(\d+) grpc_miss=(\d+) grpc_err=(\d+)\) "
    r"local_page_region\(req=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"server\(.*?raw_req=(\d+) raw_sendfile=(\d+) raw_copy=(\d+) raw_readat=(\d+) raw_miss=(\d+) raw_err=(\d+)\) "
    r"store\(.*?page_region=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\)"
)
CLIP_EMBEDDED_CACHE_SOURCES = {"content_cache_page_fd", "content_cache"}
CLIP_FD_SOURCES = {"content_cache_page_fd", "disk_cache_fd", "local_archive_fd"}
CLIP_REGISTRY_SOURCES = {"oci_registry", "remote_archive", "checkpoint", "decompressed_layer"}


def now_rfc3339():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_csv(value):
    return [part.strip() for part in value.split(",") if part.strip()]


def parse_sizes(value):
    sizes = [int(part) for part in parse_csv(value)]
    if not sizes or any(size <= 0 for size in sizes):
        raise SystemExit("--sizes-mb must contain positive integers")
    return sizes


def parse_size_mib(value):
    text = value.strip().lower()
    if text.endswith("mib"):
        text = text[:-3]
    elif text.endswith("mb"):
        text = text[:-2]
    try:
        size = int(text)
    except ValueError as exc:
        raise SystemExit(f"invalid file-plan size {value!r}") from exc
    if size <= 0:
        raise SystemExit(f"file-plan size must be positive: {value!r}")
    return size


def parse_file_plan(value, valid_access_types, valid_patterns):
    specs = []
    seen = set()
    for part in parse_csv(value):
        fields = [field.strip() for field in part.split(":")]
        if len(fields) != 3:
            raise SystemExit(f"invalid --file-plan entry {part!r}; expected access:pattern:sizeMiB")
        access_type, pattern, size_text = fields
        if access_type not in valid_access_types:
            raise SystemExit(f"invalid --file-plan access type {access_type!r}")
        if pattern not in valid_patterns:
            raise SystemExit(f"invalid --file-plan pattern {pattern!r}")
        size_mb = parse_size_mib(size_text)
        key = (access_type, pattern, size_mb)
        if key in seen:
            raise SystemExit(f"duplicate --file-plan entry {access_type}:{pattern}:{size_mb}mib")
        seen.add(key)
        specs.append({"accessType": access_type, "pattern": pattern, "sizeMiB": size_mb})
    return specs


def ordered_unique(values):
    out = []
    seen = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark beta9 embedded cache access patterns.")
    parser.add_argument("--namespace", default=os.getenv("BENCH_NAMESPACE", "beta9"))
    parser.add_argument("--gateway-url", default=os.getenv("BENCH_GATEWAY_URL", "http://127.0.0.1:11994"))
    parser.add_argument("--grpc-addr", default=os.getenv("BENCH_GRPC_ADDR", "127.0.0.1:11993"))
    parser.add_argument("--token", default=os.getenv("BENCH_TOKEN") or os.getenv("BETA9_TOKEN") or "")
    parser.add_argument("--token-cache", default=os.getenv("BENCH_TOKEN_CACHE", ""))
    parser.add_argument("--timeout-seconds", type=float, default=env_float("BENCH_TIMEOUT_SECONDS", 300))
    parser.add_argument("--poll-interval-ms", type=int, default=env_int("BENCH_POLL_INTERVAL_MS", 100))
    parser.add_argument("--cleanup-ttl-seconds", type=int, default=env_int("BENCH_CLEANUP_TTL_SECONDS", 5))
    parser.add_argument("--sdk-config", default=os.getenv("BENCH_SDK_CONFIG", ""))
    parser.add_argument("--cleanup-cache-before-run", dest="cleanup_cache_before_run", action="store_true")
    parser.add_argument("--no-cleanup-cache-before-run", dest="cleanup_cache_before_run", action="store_false")
    parser.set_defaults(cleanup_cache_before_run=env_bool("BENCH_CACHE_CLEANUP_BEFORE_RUN", True))
    parser.add_argument("--cleanup-cache-after-run", dest="cleanup_cache_after_run", action="store_true")
    parser.add_argument("--no-cleanup-cache-after-run", dest="cleanup_cache_after_run", action="store_false")
    parser.set_defaults(cleanup_cache_after_run=env_bool("BENCH_CACHE_CLEANUP_AFTER_RUN", False))
    parser.add_argument(
        "--strict-disk-cache-hit",
        dest="strict_disk_cache_hit",
        action="store_true",
        help=(
            "Make storage hot-read rows conservative by clearing geesefs/FUSE state, "
            "evicting embedded cache page files from the kernel page cache, and "
            "requiring eviction proof before timed hot and remote raw reads."
        ),
    )
    parser.add_argument("--no-strict-disk-cache-hit", dest="strict_disk_cache_hit", action="store_false")
    parser.set_defaults(strict_disk_cache_hit=env_bool("BENCH_CACHE_STRICT_DISK_HIT", False))

    parser.add_argument("--preset", choices=("default", "smoke"), default="default")
    parser.add_argument("--sizes-mb", default=os.getenv("BENCH_CACHE_SIZES_MB", "32,128"))
    parser.add_argument("--patterns", default=os.getenv("BENCH_CACHE_PATTERNS", "sequential,random"))
    parser.add_argument("--access-types", default=os.getenv("BENCH_CACHE_ACCESS_TYPES", "image_archive,volume_mount,workspace_fuse"))
    parser.add_argument("--file-plan", default=os.getenv("BENCH_CACHE_FILE_PLAN", ""))
    parser.add_argument("--iterations", type=int, default=env_int("BENCH_CACHE_ITERATIONS", 1))
    parser.add_argument("--random-block-bytes", type=int, default=env_int("BENCH_CACHE_RANDOM_BLOCK_BYTES", 4096))
    parser.add_argument("--random-seed", type=int, default=env_int("BENCH_CACHE_RANDOM_SEED", 1337))
    parser.add_argument("--verify-reads", dest="verify_reads", action="store_true")
    parser.add_argument("--no-verify-reads", dest="verify_reads", action="store_false")
    parser.set_defaults(verify_reads=env_bool("BENCH_CACHE_VERIFY_READS", True))

    parser.add_argument("--image-uri", default=os.getenv("BENCH_CACHE_IMAGE_URI", ""))
    parser.add_argument("--image-platform", default=os.getenv("BENCH_CACHE_IMAGE_PLATFORM", ""))
    parser.add_argument("--source-image", default=os.getenv("BENCH_CACHE_SOURCE_IMAGE", "python:3.10-slim"))
    parser.add_argument("--runtime-python-version", default=os.getenv("BENCH_CACHE_RUNTIME_PYTHON_VERSION", "python3.10"))
    parser.add_argument("--verify-registry-blobs", dest="verify_registry_blobs", action="store_true")
    parser.add_argument("--no-verify-registry-blobs", dest="verify_registry_blobs", action="store_false")
    parser.set_defaults(verify_registry_blobs=env_bool("BENCH_CACHE_VERIFY_REGISTRY_BLOBS", True))
    parser.add_argument("--generate-volume-payload", dest="generate_volume_payload", action="store_true")
    parser.add_argument("--no-generate-volume-payload", dest="generate_volume_payload", action="store_false")
    generate_volume_payload_env = os.getenv("BENCH_CACHE_GENERATE_VOLUME_PAYLOAD")
    parser.set_defaults(
        generate_volume_payload=None
        if generate_volume_payload_env is None
        else env_bool("BENCH_CACHE_GENERATE_VOLUME_PAYLOAD", False)
    )
    parser.add_argument("--output", default=os.getenv("BENCH_CACHE_OUTPUT", "/tmp/beta9-cache-benchmark.json"))
    parser.add_argument("--report", default=os.getenv("BENCH_CACHE_REPORT", "/tmp/beta9-cache-benchmark.md"))
    parser.add_argument("--cache-mount-path", default=os.getenv("BENCH_CACHE_MOUNT_PATH", DEFAULT_CACHE_MOUNT_PATH))
    parser.add_argument("--image-cache-path", default=os.getenv("BENCH_CACHE_IMAGE_CACHE_PATH", DEFAULT_IMAGE_CACHE_PATH))
    parser.add_argument("--cache-locality", default=os.getenv("BENCH_CACHE_LOCALITY", "default"))
    parser.add_argument("--localstack-service", default=os.getenv("BENCH_CACHE_LOCALSTACK_SERVICE", "localstack"))
    parser.add_argument("--localstack-local-port", type=int, default=env_int("BENCH_CACHE_LOCALSTACK_LOCAL_PORT", 4566))
    parser.add_argument("--localstack-service-port", type=int, default=env_int("BENCH_CACHE_LOCALSTACK_SERVICE_PORT", 4566))
    parser.add_argument("--volume-name", default=os.getenv("BENCH_CACHE_VOLUME_NAME", "embedded-cache-benchmark"))
    parser.add_argument("--volume-mount-path", default=os.getenv("BENCH_CACHE_VOLUME_MOUNT_PATH", "cache-bench"))
    parser.add_argument("--volume-subdir", default=os.getenv("BENCH_CACHE_VOLUME_SUBDIR", ""))
    parser.add_argument("--settle-seconds", type=float, default=env_float("BENCH_CACHE_SETTLE_SECONDS", 5))
    parser.add_argument("--cache-proof-timeout-seconds", type=float, default=env_float("BENCH_CACHE_PROOF_TIMEOUT_SECONDS", 30))
    parser.add_argument("--remote-cache-proof-chunk-bytes", type=int, default=env_int("BENCH_CACHE_REMOTE_PROOF_CHUNK_BYTES", 16 * 1024 * 1024))
    parser.add_argument("--remote-cache-proof-concurrency", type=int, default=env_int("BENCH_CACHE_REMOTE_PROOF_CONCURRENCY", 16))
    parser.add_argument("--runtime-prepare-attempts", type=int, default=env_int("BENCH_CACHE_RUNTIME_PREPARE_ATTEMPTS", 3))
    parser.add_argument("--worker-dd-reads", dest="worker_dd_reads", action="store_true")
    parser.add_argument("--no-worker-dd-reads", dest="worker_dd_reads", action="store_false")
    parser.set_defaults(worker_dd_reads=env_bool("BENCH_CACHE_WORKER_DD_READS", False))
    parser.add_argument("--sandbox-dd-reads", dest="sandbox_dd_reads", action="store_true")
    parser.add_argument("--no-sandbox-dd-reads", dest="sandbox_dd_reads", action="store_false")
    parser.set_defaults(sandbox_dd_reads=env_bool("BENCH_CACHE_SANDBOX_DD_READS", False))
    parser.add_argument(
        "--storage-ready-worker-count",
        type=int,
        default=env_int("BENCH_CACHE_STORAGE_READY_WORKERS", 1),
        help="Number of default worker pods that must see prepared storage before read benchmarks start.",
    )
    parser.add_argument("--worker-dd-block-size", default=os.getenv("BENCH_CACHE_WORKER_DD_BLOCK_SIZE", "4M"))
    parser.add_argument("--worker-dd-log-wait-seconds", type=float, default=env_float("BENCH_CACHE_WORKER_DD_LOG_WAIT_SECONDS", 11))
    parser.add_argument("--read-path-log-wait-seconds", type=float, default=env_float("BENCH_CACHE_READ_PATH_LOG_WAIT_SECONDS", 1))
    parser.add_argument("--min-hot-mbps", type=float, default=env_float("BENCH_CACHE_MIN_HOT_MBPS", 0))
    parser.add_argument(
        "--min-hot-file-read-mbps",
        type=float,
        default=env_float("BENCH_CACHE_MIN_HOT_FILE_READ_MBPS", 0),
    )
    parser.add_argument(
        "--min-hot-file-read-size-mb",
        type=int,
        default=env_int("BENCH_CACHE_MIN_HOT_FILE_READ_SIZE_MB", 0),
    )
    parser.add_argument("--min-remote-cache-mbps", type=float, default=env_float("BENCH_CACHE_MIN_REMOTE_MBPS", 0))
    parser.add_argument(
        "--min-remote-cache-socket-mbps",
        type=float,
        default=env_float("BENCH_CACHE_MIN_REMOTE_SOCKET_MBPS", 0),
    )
    parser.add_argument(
        "--min-remote-cache-socket-size-mb",
        type=int,
        default=env_int("BENCH_CACHE_MIN_REMOTE_SOCKET_SIZE_MB", 0),
    )
    parser.add_argument("--log-tail", type=int, default=env_int("BENCH_CACHE_LOG_TAIL", 30000))
    parser.add_argument("--sandbox-cpu", default=os.getenv("BENCH_CACHE_SANDBOX_CPU", os.getenv("BENCH_SANDBOX_CPU", "0.5")))
    parser.add_argument("--sandbox-memory", default=os.getenv("BENCH_CACHE_SANDBOX_MEMORY", os.getenv("BENCH_SANDBOX_MEMORY", "512")))
    parser.add_argument("--sandbox-keep-warm-seconds", type=int, default=env_int("BENCH_CACHE_KEEP_WARM_SECONDS", 1800))
    parser.add_argument("--sync-workspace-marker", dest="sync_workspace_marker", action="store_true")
    parser.add_argument("--no-sync-workspace-marker", dest="sync_workspace_marker", action="store_false")
    parser.set_defaults(sync_workspace_marker=env_bool("BENCH_CACHE_SYNC_WORKSPACE_MARKER", False))
    parser.add_argument("--sandbox-create-retries", type=int, default=env_int("BENCH_CACHE_CREATE_RETRIES", 20))
    parser.add_argument("--sandbox-ready-timeout-seconds", type=float, default=env_float("BENCH_CACHE_READY_TIMEOUT_SECONDS", 600))
    parser.add_argument("--sandbox-ready-retry-ms", type=int, default=env_int("BENCH_CACHE_READY_RETRY_MS", 500))
    parser.add_argument("--exec-timeout-seconds", type=float, default=env_float("BENCH_CACHE_EXEC_TIMEOUT_SECONDS", 900))

    parser.add_argument("--install", dest="install", action="store_true")
    parser.add_argument("--no-install", dest="install", action="store_false")
    parser.set_defaults(install=env_bool("BENCH_INSTALL", True))
    parser.add_argument("--port-forward", dest="port_forward", action="store_true")
    parser.add_argument("--no-port-forward", dest="port_forward", action="store_false")
    parser.set_defaults(port_forward=env_bool("BENCH_PORT_FORWARD", True))
    parser.add_argument("--localstack-port-forward", dest="localstack_port_forward", action="store_true")
    parser.add_argument("--no-localstack-port-forward", dest="localstack_port_forward", action="store_false")
    parser.set_defaults(localstack_port_forward=env_bool("BENCH_CACHE_LOCALSTACK_PORT_FORWARD", True))
    parser.add_argument("--reset-workers", dest="reset_workers", action="store_true")
    parser.add_argument("--no-reset-workers", dest="reset_workers", action="store_false")
    parser.set_defaults(reset_workers=env_bool("BENCH_RESET_WORKERS", False))
    parser.add_argument("--reset-workers-after-prepare", dest="reset_workers_after_prepare", action="store_true")
    parser.add_argument("--no-reset-workers-after-prepare", dest="reset_workers_after_prepare", action="store_false")
    parser.set_defaults(
        reset_workers_after_prepare=env_bool("BENCH_CACHE_RESET_WORKERS_AFTER_PREPARE", False)
    )
    parser.add_argument("--reset-workers-before-hot", dest="reset_workers_before_hot", action="store_true")
    parser.add_argument("--no-reset-workers-before-hot", dest="reset_workers_before_hot", action="store_false")
    parser.set_defaults(reset_workers_before_hot=env_bool("BENCH_CACHE_RESET_WORKERS_BEFORE_HOT", False))
    parser.add_argument("--reset-workers-before-image-hot", dest="reset_workers_before_image_hot", action="store_true")
    parser.add_argument("--no-reset-workers-before-image-hot", dest="reset_workers_before_image_hot", action="store_false")
    parser.set_defaults(
        reset_workers_before_image_hot=env_bool("BENCH_CACHE_RESET_WORKERS_BEFORE_IMAGE_HOT", False)
    )
    parser.add_argument("--evict-cache-pages-before-hot", dest="evict_cache_pages_before_hot", action="store_true")
    parser.add_argument("--no-evict-cache-pages-before-hot", dest="evict_cache_pages_before_hot", action="store_false")
    parser.set_defaults(
        evict_cache_pages_before_hot=env_bool("BENCH_CACHE_EVICT_PAGES_BEFORE_HOT", False)
    )
    parser.add_argument("--evict-mounted-file-before-hot", dest="evict_mounted_file_before_hot", action="store_true")
    parser.add_argument(
        "--no-evict-mounted-file-before-hot",
        dest="evict_mounted_file_before_hot",
        action="store_false",
    )
    parser.set_defaults(
        evict_mounted_file_before_hot=env_bool("BENCH_CACHE_EVICT_MOUNTED_FILE_BEFORE_HOT", False)
    )
    parser.add_argument(
        "--evict-cache-pages-before-remote-proof",
        dest="evict_cache_pages_before_remote_proof",
        action="store_true",
    )
    parser.add_argument(
        "--no-evict-cache-pages-before-remote-proof",
        dest="evict_cache_pages_before_remote_proof",
        action="store_false",
    )
    parser.set_defaults(
        evict_cache_pages_before_remote_proof=env_bool(
            "BENCH_CACHE_EVICT_PAGES_BEFORE_REMOTE_PROOF",
            False,
        )
    )
    parser.add_argument("--require-cache-page-eviction", dest="require_cache_page_eviction", action="store_true")
    parser.add_argument("--no-require-cache-page-eviction", dest="require_cache_page_eviction", action="store_false")
    parser.set_defaults(
        require_cache_page_eviction=env_bool("BENCH_CACHE_REQUIRE_PAGE_EVICTION", False)
    )
    parser.add_argument("--wait-cache-ready-after-prepare", dest="wait_cache_ready_after_prepare", action="store_true")
    parser.add_argument("--no-wait-cache-ready-after-prepare", dest="wait_cache_ready_after_prepare", action="store_false")
    parser.set_defaults(
        wait_cache_ready_after_prepare=env_bool("BENCH_CACHE_WAIT_CACHE_READY_AFTER_PREPARE", True)
    )
    parser.add_argument("--wait-storage-ready-after-prepare", dest="wait_storage_ready_after_prepare", action="store_true")
    parser.add_argument(
        "--no-wait-storage-ready-after-prepare",
        dest="wait_storage_ready_after_prepare",
        action="store_false",
    )
    parser.set_defaults(
        wait_storage_ready_after_prepare=env_bool("BENCH_CACHE_WAIT_STORAGE_READY_AFTER_PREPARE", True)
    )
    parser.add_argument("--wait-running", dest="wait_running", action="store_true")
    parser.add_argument("--no-wait-running", dest="wait_running", action="store_false")
    parser.set_defaults(wait_running=env_bool("BENCH_CACHE_WAIT_RUNNING", True))
    parser.add_argument("--require-workspace-storage", dest="require_workspace_storage", action="store_true")
    parser.add_argument("--no-require-workspace-storage", dest="require_workspace_storage", action="store_false")
    parser.set_defaults(require_workspace_storage=env_bool("BENCH_CACHE_REQUIRE_WORKSPACE_STORAGE", True))
    parser.add_argument("--require-verified-reads", dest="require_verified_reads", action="store_true")
    parser.add_argument("--no-require-verified-reads", dest="require_verified_reads", action="store_false")
    parser.set_defaults(require_verified_reads=env_bool("BENCH_CACHE_REQUIRE_VERIFIED_READS", False))
    parser.add_argument("--require-remote-cache-read", dest="require_remote_cache_read", action="store_true")
    parser.add_argument("--no-require-remote-cache-read", dest="require_remote_cache_read", action="store_false")
    parser.set_defaults(require_remote_cache_read=env_bool("BENCH_CACHE_REQUIRE_REMOTE_READ", False))
    parser.add_argument("--require-read-path-proof", dest="require_read_path_proof", action="store_true")
    parser.add_argument("--no-require-read-path-proof", dest="require_read_path_proof", action="store_false")
    parser.set_defaults(require_read_path_proof=env_bool("BENCH_CACHE_REQUIRE_READ_PATH_PROOF", True))

    args = parser.parse_args()
    if args.strict_disk_cache_hit:
        args.evict_cache_pages_before_hot = True
        args.evict_mounted_file_before_hot = True
        args.evict_cache_pages_before_remote_proof = True
        args.require_cache_page_eviction = True
        args.wait_cache_ready_after_prepare = True
        args.read_path_log_wait_seconds = max(args.read_path_log_wait_seconds, 6)
        args.reset_workers_before_image_hot = True
    if args.preset == "smoke" and args.sizes_mb == "32,128":
        args.sizes_mb = "32"
    valid_patterns = {"sequential", "random"}
    valid_access_types = {"image_archive", "volume_mount", "workspace_fuse"}
    args.file_specs = parse_file_plan(args.file_plan, valid_access_types, valid_patterns)
    if args.file_specs:
        args.sizes = ordered_unique(spec["sizeMiB"] for spec in args.file_specs)
        args.pattern_list = ordered_unique(spec["pattern"] for spec in args.file_specs)
        args.access_type_list = ordered_unique(spec["accessType"] for spec in args.file_specs)
    else:
        args.sizes = parse_sizes(args.sizes_mb)
        args.pattern_list = parse_csv(args.patterns)
        args.access_type_list = parse_csv(args.access_types)
    if unknown := sorted(set(args.pattern_list) - valid_patterns):
        raise SystemExit(f"unknown pattern(s): {', '.join(unknown)}")
    if unknown := sorted(set(args.access_type_list) - valid_access_types):
        raise SystemExit(f"unknown access type(s): {', '.join(unknown)}")
    if args.iterations <= 0:
        raise SystemExit("--iterations must be greater than 0")
    if args.random_block_bytes <= 0:
        raise SystemExit("--random-block-bytes must be greater than 0")
    if args.remote_cache_proof_chunk_bytes <= 0:
        raise SystemExit("--remote-cache-proof-chunk-bytes must be greater than 0")
    if not args.token_cache:
        args.token_cache = f"/tmp/beta9-startup-benchmark-{args.namespace}.token"
    if not args.sdk_config:
        args.sdk_config = f"/tmp/beta9-cache-benchmark-{args.namespace}.ini"
    if not args.image_uri:
        args.image_uri = f"registry.localhost:5000/beta9-cache-bench:{int(time.time())}"
    if not args.volume_subdir:
        args.volume_subdir = f"run-{int(time.time())}-{random.getrandbits(32):08x}"
    args.volume_mount_path = args.volume_mount_path.strip("/")
    if args.generate_volume_payload is None:
        args.generate_volume_payload = "image_archive" not in args.access_type_list
    if args.generate_volume_payload and "image_archive" in args.access_type_list:
        raise SystemExit("--generate-volume-payload cannot be used with image_archive access types")
    return args


def start_localstack_port_forward_if_needed(args):
    if not args.localstack_port_forward or not gateway_host_is_local(args.gateway_url):
        return None
    if tcp_reachable("127.0.0.1", args.localstack_local_port):
        log(f"LocalStack is already reachable at 127.0.0.1:{args.localstack_local_port}")
        return None

    proc = subprocess.Popen(
        [
            "kubectl",
            "-n",
            args.namespace,
            "port-forward",
            f"svc/{args.localstack_service}",
            f"{args.localstack_local_port}:{args.localstack_service_port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def stop_port_forward():
        if proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    atexit.register(stop_port_forward)
    deadline = time.monotonic() + args.timeout_seconds
    while time.monotonic() < deadline:
        if tcp_reachable("127.0.0.1", args.localstack_local_port):
            log(f"Started LocalStack port-forward on localhost:{args.localstack_local_port}")
            return proc
        if proc.poll() is not None:
            output = proc.stdout.read() if proc.stdout is not None else ""
            raise RuntimeError(f"LocalStack port-forward exited before becoming reachable. {output}")
        time.sleep(0.1)
    stop_port_forward()
    raise RuntimeError("LocalStack port-forward did not become reachable")


def tcp_reachable(host, port):
    import socket

    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except OSError:
        return False


def host_image_uri(image_uri):
    registry_prefixes = ("registry.localhost:5000/", "k3d-registry.localhost:5000/")
    if image_uri.startswith(registry_prefixes):
        return "localhost:5001/" + image_uri.split("/", 1)[1]
    return image_uri


def registry_image_parts(image_uri):
    if "/" not in image_uri:
        raise RuntimeError(f"image URI does not include a registry/repository: {image_uri}")
    registry, remainder = image_uri.split("/", 1)
    if "@" in remainder:
        repo, ref = remainder.rsplit("@", 1)
    else:
        repo, ref = remainder.rsplit(":", 1)
    return registry, repo, ref


def local_registry_base_url(registry):
    if registry in {"localhost:5001", "127.0.0.1:5001"}:
        return "http://127.0.0.1:5001"
    return ""


MANIFEST_ACCEPT = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]
)


def registry_get_json(base_url, repo, reference):
    url = f"{base_url}/v2/{repo}/manifests/{reference}"
    req = urllib.request.Request(url, headers={"Accept": MANIFEST_ACCEPT})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


def stream_registry_blob(base_url, repo, digest, expected_size):
    url = f"{base_url}/v2/{repo}/blobs/{digest}"
    req = urllib.request.Request(url)
    total = 0
    with urllib.request.urlopen(req, timeout=60) as response:
        header_size = response.headers.get("Content-Length")
        while True:
            chunk = response.read(4 * 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
    if expected_size is not None and total != expected_size:
        raise RuntimeError(
            f"registry blob {digest} is incomplete: expected {expected_size} bytes, read {total}"
        )
    if header_size and total != int(header_size):
        raise RuntimeError(
            f"registry blob {digest} Content-Length mismatch: expected {header_size} bytes, read {total}"
        )
    return total


def verify_registry_image_blobs(image_uri):
    registry, repo, ref = registry_image_parts(image_uri)
    base_url = local_registry_base_url(registry)
    if not base_url:
        log(f"Skipping registry blob verification for non-local registry {registry}")
        return {"verified": False, "blobs": 0, "bytes": 0}

    seen_manifests = set()
    seen_blobs = {}

    def verify_manifest(reference):
        if reference in seen_manifests:
            return
        seen_manifests.add(reference)
        manifest = registry_get_json(base_url, repo, reference)
        media_type = manifest.get("mediaType", "")
        if media_type.endswith("manifest.list.v2+json") or media_type.endswith("image.index.v1+json"):
            for child in manifest.get("manifests", []):
                platform = child.get("platform") or {}
                if platform.get("os") == "unknown" or platform.get("architecture") == "unknown":
                    continue
                verify_manifest(child["digest"])
            return

        descriptors = []
        if manifest.get("config"):
            descriptors.append(manifest["config"])
        descriptors.extend(manifest.get("layers") or [])
        for descriptor in descriptors:
            digest = descriptor["digest"]
            if digest in seen_blobs:
                continue
            expected_size = descriptor.get("size")
            seen_blobs[digest] = stream_registry_blob(base_url, repo, digest, expected_size)

    verify_manifest(ref)
    total_bytes = sum(seen_blobs.values())
    log(
        "Verified registry image blobs for "
        f"{image_uri}: blobs={len(seen_blobs)} bytes={total_bytes}"
    )
    return {"verified": True, "blobs": len(seen_blobs), "bytes": total_bytes}


def publish_cache_benchmark_image(build_uri, timeout_seconds):
    if shutil.which("skopeo"):
        log(f"Publishing cache benchmark image {build_uri} with skopeo")
        run(
            [
                "skopeo",
                "copy",
                "--dest-tls-verify=false",
                f"docker-daemon:{build_uri}",
                f"docker://{build_uri}",
            ],
            timeout=timeout_seconds,
        )
        return

    log(f"Pushing cache benchmark image {build_uri}")
    run(["docker", "push", build_uri], timeout=timeout_seconds)


def payload_sha256(nonce, label, size):
    hasher = hashlib.sha256()
    offset = 0
    chunk_size = 1024 * 1024
    while offset < size:
        length = min(chunk_size, size - offset)
        hasher.update(deterministic_payload_range(nonce, label, offset, length))
        offset += length
    return hasher.hexdigest()


def expected_manifest(nonce, sizes, access_types, patterns, file_specs=None):
    files = []
    specs = file_specs or [
        {"accessType": access_type, "pattern": pattern, "sizeMiB": size_mb}
        for access_type in access_types
        for pattern in patterns
        for size_mb in sizes
    ]
    for spec in specs:
        access_type = spec["accessType"]
        pattern = spec["pattern"]
        size_mb = spec["sizeMiB"]
        size = size_mb * 1024 * 1024
        label = f"{access_type}:{pattern}:size:{size_mb}mb"
        files.append(
            {
                "path": f"files/{access_type}/{pattern}/{size_mb}mb.bin",
                "accessType": access_type,
                "pattern": pattern,
                "size": size,
                "sizeMiB": size_mb,
                "sha256": payload_sha256(nonce, label, size),
                "label": label,
            }
        )
    return {"version": 3, "nonce": nonce, "files": files}


def build_payload_image(args):
    if args.generate_volume_payload and "image_archive" not in args.access_type_list:
        nonce = f"{datetime.now(timezone.utc).isoformat()}:{random.getrandbits(64)}"
        manifest = expected_manifest(nonce, args.sizes, args.access_type_list, args.pattern_list, args.file_specs)
        log(f"Generating volume benchmark payload in mounted Volume with {len(manifest['files'])} planned files")
        return {"imageUri": "", "hostImageUri": "", "nonce": nonce, "manifest": manifest}

    require_tools("docker")
    build_uri = host_image_uri(args.image_uri)
    nonce = f"{datetime.now(timezone.utc).isoformat()}:{random.getrandbits(64)}"
    manifest = expected_manifest(nonce, args.sizes, args.access_type_list, args.pattern_list, args.file_specs)
    with tempfile.TemporaryDirectory(prefix="beta9-cache-bench-image-") as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "generate_payload.py").write_text(PAYLOAD_GENERATOR, encoding="utf-8")
        (tmp_path / "Dockerfile").write_text(
            "\n".join(
                [
                    "ARG BASE_IMAGE",
                    "FROM ${BASE_IMAGE}",
                    "ARG SIZES_MB",
                    "ARG ACCESS_TYPES",
                    "ARG PATTERNS",
                    "ARG FILE_PLAN",
                    "ARG BENCH_NONCE",
                    "ENV CACHE_BENCH_SIZES_MB=${SIZES_MB}",
                    "ENV CACHE_BENCH_ACCESS_TYPES=${ACCESS_TYPES}",
                    "ENV CACHE_BENCH_PATTERNS=${PATTERNS}",
                    "ENV CACHE_BENCH_FILE_PLAN=${FILE_PLAN}",
                    "ENV CACHE_BENCH_NONCE=${BENCH_NONCE}",
                    "COPY generate_payload.py /tmp/generate_payload.py",
                    "RUN python3 /tmp/generate_payload.py && rm -f /tmp/generate_payload.py",
                    "WORKDIR /",
                    'CMD ["python3", "-c", "import time; time.sleep(3600)"]',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        if args.file_specs:
            log(f"Building cache benchmark image {build_uri} with {len(args.file_specs)} planned files")
        else:
            log(f"Building cache benchmark image {build_uri} with sizes {args.sizes_mb} MiB")
        build_cmd = ["docker"]
        use_buildx = "," in args.image_platform
        if use_buildx:
            build_cmd.extend(["buildx", "build", "--push"])
        else:
            build_cmd.append("build")
        build_cmd.extend(
            [
                "-t",
                build_uri,
                "--build-arg",
                f"BASE_IMAGE={args.source_image}",
                "--build-arg",
                f"SIZES_MB={','.join(str(size) for size in args.sizes)}",
                "--build-arg",
                f"ACCESS_TYPES={','.join(args.access_type_list)}",
                "--build-arg",
                f"PATTERNS={','.join(args.pattern_list)}",
                "--build-arg",
                f"FILE_PLAN={args.file_plan}",
                "--build-arg",
                f"BENCH_NONCE={nonce}",
            ]
        )
        if args.image_platform:
            build_cmd.extend(["--platform", args.image_platform])
        build_cmd.append(str(tmp_path))
        run(build_cmd, timeout=max(600, args.timeout_seconds))
        if not use_buildx:
            publish_cache_benchmark_image(build_uri, max(600, args.timeout_seconds))
        if args.verify_registry_blobs:
            verify_registry_image_blobs(build_uri)
    return {"imageUri": args.image_uri, "hostImageUri": build_uri, "nonce": nonce, "manifest": manifest}


def authorize_benchmark(args):
    try:
        return authorize(args.gateway_url, args.token)
    except RuntimeError as exc:
        if not args.token or "Invalid token" not in str(exc):
            raise
        log("Cached benchmark token is invalid; retrying local authorization without it")
        if args.token_cache:
            Path(args.token_cache).unlink(missing_ok=True)
        args.token = ""
        return authorize(args.gateway_url, "")


def verify_workspace_storage(args, token):
    status, body, raw, _ = http_json(
        "GET",
        api_url(args.gateway_url, "/api/v1/workspace/current"),
        token=token,
        timeout=10,
    )
    if status >= 400:
        raise RuntimeError(f"workspace storage check failed with HTTP {status}: {raw[:300]}")
    storage_id = body.get("storage_id") or body.get("storageId")
    storage = body.get("storage") or {}
    storage_available = bool(storage_id) or bool(storage.get("id"))
    if not storage_available and args.require_workspace_storage:
        raise RuntimeError("workspace storage is required for cache benchmark")
    return body


def volume_container_root(args):
    return str(Path(VOLUME_CONTAINER_PREFIX) / args.volume_mount_path / args.volume_subdir)


def workspace_name(workspace):
    for key in ("name", "workspace_name", "workspaceName"):
        value = workspace.get(key)
        if value:
            return value
    raise RuntimeError(f"workspace response does not include a filesystem name: {sorted(workspace.keys())}")


def volume_external_id(volume):
    volume_id = getattr(volume, "volume_id", "") or getattr(volume, "id", "")
    if not volume_id:
        raise RuntimeError("volume external id was not populated by SDK runtime preparation")
    return volume_id


def workspace_fuse_root(args, workspace, volume_id):
    return str(Path("/workspace/data") / workspace_name(workspace) / "volumes" / volume_id / args.volume_subdir)


def worker_volume_root(args, workspace, volume_id):
    return str(Path("/data/volumes") / workspace_name(workspace) / volume_id / args.volume_subdir)


def prepare_sandbox_runtime(args):
    Image, Sandbox, sandbox_stub_type = import_sdk()
    from beta9 import Volume  # noqa: WPS433

    if args.generate_volume_payload:
        image = Image(python_version=args.runtime_python_version).with_envs(
            {"BETA9_CACHE_BENCH_RUN": os.getenv("BENCH_CACHE_RUNTIME_MARKER", str(time.time_ns()))}
        )
    else:
        image = Image.from_registry(args.image_uri)
    volume = Volume(name=args.volume_name, mount_path=args.volume_mount_path)
    sandbox = Sandbox(
        name="cache-benchmark",
        image=image,
        cpu=parse_sdk_cpu(args.sandbox_cpu),
        memory=parse_sdk_memory(args.sandbox_memory),
        keep_warm_seconds=args.sandbox_keep_warm_seconds,
        authorized=False,
        volumes=[volume],
        sync_local_dir=False,
    )

    if args.sync_workspace_marker:
        from beta9.sync import FileSyncer  # noqa: WPS433

        sync_root = Path(tempfile.mkdtemp(prefix="beta9-cache-benchmark-sync-"))
        (sync_root / "cache-benchmark.json").write_text(
            json.dumps({"createdAt": now_rfc3339(), "imageUri": args.image_uri}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        sandbox.syncer = FileSyncer(sandbox.gateway_stub, root_dir=str(sync_root))
        sandbox.sync_local_dir = True
        sandbox._benchmark_sync_root = str(sync_root)
    sandbox.entrypoint = ["tail", "-f", "/dev/null"]

    started = time.monotonic_ns()
    ok = False
    attempts = max(1, args.runtime_prepare_attempts)
    for attempt in range(1, attempts + 1):
        ok = sandbox.prepare_runtime(
            stub_type=sandbox_stub_type,
            force_create_stub=True,
            ignore_patterns=None if args.sync_workspace_marker else ["*"],
        )
        if ok:
            break
        if attempt < attempts:
            log(f"SDK Sandbox runtime preparation failed; retrying ({attempt}/{attempts})")
            time.sleep(min(10, attempt * 2))
    prepare_ms = (time.monotonic_ns() - started) / 1_000_000
    if not ok:
        raise RuntimeError(f"SDK Sandbox runtime preparation failed after {attempts} attempts")
    return sandbox, volume, prepare_ms


def parse_json_output(stdout):
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise RuntimeError(f"command did not emit JSON: {stdout[-500:]}")


def wait_process_manager(args, instance, request_start_ns, sample):
    deadline = time.monotonic() + args.sandbox_ready_timeout_seconds
    attempts = 0
    errors = []
    while True:
        attempts += 1
        try:
            started = time.monotonic_ns()
            process = instance.process.exec(["true"], cwd="/")
            accepted = time.monotonic_ns()
            sample["processReadyMs"] = (accepted - request_start_ns) / 1_000_000
            sample["readinessExecAcceptedMs"] = (accepted - started) / 1_000_000
            sample["readinessExecAttempts"] = attempts
            sample["readinessExecErrors"] = errors
            process.wait(timeout=5)
            return
        except Exception as exc:
            errors.append(str(exc))
            if time.monotonic() >= deadline:
                raise RuntimeError(f"process manager did not become ready; last error: {exc}")
            time.sleep(args.sandbox_ready_retry_ms / 1000)


def run_instance_command(args, instance, command, request_start_ns, sample):
    wait_process_manager(args, instance, request_start_ns, sample)
    started = time.monotonic_ns()
    process = instance.process.exec(command, cwd="/")
    accepted = time.monotonic_ns()
    sample["execAcceptedMs"] = (accepted - started) / 1_000_000
    sample["execPid"] = process.pid
    exit_code = process.wait(timeout=args.exec_timeout_seconds)
    done = time.monotonic_ns()
    sample["execCompleteMs"] = (done - request_start_ns) / 1_000_000
    sample["execExitCode"] = exit_code
    sample["stdout"] = process.stdout.read()
    sample["stderr"] = process.stderr.read()
    if exit_code != 0:
        raise RuntimeError(f"command exited {exit_code}: {sample['stderr'] or sample['stdout']}")
    return parse_json_output(sample["stdout"])


def create_sample(args, sandbox, token, sample):
    instance, create_info = create_sandbox_with_retries(args, sandbox)
    sample.update(create_info)
    sample["containerId"] = instance.container_id
    sample["stubId"] = instance.stub_id
    if not instance.ok:
        raise RuntimeError(instance.error_msg or "Sandbox.create returned ok=false")
    running = wait_running(args, token, instance.container_id, sample["_requestStartNs"])
    if running:
        sample["runningObservedMs"] = running["observedMs"]
        sample["runningAttempts"] = running["attempts"]
        sample["containerState"] = running["state"]
    return instance


def read_command(args, root, entry, pattern, seed):
    total_bytes = entry["size"] if pattern == "random" else 0
    command = [
        "python3",
        "-c",
        READ_HARNESS,
        "--root",
        root,
        "--file",
        entry["path"],
        "--pattern",
        pattern,
        "--random-block-bytes",
        str(args.random_block_bytes),
        "--random-total-bytes",
        str(total_bytes),
        "--seed",
        str(seed),
    ]
    if not args.verify_reads:
        command.append("--skip-verify")
    return command


def run_sandbox_read(args, sandbox, token, access_type, root, entry, pattern, cache_state, index, geesefs_log_path=None):
    instance = None
    log_since = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": access_type,
        "pattern": pattern,
        "cacheState": cache_state,
        "filePath": entry["path"],
        "fileSizeBytes": entry["size"],
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        accepted = time.monotonic_ns()
        sample["acceptedMs"] = (accepted - started) / 1_000_000
        log_since = now_rfc3339()
        payload = run_instance_command(
            args,
            instance,
            read_command(args, root, entry, pattern, args.random_seed + index),
            started,
            sample,
        )
        sample.update(read_result_fields(payload))
        if geesefs_log_path:
            sample["geesefsLogPath"] = geesefs_log_path
            attach_read_path_diagnostics(args, sample, log_since, geesefs_log_path, entry["sha256"])
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        if access_type == "image_archive" and log_since:
            attach_clip_read_diagnostics(
                args,
                sample,
                log_since,
                image_id=getattr(sandbox, "image_id", ""),
                container_id=sample.get("containerId", ""),
            )
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def wait_for_staged_writes_to_drain(args, volume_id):
    deadline = time.monotonic() + args.cache_proof_timeout_seconds
    attempts = 0
    last = {"drained": False}
    while True:
        attempts += 1
        last = staged_write_status(args, volume_id)
        last["attempts"] = attempts
        if last.get("drained") or time.monotonic() >= deadline:
            return last
        time.sleep(min(5, max(1, args.settle_seconds)))


def run_volume_prepare(args, sandbox, token, index, volume_id=None):
    instance = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": "volume_prepare",
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        sample["acceptedMs"] = (time.monotonic_ns() - started) / 1_000_000
        command = [
            "python3",
            "-c",
            VOLUME_PREPARE_SCRIPT,
            "--dest",
            volume_container_root(args),
            "--access-types",
            ",".join(access_type for access_type in args.access_type_list if access_type != "image_archive"),
        ]
        if args.generate_volume_payload:
            command.extend(["--generate", "--manifest-json", json.dumps(args.payload_manifest, sort_keys=True)])
        else:
            command.extend(["--source", IMAGE_READ_ROOT])
        if args.strict_disk_cache_hit:
            command.append("--skip-dest-verify")
        payload = run_instance_command(
            args,
            instance,
            command,
            started,
            sample,
        )
        sample.update(read_result_fields(payload))
        sample["manifest"] = payload["manifest"]
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
        if volume_id:
            staged_drain = wait_for_staged_writes_to_drain(args, volume_id)
            sample["stagedWriteDrainBeforeCleanup"] = staged_drain
            if not staged_drain.get("drained"):
                sample["ok"] = False
                sample["status"] = "failed"
                sample["error"] = f"staged writes did not drain before prepare cleanup: {staged_drain}"
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def storage_ready_command(args, manifest, access_types):
    return [
        "python3",
        "-c",
        STORAGE_READY_HARNESS,
        "--root",
        volume_container_root(args),
        "--manifest-json",
        json.dumps(manifest, sort_keys=True),
        "--access-types",
        ",".join(sorted(access_types)),
    ]


def required_storage_probe_workers(args):
    pods = [
        pod for pod in worker_pods(args.namespace)
        if pod.get("phase") == "Running" and pod.get("name", "").startswith("worker-default-")
    ]
    requested = max(1, int(getattr(args, "storage_ready_worker_count", 1) or 1))
    return min(requested, max(1, len(pods)))


def staged_write_status(args, volume_id):
    run_root = str(Path("/tmp/geesefs-staged-write") / "volumes" / volume_id / args.volume_subdir)
    quoted_root = shlex.quote(run_root)
    script = (
        f"root={quoted_root}; "
        "if [ ! -e \"$root\" ]; then printf '{\"drained\":true,\"files\":0,\"bytes\":0}\\n'; exit 0; fi; "
        "files=$(find \"$root\" -type f 2>/dev/null | wc -l | tr -d ' '); "
        "bytes=$(find \"$root\" -type f -printf '%s\\n' 2>/dev/null | awk '{s+=$1} END {printf \"%d\", s+0}'); "
        "printf '{\"drained\":%s,\"files\":%s,\"bytes\":%s,\"root\":\"%s\"}\\n' "
        "\"$([ \"$files\" = 0 ] && echo true || echo false)\" \"$files\" \"$bytes\" \"$root\""
    )
    statuses = []
    drained = True
    for pod in worker_pods(args.namespace):
        if pod.get("phase") != "Running":
            continue
        proc = worker_shell(args.namespace, pod["name"], script, timeout=30)
        proof = {
            "pod": pod["name"],
            "nodeName": pod.get("nodeName", ""),
            "returnCode": proc.returncode,
            "stdout": proc.stdout[-1000:],
            "stderr": proc.stderr[-1000:],
        }
        try:
            payload = parse_json_output(proc.stdout)
            proof.update(payload)
        except Exception:
            proof["drained"] = False
        statuses.append(proof)
        drained = drained and bool(proof.get("drained"))
    return {"drained": drained, "root": run_root, "pods": statuses}


def storage_ready_worker_command(root, manifest, access_types):
    manifest_b64 = base64.b64encode(json.dumps(manifest, sort_keys=True).encode()).decode()
    access_types_csv = ",".join(sorted(access_types))
    return (
        f"ROOT={shlex.quote(root)} "
        f"MANIFEST_B64={shlex.quote(manifest_b64)} "
        f"ACCESS_TYPES={shlex.quote(access_types_csv)} "
        "python3 - <<'PY'\n"
        "import base64\n"
        "import json\n"
        "import os\n"
        "import time\n"
        "from pathlib import Path\n"
        "\n"
        "root = Path(os.environ['ROOT'])\n"
        "manifest = json.loads(base64.b64decode(os.environ['MANIFEST_B64']).decode())\n"
        "access_types = {part for part in os.environ.get('ACCESS_TYPES', '').split(',') if part}\n"
        "selected = [entry for entry in manifest.get('files', []) if not access_types or entry.get('accessType') in access_types]\n"
        "started = time.monotonic_ns()\n"
        "missing = []\n"
        "wrong_size = []\n"
        "bytes_seen = 0\n"
        "for entry in selected:\n"
        "    path = root / entry['path']\n"
        "    try:\n"
        "        stat = path.stat()\n"
        "    except FileNotFoundError:\n"
        "        missing.append(entry['path'])\n"
        "        continue\n"
        "    expected_size = int(entry['size'])\n"
        "    if stat.st_size != expected_size:\n"
        "        wrong_size.append({'path': entry['path'], 'size': expected_size, 'actual': stat.st_size})\n"
        "    bytes_seen += stat.st_size\n"
        "ready = not missing and not wrong_size\n"
        "print(json.dumps({\n"
        "    'ready': ready,\n"
        "    'root': str(root),\n"
        "    'checked': len(selected) - len(missing),\n"
        "    'expected': len(selected),\n"
        "    'bytes': bytes_seen,\n"
        "    'missing': missing[:10],\n"
        "    'wrongSize': wrong_size[:10],\n"
        "    'durationMs': (time.monotonic_ns() - started) / 1_000_000,\n"
        "}, sort_keys=True))\n"
        "PY"
    )


def direct_worker_storage_probes(args, workspace, volume_id, manifest, access_types):
    root = workspace_fuse_root(args, workspace, volume_id)
    probes = []
    ready = {}
    for pod in worker_pods(args.namespace):
        if pod.get("phase") != "Running" or not pod.get("name", "").startswith("worker-default-"):
            continue
        proc = worker_shell(
            args.namespace,
            pod["name"],
            storage_ready_worker_command(root, manifest, access_types),
            timeout=120,
        )
        proof = {
            "workerPod": pod["name"],
            "workerNode": pod.get("nodeName", ""),
            "root": root,
            "returnCode": proc.returncode,
            "stdout": proc.stdout[-2000:],
            "stderr": proc.stderr[-2000:],
        }
        try:
            payload = parse_json_output(proc.stdout)
            proof.update(payload)
        except Exception as exc:
            proof["ready"] = False
            proof["error"] = str(exc)
        probes.append(proof)
        if proof.get("ready"):
            ready[pod["name"]] = proof
    return {
        "readyWorkers": sorted(ready),
        "probes": probes,
        "root": root,
    }


def wait_storage_root_ready(args, sandbox, token, workspace, volume_id, manifest, access_types):
    deadline = time.monotonic() + args.cache_proof_timeout_seconds
    attempts = 0
    last = {"ready": False, "root": volume_container_root(args)}
    required_workers = required_storage_probe_workers(args)
    ready_probes_by_pod = {}
    while True:
        attempts += 1
        staged = staged_write_status(args, volume_id)
        if not staged.get("drained"):
            last = {
                "ready": False,
                "attempts": attempts,
                "requiredWorkers": required_workers,
                "readyWorkers": sorted(ready_probes_by_pod),
                "root": volume_container_root(args),
                "stagedWrites": staged,
            }
            if time.monotonic() >= deadline:
                return last
            time.sleep(min(5, max(1, args.settle_seconds)))
            continue

        worker_probes = direct_worker_storage_probes(args, workspace, volume_id, manifest, access_types)
        if len(worker_probes.get("readyWorkers") or []) >= required_workers:
            return {
                "ready": True,
                "attempts": attempts,
                "requiredWorkers": required_workers,
                "readyWorkers": worker_probes.get("readyWorkers") or [],
                "root": worker_probes.get("root") or volume_container_root(args),
                "stagedWrites": staged,
                "probes": worker_probes.get("probes") or [],
                "probeMode": "worker_geesefs_mount",
            }

        instance = None
        started = time.monotonic_ns()
        sample = {
            "_requestStartNs": started,
            "index": 0,
            "accessType": "storage_ready_probe",
            "startedAt": now_rfc3339(),
            "status": "running",
        }
        try:
            instance = create_sample(args, sandbox, token, sample)
            sample["acceptedMs"] = (time.monotonic_ns() - started) / 1_000_000
            payload = run_instance_command(
                args,
                instance,
                storage_ready_command(args, manifest, access_types),
                started,
                sample,
            )
            pod = find_worker_for_container(args, instance.container_id, volume_container_root(args))
            last = {
                "ready": bool(payload.get("ready")),
                "attempts": attempts,
                "requiredWorkers": required_workers,
                "root": volume_container_root(args),
                "stagedWrites": staged,
                "probe": payload,
                "sample": {
                    key: sample.get(key)
                    for key in (
                        "containerId",
                        "stubId",
                        "acceptedMs",
                        "execAcceptedMs",
                        "execCompleteMs",
                        "execExitCode",
                        "totalMs",
                    )
                    if key in sample
                },
            }
            if last["ready"]:
                last["workerPod"] = pod.get("name", "")
                last["workerNode"] = pod.get("nodeName", "")
                ready_probes_by_pod[pod.get("name", "")] = last
                if len(ready_probes_by_pod) >= required_workers:
                    last["readyWorkers"] = sorted(ready_probes_by_pod)
                    last["probes"] = list(ready_probes_by_pod.values())
                    return last
                last["ready"] = False
                last["readyWorkers"] = sorted(ready_probes_by_pod)
        except Exception as exc:
            last = {
                "ready": False,
                "attempts": attempts,
                "requiredWorkers": required_workers,
                "readyWorkers": sorted(ready_probes_by_pod),
                "root": volume_container_root(args),
                "stagedWrites": staged_write_status(args, volume_id),
                "error": str(exc),
            }
        finally:
            cleanup_sandbox(args, instance, sample)
            sample.pop("_requestStartNs", None)

        if time.monotonic() >= deadline:
            return last
        time.sleep(min(5, max(1, args.settle_seconds)))


def read_result_fields(payload):
    return {
        "read": payload,
        "readDurationMs": payload.get("durationMs"),
        "bytesRead": payload.get("bytes"),
        "operations": payload.get("operations") or payload.get("files"),
        "mbps": payload.get("mbps"),
        "fileReadMBps": payload.get("fileReadMBps"),
        "readTiming": payload.get("timing") or {},
        "opsPerSecond": payload.get("opsPerSecond"),
        "digest": payload.get("digest"),
        "expectedSha256": payload.get("expectedSha256"),
        "verified": payload.get("verified"),
    }


def worker_pods(namespace):
    proc = run(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            "run.beam.cloud/role=worker",
            "-o",
            "json",
        ],
        check=False,
        timeout=15,
    )
    if proc.returncode != 0:
        return []
    data = json.loads(proc.stdout)
    pods = []
    for item in data.get("items", []):
        pods.append(
            {
                "name": item.get("metadata", {}).get("name", ""),
                "nodeName": item.get("spec", {}).get("nodeName", ""),
                "podIP": item.get("status", {}).get("podIP", ""),
                "hostIP": item.get("status", {}).get("hostIP", ""),
                "phase": item.get("status", {}).get("phase", ""),
            }
        )
    return pods


def worker_exec(namespace, pod, command, timeout=60):
    return run(
        ["kubectl", "-n", namespace, "exec", pod, "-c", "worker", "--", *command],
        check=False,
        timeout=timeout,
    )


def cleanup_worker_cache_artifacts(args):
    pods = [pod for pod in worker_pods(args.namespace) if pod.get("phase") == "Running"]
    if not pods:
        log("Skipping worker cache cleanup: no running worker pods found")
        return {"ok": False, "error": "no running worker pods"}

    pod = pods[0]["name"]
    script = r"""
set -eu
echo "before"
du -sh /var/lib/beta9/cache /cache /cache/volumes /cache/images 2>/dev/null || true
for pages in /var/lib/beta9/cache/default/*/pages; do
  if [ -d "$pages" ]; then
    find "$pages" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  fi
done
for volume_root in /cache/volumes/*; do
  if [ -d "$volume_root" ]; then
    find "$volume_root" -mindepth 1 -maxdepth 1 -name "run-*" -exec rm -rf {} +
  fi
done
echo "after"
du -sh /var/lib/beta9/cache /cache /cache/volumes /cache/images 2>/dev/null || true
"""
    proc = worker_exec(args.namespace, pod, ["sh", "-lc", script], timeout=180)
    result = {
        "ok": proc.returncode == 0,
        "pod": pod,
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
    }
    if proc.returncode == 0:
        log(f"Cleaned worker cache artifacts on {pod}")
    else:
        log(f"Worker cache cleanup failed on {pod}: {proc.stderr.strip() or proc.stdout.strip()}")
    return result


def find_worker_for_container(args, container_id, root):
    pods = worker_pods(args.namespace)
    for pod in pods:
        proc = run(
            [
                "kubectl",
                "-n",
                args.namespace,
                "logs",
                pod["name"],
                "-c",
                "worker",
                "--since=15m",
                "--tail=3000",
            ],
            check=False,
            timeout=10,
        )
        if proc.returncode == 0 and container_id in proc.stdout:
            return pod

    quoted_root = shlex.quote(root)
    for pod in pods:
        proc = worker_exec(args.namespace, pod["name"], ["sh", "-lc", f"test -f {quoted_root}/manifest.json"])
        if proc.returncode == 0:
            return pod
    raise RuntimeError(f"could not find worker pod for container {container_id}")


def run_workspace_fuse_read(args, sandbox, token, workspace_root, entry, pattern, cache_state, index, geesefs_log_path=None):
    instance = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": "workspace_fuse",
        "pattern": pattern,
        "cacheState": cache_state,
        "filePath": entry["path"],
        "fileSizeBytes": entry["size"],
        "workspaceFuseRoot": workspace_root,
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        sample["acceptedMs"] = (time.monotonic_ns() - started) / 1_000_000
        pod = find_worker_for_container(args, instance.container_id, workspace_root)
        sample["workerPod"] = pod["name"]
        sample["workerNode"] = pod.get("nodeName", "")
        command = read_command(args, workspace_root, entry, pattern, args.random_seed + index)
        log_since = now_rfc3339()
        exec_started = time.monotonic_ns()
        proc = worker_exec(args.namespace, pod["name"], command, timeout=int(args.exec_timeout_seconds) + 30)
        sample["execAcceptedMs"] = (time.monotonic_ns() - exec_started) / 1_000_000
        sample["stdout"] = proc.stdout
        sample["stderr"] = proc.stderr
        sample["execExitCode"] = proc.returncode
        sample["execCompleteMs"] = (time.monotonic_ns() - started) / 1_000_000
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
        payload = parse_json_output(proc.stdout)
        sample.update(read_result_fields(payload))
        if geesefs_log_path:
            sample["geesefsLogPath"] = geesefs_log_path
            attach_read_path_diagnostics(args, sample, log_since, geesefs_log_path, entry["sha256"], pod["name"])
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def run_worker_dd_read(args, sandbox, token, workspace_root, geesefs_log_path, access_type, entry, index):
    instance = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": access_type,
        "pattern": "sequential",
        "cacheState": "dd",
        "filePath": entry["path"],
        "fileSizeBytes": entry["size"],
        "workspaceFuseRoot": workspace_root,
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        sample["acceptedMs"] = (time.monotonic_ns() - started) / 1_000_000
        pod = find_worker_for_container(args, instance.container_id, workspace_root)
        sample["workerPod"] = pod["name"]
        sample["workerPodIP"] = pod.get("podIP", "")
        sample["workerNode"] = pod.get("nodeName", "")
        sample["geesefsLogPath"] = geesefs_log_path
        path = str(Path(workspace_root) / entry["path"])
        command = [
            "python3",
            "-c",
            DD_READ_HARNESS,
            "--path",
            path,
            "--expected-size",
            str(entry["size"]),
            "--block-size",
            args.worker_dd_block_size,
        ]
        log_since = now_rfc3339()
        exec_started = time.monotonic_ns()
        proc = worker_exec(args.namespace, pod["name"], command, timeout=int(args.exec_timeout_seconds) + 30)
        sample["execAcceptedMs"] = (time.monotonic_ns() - exec_started) / 1_000_000
        sample["stdout"] = proc.stdout
        sample["stderr"] = proc.stderr
        sample["execExitCode"] = proc.returncode
        sample["execCompleteMs"] = (time.monotonic_ns() - started) / 1_000_000
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
        payload = parse_json_output(proc.stdout)
        sample["dd"] = payload
        sample["readDurationMs"] = payload.get("durationMs")
        sample["bytesRead"] = payload.get("bytes")
        sample["mbps"] = payload.get("mbps")
        sample["blockSize"] = payload.get("blockSize")
        sample["directIO"] = payload.get("directIO")
        if args.worker_dd_log_wait_seconds > 0:
            time.sleep(args.worker_dd_log_wait_seconds)
        logs = worker_logs_since(args.namespace, pod["name"], log_since, args.log_tail)
        sample["cacheProofLogs"] = {
            "ok": logs.get("ok"),
            "sinceTime": log_since,
            "lineCount": len(logs.get("lines") or []),
            "error": logs.get("error"),
        }
        if logs.get("ok"):
            sample["cacheProof"] = parse_worker_dd_cache_proof(
                logs.get("lines") or [],
                geesefs_log_path,
                entry["sha256"],
            )
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def run_sandbox_dd_read(args, sandbox, token, root, geesefs_log_path, access_type, entry, index):
    instance = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": access_type,
        "pattern": "sequential",
        "cacheState": "sandbox_dd",
        "filePath": entry["path"],
        "fileSizeBytes": entry["size"],
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        sample["acceptedMs"] = (time.monotonic_ns() - started) / 1_000_000
        if geesefs_log_path:
            try:
                pod = find_worker_for_container(args, instance.container_id, root)
                sample["workerPod"] = pod["name"]
                sample["workerPodIP"] = pod.get("podIP", "")
                sample["workerNode"] = pod.get("nodeName", "")
            except Exception as exc:
                sample["workerPodLookupError"] = str(exc)
        path = str(Path(root) / entry["path"])
        command = [
            "python3",
            "-c",
            DD_READ_HARNESS,
            "--path",
            path,
            "--expected-size",
            str(entry["size"]),
            "--block-size",
            args.worker_dd_block_size,
        ]
        log_since = now_rfc3339()
        payload = run_instance_command(args, instance, command, started, sample)
        sample["dd"] = payload
        sample["readDurationMs"] = payload.get("durationMs")
        sample["bytesRead"] = payload.get("bytes")
        sample["mbps"] = payload.get("mbps")
        sample["blockSize"] = payload.get("blockSize")
        sample["directIO"] = payload.get("directIO")
        sample["geesefsLogPath"] = geesefs_log_path
        if args.worker_dd_log_wait_seconds > 0:
            time.sleep(args.worker_dd_log_wait_seconds)
        if geesefs_log_path and sample.get("workerPod"):
            logs = worker_logs_since(args.namespace, sample["workerPod"], log_since, args.log_tail)
            sample["cacheProofLogs"] = {
                "ok": logs.get("ok"),
                "sinceTime": log_since,
                "lineCount": len(logs.get("lines") or []),
                "error": logs.get("error"),
            }
            if logs.get("ok"):
                sample["cacheProof"] = parse_worker_dd_cache_proof(
                    logs.get("lines") or [],
                    geesefs_log_path,
                    entry["sha256"],
                )
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def print_sample(sample):
    status = "ok" if sample.get("ok") else "failed"
    file_read_mbps = sample.get("fileReadMBps")
    file_read_text = f"{file_read_mbps:>9.2f}" if file_read_mbps is not None else " " * 9
    print(
        f"{sample.get('index', 0):>4} "
        f"{sample.get('accessType', '-'):<15} "
        f"{sample.get('pattern', '-'):<10} "
        f"{sample.get('cacheState', '-'):<5} "
        f"{sample.get('fileSizeBytes', 0) // (1024 * 1024):>5}MiB "
        f"{format_ms(sample.get('readDurationMs')):>9}ms "
        f"{(sample.get('mbps') or 0):>9.2f} MB/s "
        f"{file_read_text} MB/s "
        f"{status:>7}",
        flush=True,
    )
    if not sample.get("ok"):
        print(f"       error: {sample.get('error')}", flush=True)


def summarize_samples(samples):
    ok = [sample for sample in samples if sample.get("ok")]
    if not ok:
        return {
            "ok": len(samples) == 0,
            "count": len(samples),
            "okCount": 0,
            "error": samples[0].get("error") if samples else "",
        }
    durations = [sample["readDurationMs"] for sample in ok]
    mbps = [sample["mbps"] for sample in ok]
    file_read_mbps = [sample["fileReadMBps"] for sample in ok if sample.get("fileReadMBps") is not None]
    ops = [sample.get("opsPerSecond") or 0 for sample in ok]
    return {
        "ok": len(ok) == len(samples),
        "count": len(samples),
        "okCount": len(ok),
        "durationMs": percentile(durations, 50),
        "durationMsP95": percentile(durations, 95),
        "mbps": percentile(mbps, 50),
        "mbpsP95": percentile(mbps, 95),
        "fileReadMBps": percentile(file_read_mbps, 50) if file_read_mbps else None,
        "readTiming": ok[0].get("readTiming") or {},
        "bytesRead": ok[0].get("bytesRead"),
        "opsPerSecond": percentile(ops, 50),
        "digest": ok[0].get("digest"),
        "startupMs": ok[0].get("acceptedMs"),
        "readyMs": ok[0].get("runningObservedMs"),
        "processReadyMs": ok[0].get("processReadyMs"),
    }


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * (p / 100)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    weight = rank - low
    return values[low] * (1 - weight) + values[high] * weight


def strip_ansi(value):
    return ANSI_RE.sub("", value)


def cache_operation_from_log_line(line):
    clean = strip_ansi(line)
    operation = None
    for marker in (
        "loaded image archive from embedded content cache",
        "loaded image archive from embedded cachefs",
        "StoreContent[OK]",
        "StoreContent[ACK]",
        "StoreFromContent[OK]",
        "StoreFromContent[ACK]",
        "StoreContentFromSourceWithLock",
        "Store[OK]",
        "Get[OK]",
        "GetContentStream[ACK]",
        "cache_triggered",
    ):
        if marker in clean:
            operation = marker
            break
    if operation is None:
        return None
    return {"operation": operation, "hashes": re.findall(r"\b[a-f0-9]{64}\b", clean), "line": clean[-700:]}


def cache_log_counters(namespace, since_time, log_tail):
    patterns = {
        "storeFromContentOk": "StoreFromContent[OK]",
        "storeFromContentAck": "StoreFromContent[ACK]",
        "storeFromContentErr": "StoreFromContent[ERR]",
        "storeContentAck": "StoreContent[ACK]",
        "storeContentOk": "StoreContent[OK]",
        "storeOk": "Store[OK]",
        "getContentOk": "Get[OK]",
        "getContentStreamAck": "GetContentStream[ACK]",
        "setStoreFromContentLockAck": "SetStoreFromContentLock[ACK]",
        "removeStoreFromContentLockAck": "RemoveStoreFromContentLock[ACK]",
        "refreshStoreFromContentLockAck": "RefreshStoreFromContentLock[ACK]",
        "geesefsCacheTriggered": "cache_triggered",
        "geesefsEventCallback": "geesefs: event callback fired",
        "hostNotFound": "host not found",
        "sourceNotFound": "source not found",
        "unableToPopulate": "unable to populate",
        "deadlineExceeded": "context deadline exceeded",
        "cacheNoHosts": "cache has no available hosts",
    }
    proc = run(
        [
            "kubectl",
            "-n",
            namespace,
            "logs",
            "-l",
            "run.beam.cloud/role=worker",
            "-c",
            "worker",
            "--since-time",
            since_time,
            "--tail",
            str(log_tail),
        ],
        check=False,
        timeout=30,
    )
    counters = {key: 0 for key in patterns}
    if proc.returncode != 0:
        return {"error": proc.stderr.strip() or proc.stdout.strip(), "counters": counters, "operations": []}
    operations = []
    for line in proc.stdout.splitlines():
        for key, pattern in patterns.items():
            if pattern in line:
                counters[key] += 1
        operation = cache_operation_from_log_line(line)
        if operation:
            operations.append(operation)
    return {
        "counters": counters,
        "operations": operations[-500:],
        "imageArchiveLoads": [
            op for op in operations if op["operation"].startswith("loaded image archive from embedded")
        ][-20:],
    }


def worker_logs_since(namespace, pod_name, since_time, log_tail):
    target = [pod_name] if pod_name else ["-l", "run.beam.cloud/role=worker"]
    proc = run(
        [
            "kubectl",
            "-n",
            namespace,
            "logs",
            *target,
            "-c",
            "worker",
            "--since-time",
            since_time,
            "--tail",
            str(log_tail),
        ],
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        return {"ok": False, "error": proc.stderr.strip() or proc.stdout.strip(), "lines": []}
    return {"ok": True, "lines": [strip_ansi(line) for line in proc.stdout.splitlines()]}


def parse_geesefs_summary(line):
    match = GEESEFS_SUMMARY_RE.search(line)
    if match:
        groups = match.groups()
        summary = {
            "handlerCount": int(groups[0]),
            "handlerAvg": groups[1],
            "callbackCount": int(groups[2]),
            "callbackMiB": float(groups[3]),
            "callbackAvg": groups[4],
            "mmapPageAttempts": int(groups[5]),
            "mmapPageHits": int(groups[6]),
            "mmapPageMisses": int(groups[7]),
            "mmapPageFailures": int(groups[8]),
            "mmapPageMiB": float(groups[9]),
            "readIntoAttempts": int(groups[10]),
            "readIntoHits": int(groups[11]),
            "readIntoMisses": int(groups[12]),
            "readIntoMiB": float(groups[13]),
            "streamAttempts": int(groups[14]),
            "streamHits": int(groups[15]),
            "streamMisses": int(groups[16]),
            "streamMiB": float(groups[17]),
            "unaryAttempts": int(groups[18]),
            "unaryHits": int(groups[19]),
            "unaryMisses": int(groups[20]),
            "unaryMiB": float(groups[21]),
            "cloudRequests": int(groups[22]),
            "cloudMiB": float(groups[23]),
        }
        add_geesefs_cache_event(summary, line)
        return summary
    match = GEESEFS_SUMMARY_LEGACY_RE.search(line)
    if not match:
        return None
    groups = match.groups()
    summary = {
        "mmapPageAttempts": int(groups[0]),
        "mmapPageHits": int(groups[1]),
        "mmapPageMisses": int(groups[2]),
        "mmapPageFailures": int(groups[3]),
        "mmapPageMiB": float(groups[4]),
        "readIntoAttempts": int(groups[5]),
        "readIntoHits": int(groups[6]),
        "readIntoMisses": int(groups[7]),
        "readIntoMiB": float(groups[8]),
        "streamAttempts": int(groups[9]),
        "streamHits": int(groups[10]),
        "streamMisses": int(groups[11]),
        "streamMiB": float(groups[12]),
        "unaryAttempts": int(groups[13]),
        "unaryHits": int(groups[14]),
        "unaryMisses": int(groups[15]),
        "unaryMiB": float(groups[16]),
        "cloudRequests": int(groups[17]),
        "cloudMiB": float(groups[18]),
    }
    add_geesefs_cache_event(summary, line)
    return summary


def add_geesefs_cache_event(summary, line):
    match = GEESEFS_CACHE_EVENT_RE.search(line)
    if not match:
        return
    groups = match.groups()
    summary.update(
        {
            "cacheEventQueued": int(groups[0]),
            "cacheEventStarted": int(groups[1]),
            "cacheEventOk": int(groups[2]),
            "cacheEventErr": int(groups[3]),
            "cacheEventMismatch": int(groups[4]),
            "cacheEventDropped": int(groups[5]),
            "cacheEventMiB": float(groups[6]),
        }
    )


def parse_cache_summary(line):
    match = CACHE_SUMMARY_RE.search(line)
    if not match:
        return None
    groups = match.groups()
    return {
        "clientReadInto": int(groups[0]),
        "clientReadIntoMiB": float(groups[1]),
        "clientLocalHits": int(groups[2]),
        "clientLocalMisses": int(groups[3]),
        "clientRawHits": int(groups[4]),
        "clientRawMisses": int(groups[5]),
        "clientRawErrors": int(groups[6]),
        "clientGRPCHits": int(groups[7]),
        "clientGRPCMisses": int(groups[8]),
        "clientGRPCErrors": int(groups[9]),
        "localPageRegionRequests": int(groups[10]),
        "localPageRegionHits": int(groups[11]),
        "localPageRegionMisses": int(groups[12]),
        "localPageRegionMiB": float(groups[13]),
        "serverRawRequests": int(groups[14]),
        "serverRawSendfileHits": int(groups[15]),
        "serverRawCopyHits": int(groups[16]),
        "serverRawReadAtHits": int(groups[17]),
        "serverRawMisses": int(groups[18]),
        "serverRawErrors": int(groups[19]),
        "storePageRegions": int(groups[20]),
        "storePageRegionHits": int(groups[21]),
        "storePageRegionMisses": int(groups[22]),
        "storePageRegionMiB": float(groups[23]),
    }


def parse_log_fields(line):
    clean = strip_ansi(line)
    fields = {}
    try:
        tokens = shlex.split(clean)
    except ValueError:
        tokens = clean.split()
    for token in tokens:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            continue
        fields[key] = value.strip()
    return fields


def json_log_field(fields, key):
    value = fields.get(key)
    if not value:
        return []
    value = value.strip()
    for candidate in (value, value.strip('"'), value.strip("'")):
        try:
            parsed = json.loads(candidate)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            pass
    try:
        parsed = json.loads(bytes(value, "utf-8").decode("unicode_escape"))
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def int_log_field(fields, key, default=0):
    try:
        return int(fields.get(key, default))
    except (TypeError, ValueError):
        return default


def clip_rollup_source(rollup):
    return rollup.get("source") or rollup.get("Source") or "unknown"


def clip_rollup_bytes(rollup):
    for key in ("bytes_read", "BytesRead"):
        try:
            return int(rollup.get(key) or 0)
        except (TypeError, ValueError):
            return 0
    return 0


def parse_clip_read_summary(line):
    if "clip read path summary" not in line:
        return None
    fields = parse_log_fields(line)
    top_sources = json_log_field(fields, "top_sources_json")
    source_bytes = {}
    source_counts = {}
    for rollup in top_sources:
        if not isinstance(rollup, dict):
            continue
        source = clip_rollup_source(rollup)
        source_bytes[source] = source_bytes.get(source, 0) + clip_rollup_bytes(rollup)
        try:
            source_counts[source] = source_counts.get(source, 0) + int(rollup.get("count") or rollup.get("Count") or 0)
        except (TypeError, ValueError):
            pass

    embedded_bytes = sum(source_bytes.get(source, 0) for source in CLIP_EMBEDDED_CACHE_SOURCES)
    fd_bytes = sum(source_bytes.get(source, 0) for source in CLIP_FD_SOURCES)
    registry_bytes = sum(source_bytes.get(source, 0) for source in CLIP_REGISTRY_SOURCES)
    disk_fd_bytes = source_bytes.get("disk_cache_fd", 0)
    content_page_fd_bytes = source_bytes.get("content_cache_page_fd", 0)
    local_archive_fd_bytes = source_bytes.get("local_archive_fd", 0)

    return {
        "containerId": fields.get("container_id", ""),
        "imageId": fields.get("image_id", ""),
        "flushReason": fields.get("flush_reason", ""),
        "readCount": int_log_field(fields, "read_count"),
        "bytesRead": int_log_field(fields, "bytes_read"),
        "totalUs": int_log_field(fields, "total_us"),
        "wallUs": int_log_field(fields, "wall_us"),
        "errorCount": int_log_field(fields, "error_count"),
        "sourceBytes": source_bytes,
        "sourceCounts": source_counts,
        "embeddedCacheBytes": embedded_bytes,
        "fdBytes": fd_bytes,
        "contentCachePageFdBytes": content_page_fd_bytes,
        "diskCacheFdBytes": disk_fd_bytes,
        "localArchiveFdBytes": local_archive_fd_bytes,
        "registryBytes": registry_bytes,
        "topSources": top_sources,
        "topPaths": json_log_field(fields, "top_paths_json"),
        "topOperations": json_log_field(fields, "top_operations_json"),
        "topContent": json_log_field(fields, "top_content_json"),
    }


def add_clip_summary_totals(total, summary):
    for key in (
        "readCount",
        "bytesRead",
        "totalUs",
        "wallUs",
        "errorCount",
        "embeddedCacheBytes",
        "fdBytes",
        "contentCachePageFdBytes",
        "diskCacheFdBytes",
        "localArchiveFdBytes",
        "registryBytes",
    ):
        total[key] = total.get(key, 0) + int(summary.get(key) or 0)
    for map_key in ("sourceBytes", "sourceCounts"):
        target = total.setdefault(map_key, {})
        for key, value in (summary.get(map_key) or {}).items():
            target[key] = target.get(key, 0) + value
    for list_key in ("topSources", "topPaths", "topOperations", "topContent"):
        if list_key not in total:
            total[list_key] = []
        total[list_key].extend(summary.get(list_key) or [])


def add_summary_totals(total, summary):
    for key, value in summary.items():
        if isinstance(value, (int, float)):
            total[key] = total.get(key, 0) + value
        else:
            total[key] = value


def parse_worker_dd_cache_proof(lines, geesefs_log_path, content_hash):
    proof = {
        "ok": False,
        "geesefsLogPath": geesefs_log_path,
        "hash": content_hash,
        "externalPageHitLines": 0,
        "externalPageHitBytes": 0,
        "externalPageMmapCacheHitLines": 0,
        "externalPageLocalRegionHitLines": 0,
        "externalPageMissLines": 0,
        "contentCloudReadLines": 0,
        "cacheLocalPageRegionLines": 0,
        "fuseResponseLines": 0,
        "fuseResponseBytes": 0,
        "fuseHandlerMs": 0.0,
        "fuseResponseMs": 0.0,
        "fuseResponseMaxMs": 0.0,
        "geesefsSummary": {},
        "cacheSummary": {},
        "matchedLines": [],
    }
    path_token = f'path="{geesefs_log_path}"'
    hash_token = f'hash="{content_hash}"'
    for line in lines:
        exact = path_token in line and hash_token in line
        content_path_seen = geesefs_log_path and geesefs_log_path in line
        if content_path_seen and (
            "s3.WARNING Error reading" in line
            or "cloud read" in line.lower()
            or "cloud(req=" in line
        ):
            proof["contentCloudReadLines"] += 1
            if len(proof["matchedLines"]) < 20:
                proof["matchedLines"].append(line[-900:])
            continue
        if "geesefs external page hit:" in line and exact:
            proof["externalPageHitLines"] += 1
            if "source=mmap_cache" in line:
                proof["externalPageMmapCacheHitLines"] += 1
            if "source=local_page_region" in line:
                proof["externalPageLocalRegionHitLines"] += 1
            if "source=read_into" in line:
                proof["externalPageReadIntoHitLines"] = proof.get("externalPageReadIntoHitLines", 0) + 1
            size_match = LOG_SIZE_RE.search(line)
            if size_match:
                proof["externalPageHitBytes"] += int(size_match.group(1))
            if len(proof["matchedLines"]) < 20:
                proof["matchedLines"].append(line[-900:])
            continue
        if "geesefs external page miss:" in line and exact:
            proof["externalPageMissLines"] += 1
            if len(proof["matchedLines"]) < 20:
                proof["matchedLines"].append(line[-900:])
            continue
        response_match = GEESEFS_READ_RESPONSE_RE.search(line)
        if response_match and response_match.group(1) == geesefs_log_path and response_match.group(2) == content_hash:
            bytes_read = int(response_match.group(5))
            handler_ms = float(response_match.group(6))
            response_ms = float(response_match.group(7))
            proof["fuseResponseLines"] += 1
            proof["fuseResponseBytes"] += bytes_read
            proof["fuseHandlerMs"] += handler_ms
            proof["fuseResponseMs"] += response_ms
            proof["fuseResponseMaxMs"] = max(proof["fuseResponseMaxMs"], response_ms)
            if len(proof["matchedLines"]) < 20:
                proof["matchedLines"].append(line[-900:])
            continue
        if "cache local page regions result:" in line and content_hash in line:
            proof["cacheLocalPageRegionLines"] += 1
            if len(proof["matchedLines"]) < 20:
                proof["matchedLines"].append(line[-900:])
            continue
        geesefs_summary = parse_geesefs_summary(line)
        if geesefs_summary:
            add_summary_totals(proof["geesefsSummary"], geesefs_summary)
            continue
        cache_summary = parse_cache_summary(line)
        if cache_summary:
            add_summary_totals(proof["cacheSummary"], cache_summary)

    geesefs_summary = proof["geesefsSummary"]
    cache_summary = proof["cacheSummary"]
    exact_external_hit = proof["externalPageHitLines"] > 0 and proof["externalPageHitBytes"] > 0
    summary_external_hit = (
        geesefs_summary.get("mmapPageHits", 0) > 0
        or geesefs_summary.get("readIntoHits", 0) > 0
        or geesefs_summary.get("streamHits", 0) > 0
        or geesefs_summary.get("unaryHits", 0) > 0
    )
    embedded_cache_hit = (
        cache_summary.get("localPageRegionHits", 0) > 0
        or cache_summary.get("clientLocalHits", 0) > 0
        or cache_summary.get("clientRawHits", 0) > 0
        or cache_summary.get("serverRawSendfileHits", 0) > 0
    )
    no_cloud_read = geesefs_summary.get("cloudRequests", 0) == 0
    no_content_cloud_read = proof["contentCloudReadLines"] == 0
    proof["noCloudReadInWindow"] = no_cloud_read
    proof["noContentCloudRead"] = no_content_cloud_read
    proof["ok"] = (
        (exact_external_hit or summary_external_hit)
        and (embedded_cache_hit or exact_external_hit)
        and no_content_cloud_read
    )
    return proof


def attach_read_path_diagnostics(args, sample, log_since, geesefs_log_path, content_hash, pod_name=None):
    if args.read_path_log_wait_seconds > 0:
        time.sleep(args.read_path_log_wait_seconds)
    logs = worker_logs_since(args.namespace, pod_name, log_since, args.log_tail)
    sample["readPathLogs"] = {
        "ok": logs.get("ok"),
        "sinceTime": log_since,
        "lineCount": len(logs.get("lines") or []),
        "error": logs.get("error"),
        "pod": pod_name or "",
    }
    if logs.get("ok"):
        sample["readPathProof"] = parse_worker_dd_cache_proof(
            logs.get("lines") or [],
            geesefs_log_path,
            content_hash,
        )


def parse_clip_read_proof(lines, image_id="", container_id=""):
    proof = {
        "kind": "image_archive",
        "ok": False,
        "imageId": image_id,
        "containerId": container_id,
        "summaryCount": 0,
        "readCount": 0,
        "bytesRead": 0,
        "embeddedCacheBytes": 0,
        "fdBytes": 0,
        "contentCachePageFdBytes": 0,
        "diskCacheFdBytes": 0,
        "localArchiveFdBytes": 0,
        "registryBytes": 0,
        "localPageRegionLines": 0,
        "localPageRegionErrors": 0,
        "matchedLines": [],
        "summary": {},
    }
    for line in lines:
        summary = parse_clip_read_summary(line)
        if summary:
            if container_id and summary.get("containerId") and summary.get("containerId") != container_id:
                continue
            if image_id and summary.get("imageId") and summary.get("imageId") != image_id:
                continue
            proof["summaryCount"] += 1
            add_clip_summary_totals(proof["summary"], summary)
            if len(proof["matchedLines"]) < 20:
                proof["matchedLines"].append(line[-1200:])
            continue
        if "clip image content cache local page regions result" in line:
            if image_id and f'image_id={image_id}' not in line and f'image_id="{image_id}"' not in line:
                continue
            proof["localPageRegionLines"] += 1
            if "error=" in line and 'error="<nil>"' not in line:
                proof["localPageRegionErrors"] += 1
            if len(proof["matchedLines"]) < 20:
                proof["matchedLines"].append(line[-1200:])

    summary = proof["summary"]
    for key in (
        "readCount",
        "bytesRead",
        "embeddedCacheBytes",
        "fdBytes",
        "contentCachePageFdBytes",
        "diskCacheFdBytes",
        "localArchiveFdBytes",
        "registryBytes",
    ):
        proof[key] = int(summary.get(key) or 0)
    proof["sourceBytes"] = summary.get("sourceBytes") or {}
    proof["sourceCounts"] = summary.get("sourceCounts") or {}
    proof["topSources"] = summary.get("topSources") or []
    proof["topPaths"] = summary.get("topPaths") or []
    proof["topOperations"] = summary.get("topOperations") or []
    proof["topContent"] = summary.get("topContent") or []
    proof["noRegistryReadInWindow"] = proof["registryBytes"] == 0
    proof["embeddedCacheHit"] = proof["embeddedCacheBytes"] > 0
    proof["fdFastPathHit"] = proof["fdBytes"] > 0
    proof["ok"] = proof["summaryCount"] > 0 and proof["embeddedCacheHit"] and proof["noRegistryReadInWindow"]
    return proof


def attach_clip_read_diagnostics(args, sample, log_since, image_id="", container_id="", pod_name=None):
    if args.read_path_log_wait_seconds > 0:
        time.sleep(args.read_path_log_wait_seconds)
    logs = worker_logs_since(args.namespace, pod_name, log_since, args.log_tail)
    sample["readPathLogs"] = {
        "ok": logs.get("ok"),
        "sinceTime": log_since,
        "lineCount": len(logs.get("lines") or []),
        "error": logs.get("error"),
        "pod": pod_name or "",
    }
    if logs.get("ok"):
        sample["readPathProof"] = parse_clip_read_proof(
            logs.get("lines") or [],
            image_id=image_id,
            container_id=container_id,
        )


def worker_shell(namespace, pod_name, script, timeout=20):
    return run(
        ["kubectl", "-n", namespace, "exec", pod_name, "-c", "worker", "--", "sh", "-c", script],
        check=False,
        timeout=timeout,
    )


def cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash):
    node_name = pod.get("nodeName") or pod.get("hostIP") or ""
    bucket = content_hash[:2] if content_hash and len(content_hash) >= 2 else "00"
    candidates = []
    if node_name:
        candidates.append(str(Path(cache_mount_path) / locality / node_name / "pages" / bucket / content_hash))
        candidates.append(str(Path(cache_mount_path) / locality / node_name / content_hash))
    candidates.append(str(Path(cache_mount_path) / "pages" / bucket / content_hash))
    candidates.append(str(Path(cache_mount_path) / content_hash))
    return candidates


def delete_cache_object(namespace, cache_mount_path, locality, content_hash):
    if not content_hash or len(content_hash) != 64:
        return {"hash": content_hash, "deleted": False}
    result = {"hash": content_hash, "deleted": True, "errors": []}
    for pod in worker_pods(namespace):
        paths = " ".join(shlex.quote(path) for path in cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash))
        proc = worker_shell(namespace, pod["name"], f"rm -rf {paths}")
        if proc.returncode != 0:
            result["errors"].append({"pod": pod["name"], "error": proc.stderr.strip() or proc.stdout.strip()})
    return result


def cache_object_proof(namespace, cache_mount_path, locality, content_hash):
    if not content_hash:
        return {"hash": content_hash, "exists": False, "error": "empty hash"}
    for pod in worker_pods(namespace):
        dirs = " ".join(shlex.quote(path) for path in cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash))
        script = (
            f"hash={shlex.quote(content_hash)}; "
            "found=''; for candidate in "
            f"{dirs}; do [ -d \"$candidate\" ] && found=\"$candidate\" && break; done; "
            'if [ -z "$found" ]; then printf \'{"exists":false}\\n\'; exit 0; fi; '
            "i=0; bytes=0; tmp=$(mktemp); "
            'while [ -f "$found/$hash-$i" ]; do cat "$found/$hash-$i" >> "$tmp"; '
            'bytes=$((bytes + $(wc -c < "$found/$hash-$i"))); i=$((i + 1)); done; '
            'sha=$(sha256sum "$tmp" | awk \'{print $1}\'); rm -f "$tmp"; '
            'printf \'{"exists":true,"path":"%s","chunks":%s,"bytes":%s,"sha256":"%s"}\\n\' "$found" "$i" "$bytes" "$sha"'
        )
        proc = worker_shell(namespace, pod["name"], script, timeout=60)
        if proc.returncode != 0:
            continue
        try:
            proof = json.loads(proc.stdout.strip().splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            continue
        proof.update({"hash": content_hash, "pod": pod["name"], "nodeName": pod.get("nodeName", "")})
        if proof.get("exists"):
            proof["matchesHash"] = proof.get("sha256") == content_hash
            return proof
    return {"hash": content_hash, "exists": False}


def wait_cache_object_proof(args, content_hash):
    deadline = time.monotonic() + args.cache_proof_timeout_seconds
    last = {"hash": content_hash, "exists": False}
    while True:
        last = cache_object_proof(args.namespace, args.cache_mount_path, args.cache_locality, content_hash)
        if last.get("exists"):
            return last
        if time.monotonic() >= deadline:
            return last
        time.sleep(1)


def cache_object_ready(namespace, cache_mount_path, locality, content_hash, expected_size):
    if not content_hash:
        return {"hash": content_hash, "ready": False, "exists": False, "error": "empty hash"}
    for pod in worker_pods(namespace):
        dirs = " ".join(shlex.quote(path) for path in cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash))
        script = (
            f"hash={shlex.quote(content_hash)}; expected={int(expected_size)}; "
            "found=''; for candidate in "
            f"{dirs}; do [ -d \"$candidate\" ] && found=\"$candidate\" && break; done; "
            'if [ -z "$found" ]; then printf \'{"exists":false,"ready":false}\\n\'; exit 0; fi; '
            "i=0; bytes=0; "
            'while [ -f "$found/$hash-$i" ]; do '
            'size=$(stat -c %s "$found/$hash-$i" 2>/dev/null || stat -f %z "$found/$hash-$i" 2>/dev/null || echo 0); '
            'bytes=$((bytes + size)); i=$((i + 1)); '
            "done; "
            'ready=false; [ "$bytes" -eq "$expected" ] && ready=true; '
            'printf \'{"exists":true,"ready":%s,"path":"%s","chunks":%s,"bytes":%s}\\n\' "$ready" "$found" "$i" "$bytes"'
        )
        proc = worker_shell(namespace, pod["name"], script, timeout=20)
        if proc.returncode != 0:
            continue
        try:
            ready = json.loads(proc.stdout.strip().splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            continue
        ready.update({"hash": content_hash, "pod": pod["name"], "nodeName": pod.get("nodeName", "")})
        if ready.get("ready"):
            return ready
    return {"hash": content_hash, "ready": False, "exists": False}


def wait_cache_object_ready(args, content_hash, expected_size):
    deadline = time.monotonic() + args.cache_proof_timeout_seconds
    last = {"hash": content_hash, "ready": False, "exists": False}
    last_log = 0.0
    while True:
        last = cache_object_ready(
            args.namespace,
            args.cache_mount_path,
            args.cache_locality,
            content_hash,
            expected_size,
        )
        if last.get("ready"):
            return last
        now = time.monotonic()
        if now >= deadline:
            log(
                "Timed out waiting for embedded cache page files "
                f"hash={content_hash} expected={expected_size} last={last}"
            )
            return last
        if now - last_log >= 10:
            remaining = max(0, deadline - now)
            log(
                "Waiting for embedded cache page files "
                f"hash={content_hash} expected={expected_size} remaining={remaining:.0f}s last={last}"
            )
            last_log = now
        time.sleep(1)


def evict_cache_object_pages(namespace, cache_mount_path, locality, content_hash, expected_size=0):
    if not content_hash:
        return {"hash": content_hash, "ok": False, "error": "empty hash", "pods": []}

    result = {
        "hash": content_hash,
        "expectedBytes": int(expected_size or 0),
        "ok": False,
        "pods": [],
        "totalBytes": 0,
        "totalChunks": 0,
    }
    seen_nodes = set()
    for pod in worker_pods(namespace):
        if pod.get("phase") != "Running":
            continue
        node_key = pod.get("nodeName") or pod.get("hostIP") or pod["name"]
        if node_key in seen_nodes:
            continue
        seen_nodes.add(node_key)
        candidates = cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash)
        command = [
            "python3",
            "-c",
            EVICT_CACHE_OBJECT_HARNESS,
            content_hash,
            *candidates,
        ]
        proc = worker_exec(namespace, pod["name"], command, timeout=120)
        entry = {
            "pod": pod["name"],
            "nodeName": pod.get("nodeName", ""),
            "execExitCode": proc.returncode,
            "stdout": proc.stdout[-1000:],
            "stderr": proc.stderr[-1000:],
        }
        if proc.returncode == 0:
            try:
                payload = parse_json_output(proc.stdout)
                entry.update(payload)
            except Exception as exc:
                entry["error"] = str(exc)
        else:
            entry["error"] = proc.stderr.strip() or proc.stdout.strip()
        if entry.get("evicted"):
            result["totalBytes"] += int(entry.get("bytes") or 0)
            result["totalChunks"] += int(entry.get("chunks") or 0)
        result["pods"].append(entry)

    result["ok"] = any(pod.get("evicted") for pod in result["pods"])
    if expected_size:
        result["bytesMatchExpected"] = any(int(pod.get("bytes") or 0) == int(expected_size) for pod in result["pods"])
    return result


def evict_mounted_file_pages(namespace, path, expected_size=0):
    if not path:
        return {"path": path, "ok": False, "error": "empty path", "pods": []}

    result = {
        "path": path,
        "expectedBytes": int(expected_size or 0),
        "ok": False,
        "pods": [],
    }
    for pod in worker_pods(namespace):
        if pod.get("phase") != "Running" or not pod.get("name", "").startswith("worker-"):
            continue
        command = [
            "python3",
            "-c",
            EVICT_MOUNTED_FILE_HARNESS,
            path,
            str(int(expected_size or 0)),
        ]
        proc = worker_exec(namespace, pod["name"], command, timeout=120)
        entry = {
            "pod": pod["name"],
            "nodeName": pod.get("nodeName", ""),
            "execExitCode": proc.returncode,
            "stdout": proc.stdout[-1000:],
            "stderr": proc.stderr[-1000:],
        }
        if proc.returncode == 0:
            try:
                payload = parse_json_output(proc.stdout)
                entry.update(payload)
            except Exception as exc:
                entry["error"] = str(exc)
        else:
            entry["error"] = proc.stderr.strip() or proc.stdout.strip()
        result["pods"].append(entry)

    result["ok"] = any(pod.get("evicted") for pod in result["pods"])
    if expected_size:
        result["bytesMatchExpected"] = any(int(pod.get("bytes") or 0) == int(expected_size) for pod in result["pods"])
    return result


def delete_image_archive_cache_files(namespace, image_cache_path, image_id):
    if not image_id:
        return {"deleted": [], "errors": []}
    names = [f"{image_id}.clip", f"{image_id}.rclip", f"{image_id}.clip.lock", f"{image_id}.rclip.lock"]
    quoted = " ".join(shlex.quote(name) for name in names)
    result = {"deleted": names, "errors": []}
    for pod in worker_pods(namespace):
        script = f"path={shlex.quote(image_cache_path)}; for name in {quoted}; do rm -f \"$path/$name\"; done"
        proc = worker_shell(namespace, pod["name"], script)
        if proc.returncode != 0:
            result["errors"].append({"pod": pod["name"], "error": proc.stderr.strip() or proc.stdout.strip()})
    return result


def delete_image_local_layer_cache_files(namespace, image_cache_path):
    result = {"deletedFiles": 0, "deletedBytes": 0, "pods": [], "errors": []}
    script = (
        f"path={shlex.quote(image_cache_path)}; "
        'deleted=0; bytes=0; '
        'if [ -d "$path" ]; then '
        'for f in "$path"/*; do '
        '[ -f "$f" ] || continue; '
        'name=${f##*/}; '
        'case "$name" in *.clip|*.rclip|*.lock|*.cache) continue;; esac; '
        'if [ ${#name} -eq 64 ] && printf "%s" "$name" | grep -Eq "^[a-f0-9]{64}$"; then '
        'size=$(wc -c < "$f" 2>/dev/null || printf 0); '
        'rm -f "$f" && deleted=$((deleted + 1)) && bytes=$((bytes + size)); '
        'fi; '
        'done; '
        'fi; '
        'printf \'{"deletedFiles":%s,"deletedBytes":%s}\\n\' "$deleted" "$bytes"'
    )
    for pod in worker_pods(namespace):
        proc = worker_shell(namespace, pod["name"], script, timeout=60)
        entry = {"pod": pod["name"], "execExitCode": proc.returncode}
        if proc.returncode == 0:
            try:
                payload = json.loads(proc.stdout.strip().splitlines()[-1])
                entry.update(payload)
                result["deletedFiles"] += int(payload.get("deletedFiles") or 0)
                result["deletedBytes"] += int(payload.get("deletedBytes") or 0)
            except (json.JSONDecodeError, IndexError, ValueError) as exc:
                entry["error"] = str(exc)
                result["errors"].append({"pod": pod["name"], "error": str(exc)})
        else:
            entry["error"] = proc.stderr.strip() or proc.stdout.strip()
            result["errors"].append({"pod": pod["name"], "error": entry["error"]})
        result["pods"].append(entry)
    return result


def cache_disk_stats(namespace, cache_mount_path):
    stats = {"path": cache_mount_path, "pods": [], "totalBytes": 0, "totalFiles": 0}
    counted_nodes = set()
    script = (
        f"path={shlex.quote(cache_mount_path)}; "
        'if [ -d "$path" ]; then bytes=$(du -sk "$path" 2>/dev/null | awk \'{print $1 * 1024}\'); '
        'files=$(find "$path" -type f 2>/dev/null | wc -l | tr -d " "); '
        'printf \'{"exists":true,"bytes":%s,"files":%s}\\n\' "${bytes:-0}" "${files:-0}"; '
        'else printf \'{"exists":false,"bytes":0,"files":0}\\n\'; fi'
    )
    for pod in worker_pods(namespace):
        proc = worker_shell(namespace, pod["name"], script)
        entry = {"pod": pod["name"], "nodeName": pod.get("nodeName", "")}
        if proc.returncode == 0:
            try:
                entry.update(json.loads(proc.stdout.strip().splitlines()[-1]))
            except (json.JSONDecodeError, IndexError):
                pass
        node_key = pod.get("nodeName") or pod["name"]
        entry["countedInTotal"] = node_key not in counted_nodes
        stats["pods"].append(entry)
        if entry["countedInTotal"]:
            counted_nodes.add(node_key)
            stats["totalBytes"] += int(entry.get("bytes") or 0)
            stats["totalFiles"] += int(entry.get("files") or 0)
    return stats


def cache_hosts_snapshot(namespace, redis_pod, locality):
    if not redis_pod:
        return {"available": False, "hosts": []}
    raw_keys = redis_cli(namespace, redis_pod, "KEYS", f"cache:host_index:{locality}")
    if raw_keys is None:
        return {"available": False, "hosts": []}
    hosts = []
    for key in [line for line in raw_keys.splitlines() if line.strip()]:
        raw_members = redis_cli(namespace, redis_pod, "SMEMBERS", key) or ""
        for host_id in [line for line in raw_members.splitlines() if line.strip()]:
            host = {"locality": locality, "hostId": host_id}
            raw_host = redis_cli(namespace, redis_pod, "GET", f"cache:host:keepalive:{locality}:{host_id}")
            if raw_host:
                try:
                    keepalive = json.loads(raw_host)
                    host.update(
                        {
                            "hostId": keepalive.get("host_id") or keepalive.get("hostId") or host_id,
                            "addr": keepalive.get("addr") or keepalive.get("Addr") or "",
                            "privateAddr": keepalive.get("private_addr") or keepalive.get("privateAddr") or "",
                            "capacityUsagePct": keepalive.get("capacity_usage_pct")
                            or keepalive.get("capacityUsagePct")
                            or 0,
                        }
                    )
                except json.JSONDecodeError:
                    host["keepaliveRaw"] = raw_host[-300:]
            hosts.append(host)
    return {"available": True, "hosts": hosts}


def redis_hgetall_map(namespace, redis_pod, key):
    raw = redis_cli(namespace, redis_pod, "HGETALL", key)
    if raw is None:
        return {}
    lines = [line for line in raw.splitlines() if line != ""]
    if len(lines) % 2 != 0:
        return {}
    return {lines[i]: lines[i + 1] for i in range(0, len(lines), 2)}


def cachefs_node_id(path):
    return hashlib.sha256(path.encode()).hexdigest()


def image_archive_cachefs_proof(args, image_id):
    if not image_id:
        return {"exists": False, "error": "empty image id"}
    redis_pod = find_redis_pod(args.namespace)
    candidates = [f"/images/{image_id}.clip", f"/images/{image_id}.rclip"]
    for cache_path in candidates:
        node_id = cachefs_node_id(cache_path)
        metadata = redis_hgetall_map(args.namespace, redis_pod, f"cache:fs:node:{node_id}")
        content_hash = metadata.get("hash")
        if not content_hash:
            continue
        cas_proof = cache_object_proof(args.namespace, args.cache_mount_path, args.cache_locality, content_hash)
        return {
            "exists": bool(metadata),
            "cachePath": cache_path,
            "nodeId": node_id,
            "hash": content_hash,
            "metadata": metadata,
            "casProof": cas_proof,
            "matchesHash": bool(cas_proof.get("matchesHash")),
            "bytes": cas_proof.get("bytes"),
        }
    return {"exists": False, "imageId": image_id, "checkedPaths": candidates}


def safe_cache_name(value):
    value = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value or "")
    return value or "default"


def cache_host_addr(host):
    return host.get("privateAddr") or host.get("private_addr") or host.get("addr") or host.get("Addr") or ""


def host_part(addr):
    if not addr:
        return ""
    if addr.startswith("["):
        return addr[1:].partition("]")[0]
    return addr.rsplit(":", 1)[0]


def choose_cache_host_for_proof(hosts_snapshot, cas_proof):
    hosts = [host for host in hosts_snapshot.get("hosts", []) if cache_host_addr(host)]
    if not hosts:
        return None

    node_name = cas_proof.get("nodeName") or ""
    if node_name:
        safe_node = safe_cache_name(node_name)
        for host in hosts:
            host_id = host.get("hostId") or host.get("host_id") or ""
            if node_name in host_id or safe_node in host_id:
                return host

    proof_pod = cas_proof.get("pod") or ""
    if proof_pod:
        safe_pod = safe_cache_name(proof_pod)
        for host in hosts:
            host_id = host.get("hostId") or host.get("host_id") or ""
            if proof_pod in host_id or safe_pod in host_id:
                return host

    return hosts[0]


def target_worker_for_cache_host(namespace, target_host):
    target_ip = host_part(cache_host_addr(target_host))
    if not target_ip:
        return None
    for pod in worker_pods(namespace):
        if pod.get("phase") == "Running" and pod.get("podIP") == target_ip:
            return pod
    return None


def choose_worker_for_remote_proof(namespace, target_host):
    pods = [pod for pod in worker_pods(namespace) if pod.get("phase") == "Running"]
    if not pods:
        return None, None

    target_pod = target_worker_for_cache_host(namespace, target_host)
    target_pod_name = (target_pod or {}).get("name", "")
    target_pod_ip = (target_pod or {}).get("podIP", "")
    for pod in pods:
        if target_pod_name and pod["name"] == target_pod_name:
            continue
        if target_pod_ip and pod.get("podIP") == target_pod_ip:
            continue
        return pod, target_pod

    return None, target_pod


def adaptive_remote_proof_chunk_bytes(size, requested_chunk_bytes, concurrency):
    min_chunk = 4 * 1024 * 1024
    if requested_chunk_bytes <= 0:
        requested_chunk_bytes = 16 * 1024 * 1024
    if concurrency <= 1:
        return min(size, requested_chunk_bytes)
    per_worker = (size + concurrency - 1) // concurrency
    per_worker = max(min_chunk, per_worker)
    return min(size, min(requested_chunk_bytes, per_worker))


def node_goarch(namespace, node_name):
    if not node_name:
        return "amd64"
    proc = run(
        ["kubectl", "get", "node", node_name, "-o", "jsonpath={.status.nodeInfo.architecture}"],
        check=False,
        timeout=10,
    )
    arch = (proc.stdout or "").strip().lower()
    if arch in {"arm64", "aarch64"}:
        return "arm64"
    if arch in {"amd64", "x86_64"}:
        return "amd64"
    return arch or "amd64"


def build_remote_raw_go_binary(goarch):
    cached = REMOTE_RAW_READ_BINARIES.get(goarch)
    if cached and Path(cached).exists():
        return cached

    build_dir = Path(tempfile.gettempdir()) / "beta9-cache-benchmark-rawread"
    build_dir.mkdir(parents=True, exist_ok=True)
    source = build_dir / "rawread.go"
    output = build_dir / f"rawread-linux-{goarch}"
    source.write_text(REMOTE_RAW_READ_GO_HARNESS, encoding="utf-8")
    proc = run(
        [
            "env",
            "-u",
            "GOROOT",
            "GOOS=linux",
            f"GOARCH={goarch}",
            "CGO_ENABLED=0",
            "go",
            "build",
            "-trimpath",
            "-o",
            str(output),
            str(source),
        ],
        check=False,
        timeout=120,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or f"go build exited {proc.returncode}")[-1000:])
    REMOTE_RAW_READ_BINARIES[goarch] = str(output)
    return str(output)


def install_remote_raw_go_binary(args, source_pod):
    goarch = node_goarch(args.namespace, source_pod.get("nodeName", ""))
    binary = build_remote_raw_go_binary(goarch)
    remote_path = f"/tmp/beta9-raw-read-proof-{goarch}"
    proc = run(
        [
            "kubectl",
            "-n",
            args.namespace,
            "cp",
            binary,
            f"{source_pod['name']}:{remote_path}",
            "-c",
            "worker",
        ],
        check=False,
        timeout=60,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or f"kubectl cp exited {proc.returncode}")[-1000:])
    proc = worker_exec(args.namespace, source_pod["name"], ["chmod", "+x", remote_path], timeout=10)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or f"chmod exited {proc.returncode}")[-1000:])
    return remote_path, goarch


def remote_cache_read_proof(args, entry, cache_location_proof):
    redis_pod = find_redis_pod(args.namespace)
    hosts_snapshot = cache_hosts_snapshot(args.namespace, redis_pod, args.cache_locality)
    target_host = choose_cache_host_for_proof(hosts_snapshot, cache_location_proof or {})
    if target_host is None:
        return {
            "ok": False,
            "error": "no cache host with an advertised address was available for raw remote proof",
            "hosts": hosts_snapshot,
        }

    source_pod, target_pod = choose_worker_for_remote_proof(args.namespace, target_host)
    if source_pod is None:
        return {
            "ok": False,
            "error": "no separate running worker pod was available to execute cross-worker raw remote proof",
            "targetHost": target_host,
            "targetPod": target_pod,
        }

    addr = cache_host_addr(target_host)
    chunk_bytes = adaptive_remote_proof_chunk_bytes(
        entry["size"],
        args.remote_cache_proof_chunk_bytes,
        args.remote_cache_proof_concurrency,
    )
    timeout = int(max(args.exec_timeout_seconds, entry["size"] / (50 * 1024 * 1024) + 60))
    harness = "go"
    harness_error = ""
    try:
        remote_binary, goarch = install_remote_raw_go_binary(args, source_pod)
        command = [
            remote_binary,
            "--addr",
            addr,
            "--hash",
            entry["sha256"],
            "--size",
            str(entry["size"]),
            "--expected-sha256",
            entry["sha256"],
            "--chunk-bytes",
            str(chunk_bytes),
            "--concurrency",
            str(args.remote_cache_proof_concurrency),
        ]
    except Exception as exc:
        harness = "python"
        goarch = ""
        harness_error = str(exc)
        command = [
            "python3",
            "-c",
            REMOTE_RAW_READ_HARNESS,
            "--addr",
            addr,
            "--hash",
            entry["sha256"],
            "--size",
            str(entry["size"]),
            "--expected-sha256",
            entry["sha256"],
            "--chunk-bytes",
            str(chunk_bytes),
            "--concurrency",
            str(args.remote_cache_proof_concurrency),
        ]
    proc = worker_exec(args.namespace, source_pod["name"], command, timeout=timeout)
    proof = {
        "ok": False,
        "harness": harness,
        "harnessArch": goarch,
        "harnessError": harness_error,
        "sourcePod": source_pod["name"],
        "sourcePodIP": source_pod.get("podIP", ""),
        "sourceNode": source_pod.get("nodeName", ""),
        "targetHost": target_host,
        "targetPod": target_pod,
        "targetAddr": addr,
        "requestedChunkBytes": args.remote_cache_proof_chunk_bytes,
        "effectiveChunkBytes": chunk_bytes,
        "cacheLocationProof": cache_location_proof,
        "differentWorkerPod": bool(target_pod) and source_pod.get("name") != target_pod.get("name"),
        "execExitCode": proc.returncode,
        "stdout": proc.stdout[-2000:],
        "stderr": proc.stderr[-2000:],
    }
    if proc.returncode != 0:
        proof["error"] = proc.stderr.strip() or proc.stdout.strip() or f"raw proof exited {proc.returncode}"
        return proof

    try:
        payload = parse_json_output(proc.stdout)
    except Exception as exc:
        proof["error"] = str(exc)
        return proof

    proof.update(payload)
    proof["matchesHash"] = payload.get("sha256") == entry["sha256"]
    proof["ok"] = bool(payload.get("ok")) and proof["matchesHash"]
    return proof


def manifest_entry(manifest, size_mb, access_type, pattern):
    for entry in manifest["files"]:
        if (
            entry["sizeMiB"] == size_mb
            and entry.get("accessType") == access_type
            and entry.get("pattern") == pattern
        ):
            return entry
    raise RuntimeError(f"manifest missing {access_type}/{pattern}/{size_mb} MiB file")


def row_key(access_type, pattern, size_mb):
    return f"{access_type}:{pattern}:{size_mb}mib"


def row_size_mib(row):
    return row.get("fileSizeMiB") or row.get("sizeMiB") or 0


def cold_cache_population_error(sample):
    proof = sample.get("readPathProof") or {}
    geesefs_summary = proof.get("geesefsSummary") or {}
    cloud_requests = geesefs_summary.get("cloudRequests", 0)
    cache_events = (
        geesefs_summary.get("cacheEventQueued", 0)
        + geesefs_summary.get("cacheEventStarted", 0)
        + geesefs_summary.get("cacheEventOk", 0)
    )
    external_hits = proof.get("externalPageHitLines", 0)
    if cloud_requests > 0 and cache_events == 0 and external_hits == 0:
        return (
            "cold read went to cloud but did not queue embedded-cache population "
            f"(hash metadata may be missing): geesefs_summary={geesefs_summary}"
        )
    return ""


def manifest_entries_for_access(args, manifest, access_type):
    if args.file_specs:
        return [entry for entry in manifest["files"] if entry.get("accessType") == access_type]
    entries = []
    for size_mb in args.sizes:
        for pattern in args.pattern_list:
            entries.append(manifest_entry(manifest, size_mb, access_type, pattern))
    return entries


def print_sample_header():
    print(
        "\n"
        " run access          pattern    state  size      read      total mb/s  file mb/s status\n"
        "---- --------------- ---------- ----- ----- --------- --------------- ---------- -------"
    )


def run_matrix(args, sandbox, token, workspace, volume, manifest, access_types, start_index):
    rows = []
    samples = []
    volume_id = volume_external_id(volume)
    evidence = {"image": {"imageId": getattr(sandbox, "image_id", "")}, "volume": {"volumeId": volume_id}}
    index = start_index

    for access_type in access_types:
        if access_type == "image_archive":
            root = IMAGE_READ_ROOT
        elif access_type == "volume_mount":
            root = volume_container_root(args)
        else:
            root = workspace_fuse_root(args, workspace, volume_id)

        for entry in manifest_entries_for_access(args, manifest, access_type):
            size_mb = entry["sizeMiB"]
            pattern = entry["pattern"]
            skip_cold_read = args.strict_disk_cache_hit and access_type in {"volume_mount", "workspace_fuse"}

            geesefs_log_path = None
            if access_type in {"volume_mount", "workspace_fuse"}:
                geesefs_log_path = str(Path("volumes") / volume_id / args.volume_subdir / entry["path"])

            cold_samples = []
            cold = None
            if skip_cold_read:
                log(
                    "Skipping storage cold read in strict disk-cache mode; "
                    f"the first timed read will be after CAS readiness and page-cache eviction for {entry['path']}"
                )
            else:
                cold = run_one(args, sandbox, token, workspace, access_type, root, entry, pattern, "cold", index, geesefs_log_path)
                index += 1
                cold_samples.append(cold)
                samples.append(cold)
                print_sample(cold)
            if not skip_cold_read and args.settle_seconds > 0:
                time.sleep(args.settle_seconds)

            pre_hot_cache_ready = None
            pre_hot_eviction = None
            pre_hot_mounted_eviction = None
            cache_population_error = ""
            if access_type in {"volume_mount", "workspace_fuse"} and args.strict_disk_cache_hit and cold is not None:
                cache_population_error = cold_cache_population_error(cold)
                if cache_population_error:
                    log(
                        "Cold read did not prove embedded-cache population; strict hot read will fail "
                        f"cache-readiness validation instead of waiting indefinitely: {cache_population_error}"
                    )
            if access_type in {"volume_mount", "workspace_fuse"} and args.wait_cache_ready_after_prepare:
                pre_hot_cache_ready = wait_cache_object_ready(args, entry["sha256"], entry["size"])
                if not pre_hot_cache_ready.get("ready"):
                    log(
                        "Cache object was not ready before hot read "
                        f"{entry['path']} hash={entry['sha256']} proof={pre_hot_cache_ready}"
                    )
            if access_type in {"volume_mount", "workspace_fuse"} and args.reset_workers_before_hot:
                if cache_population_error:
                    pre_hot_cache_ready = {
                        "hash": entry["sha256"],
                        "ready": False,
                        "exists": False,
                        "error": cache_population_error,
                    }
                elif not pre_hot_cache_ready or not pre_hot_cache_ready.get("ready"):
                    pre_hot_cache_ready = wait_cache_object_ready(args, entry["sha256"], entry["size"])
                log(
                    "Deleting worker jobs before hot read to clear geesefs/FUSE mmap state "
                    f"for {entry['path']} ({size_mb} MiB)"
                )
                delete_workers(args.namespace)
                if args.settle_seconds > 0:
                    time.sleep(args.settle_seconds)
            if access_type in {"volume_mount", "workspace_fuse"} and args.evict_cache_pages_before_hot:
                pre_hot_eviction = evict_cache_object_pages(
                    args.namespace,
                    args.cache_mount_path,
                    args.cache_locality,
                    entry["sha256"],
                    entry["size"],
                )
                log(
                    "Evicted cache page files before hot read "
                    f"{entry['path']} hash={entry['sha256']} ok={pre_hot_eviction.get('ok')} "
                    f"bytes={pre_hot_eviction.get('totalBytes')}"
                )
            if access_type in {"volume_mount", "workspace_fuse"} and args.evict_mounted_file_before_hot:
                mounted_path = str(Path(workspace_fuse_root(args, workspace, volume_id)) / entry["path"])
                pre_hot_mounted_eviction = evict_mounted_file_pages(
                    args.namespace,
                    mounted_path,
                    entry["size"],
                )
                log(
                    "Evicted mounted file page cache before hot read "
                    f"{mounted_path} ok={pre_hot_mounted_eviction.get('ok')}"
                )

            hot_samples = []
            for _ in range(args.iterations):
                if access_type == "image_archive":
                    evidence["image"].setdefault("localArchiveEvictions", []).append(
                        delete_image_archive_cache_files(
                            args.namespace,
                            args.image_cache_path,
                            getattr(sandbox, "image_id", ""),
                        )
                    )
                    evidence["image"].setdefault("localLayerEvictions", []).append(
                        delete_image_local_layer_cache_files(args.namespace, args.image_cache_path)
                    )
                    if args.reset_workers_before_image_hot:
                        log(
                            "Deleting worker jobs before hot image read to clear CLIP/FUSE/kernel image state "
                            f"for image {getattr(sandbox, 'image_id', '')}"
                        )
                        delete_workers(args.namespace)
                        if args.settle_seconds > 0:
                            time.sleep(args.settle_seconds)

                hot = run_one(args, sandbox, token, workspace, access_type, root, entry, pattern, "hot", index, geesefs_log_path)
                index += 1
                hot_samples.append(hot)
                samples.append(hot)
                print_sample(hot)

            row = {
                "key": row_key(access_type, pattern, size_mb),
                "accessType": access_type,
                "pattern": pattern,
                "fileSizeBytes": entry["size"],
                "fileSizeMiB": size_mb,
                "path": entry["path"],
                "expectedSha256": entry["sha256"],
                "cold": summarize_samples(cold_samples),
                "hot": summarize_samples(hot_samples),
                "samples": {"cold": cold_samples, "hot": hot_samples},
            }
            if access_type in {"volume_mount", "workspace_fuse"}:
                row["cacheEvidence"] = {
                    "preHotReady": pre_hot_cache_ready,
                    "preHotEviction": pre_hot_eviction,
                    "preHotMountedEviction": pre_hot_mounted_eviction,
                }
                route_proof = pre_hot_cache_ready
                if not route_proof or not route_proof.get("exists"):
                    route_proof = cache_object_ready(
                        args.namespace,
                        args.cache_mount_path,
                        args.cache_locality,
                        entry["sha256"],
                        entry["size"],
                    )
                if args.require_remote_cache_read:
                    if args.evict_cache_pages_before_remote_proof:
                        remote_eviction = evict_cache_object_pages(
                            args.namespace,
                            args.cache_mount_path,
                            args.cache_locality,
                            entry["sha256"],
                            entry["size"],
                        )
                        row["cacheEvidence"]["preRemoteEviction"] = remote_eviction
                        log(
                            "Evicted cache page files before remote raw proof "
                            f"{entry['path']} hash={entry['sha256']} ok={remote_eviction.get('ok')} "
                            f"bytes={remote_eviction.get('totalBytes')}"
                        )
                    row["cacheEvidence"]["remoteRawRead"] = remote_cache_read_proof(args, entry, route_proof)
                if args.wait_cache_ready_after_prepare:
                    cas_proof = wait_cache_object_proof(args, entry["sha256"])
                else:
                    cas_proof = cache_object_proof(
                        args.namespace,
                        args.cache_mount_path,
                        args.cache_locality,
                        entry["sha256"],
                    )
                row["cacheEvidence"]["casProof"] = cas_proof
                if args.worker_dd_reads and pattern == "sequential":
                    dd_sample = run_worker_dd_read(
                        args,
                        sandbox,
                        token,
                        workspace_fuse_root(args, workspace, volume_id),
                        geesefs_log_path,
                        access_type,
                        entry,
                        index,
                    )
                    index += 1
                    print_sample(dd_sample)
                    row["cacheEvidence"]["workerDDRead"] = dd_sample
                if args.sandbox_dd_reads and access_type == "volume_mount" and pattern == "sequential":
                    sandbox_dd_sample = run_sandbox_dd_read(
                        args,
                        sandbox,
                        token,
                        root,
                        geesefs_log_path,
                        access_type,
                        entry,
                        index,
                    )
                    index += 1
                    print_sample(sandbox_dd_sample)
                    row["cacheEvidence"]["sandboxDDRead"] = sandbox_dd_sample
            rows.append(row)

    return rows, samples, evidence, index


def run_one(args, sandbox, token, workspace, access_type, root, entry, pattern, cache_state, index, geesefs_log_path=None):
    if access_type == "workspace_fuse":
        return run_workspace_fuse_read(args, sandbox, token, root, entry, pattern, cache_state, index, geesefs_log_path)
    return run_sandbox_read(args, sandbox, token, access_type, root, entry, pattern, cache_state, index, geesefs_log_path)


def write_markdown_report(path, report):
    if report["config"].get("payloadMode") == "volume_generate":
        image_line = f"SDK `{report['config']['runtimePythonVersion']}` image id `{report['sandbox'].get('imageId', '')}`"
    else:
        image_line = f"`{report['config']['imageUri']}`"
    mode = report["config"].get("benchmarkMode", "warm_cache_hit")
    strict_storage = mode == "strict_disk_cache_hit"

    lines = [
        "# Cache Benchmark Report",
        "",
        f"- Started: `{report['startedAt']}`",
        f"- Finished: `{report['finishedAt']}`",
        f"- Namespace: `{report['config']['namespace']}`",
        f"- Image: {image_line}",
        f"- Volume: `{report['config']['volumeName']}` mounted at `/volumes/{report['config']['volumeMountPath']}`",
        f"- Mode: `{mode}`",
        "",
        "## Benchmark Semantics",
        "",
        (
            "- Storage hot-read rows are strict embedded-disk-cache measurements: the benchmark skips pre-hot storage cold reads, waits for embedded CAS page files, evicts both CAS page files and the mounted FUSE file with `posix_fadvise(..., DONTNEED)`, then times the first mounted read and requires geesefs external-page diagnostics. CAS byte-hash proof is delayed until after the timed read."
            if strict_storage
            else "- Storage hot-read rows may include warmed geesefs mmap or kernel page-cache hits unless the strict disk-cache flags are enabled."
        ),
        "- CAS proof verifies content correctness, but reading CAS page files can warm page cache; the benchmark treats CAS proof as a post-read correctness gate, not as a pre-read readiness gate.",
        "- Worker-pod `dd` rows are mounted-read probes after the hot read and CAS proof. They prove the geesefs path can drain cached data, but they are not strict cold-page disk-cache throughput.",
        "- Remote raw proof reads from a different worker's advertised cache endpoint and verifies the returned SHA-256, separating over-the-wire cache throughput from FUSE/Python read overhead.",
        "",
        "| Access | Pattern | Size | Cold MB/s | Hot MB/s | Hot File MB/s | Cold ms | Hot ms | Speedup |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in report["rows"]:
        cold = row["cold"]
        hot = row["hot"]
        speedup = None
        if cold.get("durationMs") and hot.get("durationMs"):
            speedup = cold["durationMs"] / hot["durationMs"]
        lines.append(
            "| "
            f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
            f"{num(cold.get('mbps'))} | {num(hot.get('mbps'))} | {num(hot.get('fileReadMBps'))} | "
            f"{num(cold.get('durationMs'))} | {num(hot.get('durationMs'))} | {num(speedup)} |"
        )

    lines.extend(
        [
            "",
            "## Hot Read Timing",
            "",
            "| Access | Pattern | Size | Hot Mode | Total MB/s | File-read MB/s | Open ms | File-read ms | Verify ms |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in report["rows"]:
        hot = row["hot"]
        timing = hot.get("readTiming") or {}
        hot_mode = row_hot_mode(report["config"], row)
        lines.append(
            "| "
            f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
            f"`{hot_mode}` | "
            f"{num(hot.get('mbps'))} | {num(hot.get('fileReadMBps'))} | "
            f"{num(timing.get('openMs'))} | {num(timing.get('fileReadMs'))} | {num(timing.get('verifyMs'))} |"
        )

    read_path_rows = []
    image_read_path_rows = []
    for row in report["rows"]:
        for sample in row.get("samples", {}).get("hot", []):
            proof = sample.get("readPathProof")
            if proof:
                if proof.get("kind") == "image_archive" or row["accessType"] == "image_archive":
                    image_read_path_rows.append((row, sample, proof))
                else:
                    read_path_rows.append((row, sample, proof))
    if read_path_rows:
        lines.extend(
            [
                "",
                "## Hot Read Path Proof",
                "",
                "| Access | Pattern | Size | Cache Proof | External Hits | External MiB | Cache Events OK | Cache Event MiB | FUSE Responses | FUSE Response ms | FUSE Handler ms | Content Cloud Lines | Window Cloud Reads |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row, _, proof in read_path_rows:
            geesefs_summary = proof.get("geesefsSummary") or {}
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"{proof.get('ok', False)} | "
                f"{proof.get('externalPageHitLines', 0)} | "
                f"{num(proof.get('externalPageHitBytes', 0) / (1024 * 1024))} | "
                f"{geesefs_summary.get('cacheEventOk', 0)} | "
                f"{num(geesefs_summary.get('cacheEventMiB', 0))} | "
                f"{proof.get('fuseResponseLines', 0)} | "
                f"{num(proof.get('fuseResponseMs'))} | "
                f"{num(proof.get('fuseHandlerMs'))} | "
                f"{proof.get('contentCloudReadLines', 0)} | "
                f"{geesefs_summary.get('cloudRequests', 0)} |"
            )
    if image_read_path_rows:
        lines.extend(
            [
                "",
                "## Hot Image Read Path Proof",
                "",
                "| Access | Pattern | Size | Cache Proof | Read MiB | Embedded MiB | FD MiB | Page FD MiB | Disk FD MiB | Registry MiB | Sources |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row, _, proof in image_read_path_rows:
            source_bytes = proof.get("sourceBytes") or {}
            sources = ", ".join(f"{key}:{num(value / (1024 * 1024))}MiB" for key, value in sorted(source_bytes.items()))
            safe_sources = sources.replace("|", "\\|")
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"{proof.get('ok', False)} | "
                f"{num(proof.get('bytesRead', 0) / (1024 * 1024))} | "
                f"{num(proof.get('embeddedCacheBytes', 0) / (1024 * 1024))} | "
                f"{num(proof.get('fdBytes', 0) / (1024 * 1024))} | "
                f"{num(proof.get('contentCachePageFdBytes', 0) / (1024 * 1024))} | "
                f"{num(proof.get('diskCacheFdBytes', 0) / (1024 * 1024))} | "
                f"{num(proof.get('registryBytes', 0) / (1024 * 1024))} | "
                f"{safe_sources} |"
            )

    readiness_rows = []
    for row in report["rows"]:
        proof = row.get("cacheEvidence", {}).get("preHotReady")
        if proof:
            readiness_rows.append((row, proof))
    if readiness_rows:
        lines.extend(
            [
                "",
                "## Pre-Hot Cache Readiness",
                "",
                "| Access | Pattern | Size | Ready | Exists | Bytes | Pod | Error |",
                "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
            ]
        )
        for row, proof in readiness_rows:
            error = str(proof.get("error") or "").replace("|", "\\|")
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"{proof.get('ready', False)} | {proof.get('exists', False)} | "
                f"{proof.get('bytes', '')} | `{proof.get('pod', '')}` | {error} |"
            )

    eviction_rows = []
    for row in report["rows"]:
        evidence = row.get("cacheEvidence", {})
        for phase, proof in (
            ("before_hot", evidence.get("preHotEviction")),
            ("before_remote", evidence.get("preRemoteEviction")),
        ):
            if proof:
                eviction_rows.append((row, phase, proof))
    if eviction_rows:
        lines.extend(
            [
                "",
                "## Cache Page Eviction Proof",
                "",
                "| Access | Pattern | Size | Phase | Evicted | Bytes | Chunks | Matching Size |",
                "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for row, phase, proof in eviction_rows:
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"{phase} | {proof.get('ok', False)} | {proof.get('totalBytes', 0)} | "
                f"{proof.get('totalChunks', 0)} | {proof.get('bytesMatchExpected', False)} |"
            )

    mounted_eviction_rows = []
    for row in report["rows"]:
        proof = row.get("cacheEvidence", {}).get("preHotMountedEviction")
        if proof:
            mounted_eviction_rows.append((row, proof))
    if mounted_eviction_rows:
        lines.extend(
            [
                "",
                "## Mounted File Eviction Proof",
                "",
                "| Access | Pattern | Size | Evicted | Matching Size | Path |",
                "| --- | --- | ---: | ---: | ---: | --- |",
            ]
        )
        for row, proof in mounted_eviction_rows:
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"{proof.get('ok', False)} | {proof.get('bytesMatchExpected', False)} | "
                f"`{proof.get('path', '')}` |"
            )

    counters = report["cacheEvidence"]["logs"].get("counters", {})
    lines.extend(["", "## Cache Counters", "", "| Counter | Count |", "| --- | ---: |"])
    for key in sorted(counters):
        lines.append(f"| {key} | {counters[key]} |")

    proofs = report["cacheEvidence"].get("knownHashes", {})
    if proofs:
        lines.extend(["", "## CAS Proof", "", "| Name | Hash | Exists | Bytes | Match |", "| --- | --- | ---: | ---: | ---: |"])
        for name, proof in proofs.items():
            lines.append(
                f"| {name} | `{proof.get('hash', '')}` | {proof.get('exists', False)} | "
                f"{proof.get('bytes', '')} | {proof.get('matchesHash', False)} |"
            )

    remote_proofs = []
    for row in report["rows"]:
        proof = row.get("cacheEvidence", {}).get("remoteRawRead")
        if proof:
            remote_proofs.append((row, proof))
    if remote_proofs:
        lines.extend(
            [
                "",
                "## Remote Raw Read Proof",
                "",
                "| Access | Pattern | Size | Harness | Source Pod | Target Pod | Target | Total MB/s | Socket MB/s | Concurrency | Verify ms | Bytes | Match |",
                "| --- | --- | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row, proof in remote_proofs:
            target = proof.get("targetAddr") or cache_host_addr(proof.get("targetHost", {}))
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"{proof.get('harness', '')} | "
                f"`{proof.get('sourcePod', '')}` | `{(proof.get('targetPod') or {}).get('name', '')}` | "
                f"`{target}` | {num(proof.get('mbps'))} | {num(proof.get('socketReadMBps'))} | "
                f"{proof.get('concurrency', '')} | "
                f"{num(proof.get('verifyMs'))} | "
                f"{proof.get('bytes', '')} | {proof.get('matchesHash', False)} |"
            )

    dd_reads = []
    for row in report["rows"]:
        dd_read = row.get("cacheEvidence", {}).get("workerDDRead")
        if dd_read:
            dd_reads.append((row, dd_read))
    if dd_reads:
        lines.extend(
            [
                "",
                "## Worker Pod DD Read",
                "",
                "| Access | Pattern | Size | Worker Pod | Direct | Block Size | MB/s | Bytes | Cache Proof | External Hit Lines | External Hit Bytes |",
                "| --- | --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row, dd_read in dd_reads:
            cache_proof = dd_read.get("cacheProof") or {}
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"`{dd_read.get('workerPod', '')}` | {dd_read.get('directIO', False)} | "
                f"`{dd_read.get('blockSize', '')}` | {num(dd_read.get('mbps'))} | "
                f"{dd_read.get('bytesRead', '')} | {cache_proof.get('ok', False)} | "
                f"{cache_proof.get('externalPageHitLines', 0)} | {cache_proof.get('externalPageHitBytes', 0)} |"
            )

    sandbox_dd_reads = []
    for row in report["rows"]:
        dd_read = row.get("cacheEvidence", {}).get("sandboxDDRead")
        if dd_read:
            sandbox_dd_reads.append((row, dd_read))
    if sandbox_dd_reads:
        lines.extend(
            [
                "",
                "## Sandbox DD Read",
                "",
                "| Access | Pattern | Size | Direct | Block Size | MB/s | Bytes |",
                "| --- | --- | ---: | ---: | --- | ---: | ---: |",
            ]
        )
        for row, dd_read in sandbox_dd_reads:
            lines.append(
                "| "
                f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
                f"{dd_read.get('directIO', False)} | "
                f"`{dd_read.get('blockSize', '')}` | {num(dd_read.get('mbps'))} | "
                f"{dd_read.get('bytesRead', '')} |"
            )

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def num(value):
    if value is None:
        return ""
    return f"{value:.2f}" if isinstance(value, float) else str(value)


def row_hot_mode(config, row):
    if row["accessType"] not in {"volume_mount", "workspace_fuse"}:
        return "cachefs_hot"
    if config.get("benchmarkMode") == "strict_disk_cache_hit":
        return "strict_disk_cache_hit"
    if config.get("evictCachePagesBeforeHot") and config.get("resetWorkersBeforeHot"):
        return "evicted_disk_cache_hit"
    return "warm_mmap_or_page_cache_hit"


def print_table(rows):
    print("\nCache benchmark:")
    print("| Access | Pattern | Size | Cold MB/s | Hot MB/s | Hot File MB/s | Cold ms | Hot ms | Speedup |")
    print("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in rows:
        cold = row["cold"]
        hot = row["hot"]
        speedup = cold["durationMs"] / hot["durationMs"] if cold.get("durationMs") and hot.get("durationMs") else None
        print(
            f"| {row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
            f"{num(cold.get('mbps'))} | {num(hot.get('mbps'))} | {num(hot.get('fileReadMBps'))} | "
            f"{num(cold.get('durationMs'))} | {num(hot.get('durationMs'))} | {num(speedup)} |"
        )


def merge_evidence(target, source):
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            target[key].update(value)
        else:
            target[key] = value


def collect_known_hashes(args, rows, logs, image_id=""):
    known = {}
    for row in rows:
        if row["accessType"] in {"volume_mount", "workspace_fuse"}:
            name = f"{row['accessType']}:{row['pattern']}:{row['fileSizeMiB']}mib"
            if name not in known:
                known[name] = row.get("cacheEvidence", {}).get("casProof") or cache_object_proof(
                    args.namespace,
                    args.cache_mount_path,
                    args.cache_locality,
                    row["expectedSha256"],
                )

    for operation in logs.get("imageArchiveLoads", []):
        if operation.get("hashes"):
            image_hash = operation["hashes"][0]
            known["imageArchive"] = cache_object_proof(
                args.namespace,
                args.cache_mount_path,
                args.cache_locality,
                image_hash,
            )
            break
    if any(row["accessType"] == "image_archive" for row in rows) and "imageArchive" not in known:
        known["imageArchive"] = image_archive_cachefs_proof(args, image_id)
    return known


def cache_error_counters(counters):
    error_keys = {
        "storeFromContentErr",
        "hostNotFound",
        "sourceNotFound",
        "unableToPopulate",
        "deadlineExceeded",
        "cacheNoHosts",
    }
    return {key: counters.get(key, 0) for key in sorted(error_keys) if counters.get(key, 0)}


def cache_proof_failures(rows, known_hashes):
    failures = []
    for row in rows:
        if row["accessType"] in {"volume_mount", "workspace_fuse"}:
            proof = row.get("cacheEvidence", {}).get("casProof", {})
            if not proof.get("matchesHash"):
                failures.append(f"{row['key']} missing matching CAS proof for {row['expectedSha256']}")
    if any(row["accessType"] == "image_archive" for row in rows):
        image_proof = known_hashes.get("imageArchive")
        if not image_proof or not image_proof.get("matchesHash"):
            failures.append(f"imageArchive missing matching CAS proof for {(image_proof or {}).get('hash')}")
    return failures


def sample_correctness_failures(args, samples):
    failures = []
    for sample in samples:
        if not sample.get("ok"):
            continue
        if sample.get("accessType") == "volume_prepare":
            continue

        expected_size = sample.get("fileSizeBytes")
        bytes_read = sample.get("bytesRead")
        if expected_size is not None and bytes_read != expected_size:
            failures.append(
                f"{sample.get('accessType')}:{sample.get('pattern')}:{sample.get('cacheState')} "
                f"read {bytes_read} bytes, expected {expected_size}"
            )

        duration_ms = sample.get("readDurationMs")
        mbps = sample.get("mbps")
        if bytes_read and duration_ms and mbps is not None:
            expected_mbps = (bytes_read / (1024 * 1024)) / (duration_ms / 1000)
            tolerance = max(0.01, expected_mbps * 0.005)
            if abs(expected_mbps - mbps) > tolerance:
                failures.append(
                    f"{sample.get('accessType')}:{sample.get('pattern')}:{sample.get('cacheState')} "
                    f"reported MB/s {mbps:.2f} did not match bytes/time {expected_mbps:.2f}"
                )

        if args.require_verified_reads:
            digest = sample.get("digest")
            expected_digest = sample.get("expectedSha256")
            if not sample.get("verified"):
                failures.append(
                    f"{sample.get('accessType')}:{sample.get('pattern')}:{sample.get('cacheState')} "
                    "did not run verified content reads"
                )
            elif sample.get("pattern") == "sequential" and (not digest or digest != expected_digest):
                failures.append(
                    f"{sample.get('accessType')}:{sample.get('pattern')}:{sample.get('cacheState')} "
                    f"did not verify full-file digest: got={digest} want={expected_digest}"
                )
            elif sample.get("pattern") == "random" and not digest:
                failures.append(
                    f"{sample.get('accessType')}:{sample.get('pattern')}:{sample.get('cacheState')} "
                    "did not report verified random-read trace digest"
                )
    return failures


def throughput_failures(args, rows):
    failures = []
    if args.min_hot_mbps <= 0 and args.min_hot_file_read_mbps <= 0:
        return failures
    for row in rows:
        if row_size_mib(row) < args.min_hot_file_read_size_mb:
            continue
        hot_mbps = row.get("hot", {}).get("mbps")
        if args.min_hot_mbps > 0 and (hot_mbps is None or hot_mbps < args.min_hot_mbps):
            failures.append(
                f"{row['key']} hot throughput {num(hot_mbps)} MB/s was below required "
                f"{args.min_hot_mbps:.2f} MB/s"
            )
        file_read_mbps = row.get("hot", {}).get("fileReadMBps")
        if args.min_hot_file_read_mbps > 0 and (
            file_read_mbps is None or file_read_mbps < args.min_hot_file_read_mbps
        ):
            failures.append(
                f"{row['key']} hot file-read throughput {num(file_read_mbps)} MB/s was below required "
                f"{args.min_hot_file_read_mbps:.2f} MB/s"
            )
    return failures


def remote_cache_read_failures(args, rows):
    failures = []
    if not args.require_remote_cache_read:
        return failures
    for row in rows:
        if row["accessType"] not in {"volume_mount", "workspace_fuse"}:
            continue
        if row_size_mib(row) < args.min_remote_cache_socket_size_mb:
            continue
        proof = row.get("cacheEvidence", {}).get("remoteRawRead")
        if not proof:
            failures.append(f"{row['key']} missing remote raw read proof")
            continue
        if not proof.get("ok") or not proof.get("matchesHash"):
            failures.append(f"{row['key']} remote raw read proof failed: {proof.get('error') or proof}")
            continue
        if not proof.get("differentWorkerPod"):
            failures.append(
                f"{row['key']} remote raw read proof did not execute from a different worker pod: "
                f"source={proof.get('sourcePod')} target={(proof.get('targetPod') or {}).get('name')}"
            )
            continue
        if args.min_remote_cache_mbps > 0 and (proof.get("mbps") or 0) < args.min_remote_cache_mbps:
            failures.append(
                f"{row['key']} remote raw read throughput {num(proof.get('mbps'))} MB/s was below required "
                f"{args.min_remote_cache_mbps:.2f} MB/s"
            )
        if args.min_remote_cache_socket_mbps > 0 and (
            proof.get("socketReadMBps") or 0
        ) < args.min_remote_cache_socket_mbps:
            failures.append(
                f"{row['key']} remote raw socket throughput {num(proof.get('socketReadMBps'))} MB/s was below required "
                f"{args.min_remote_cache_socket_mbps:.2f} MB/s"
            )
    return failures


def worker_dd_read_failures(args, rows):
    failures = []
    if not args.worker_dd_reads:
        return failures
    for row in rows:
        if row["accessType"] not in {"volume_mount", "workspace_fuse"} or row["pattern"] != "sequential":
            continue
        dd_read = row.get("cacheEvidence", {}).get("workerDDRead")
        if not dd_read:
            failures.append(f"{row['key']} missing worker-pod dd read proof")
            continue
        if not dd_read.get("ok"):
            failures.append(f"{row['key']} worker-pod dd read failed: {dd_read.get('error') or dd_read}")
            continue
        if dd_read.get("bytesRead") != row["fileSizeBytes"]:
            failures.append(
                f"{row['key']} worker-pod dd read {dd_read.get('bytesRead')} bytes, "
                f"expected {row['fileSizeBytes']}"
            )
            continue
        proof = dd_read.get("cacheProof")
        if not proof:
            failures.append(f"{row['key']} worker-pod dd read missing geesefs/cache proof")
            continue
        if not proof.get("ok"):
            failures.append(
                f"{row['key']} worker-pod dd did not prove geesefs -> embedded cache path: "
                f"external_hits={proof.get('externalPageHitLines')} "
                f"external_hit_bytes={proof.get('externalPageHitBytes')} "
                f"geesefs_summary={proof.get('geesefsSummary')} cache_summary={proof.get('cacheSummary')} "
                f"no_content_cloud={proof.get('noContentCloudRead')} "
                f"window_cloud={proof.get('noCloudReadInWindow')}"
            )
    return failures


def sandbox_dd_read_failures(args, rows):
    failures = []
    if not args.sandbox_dd_reads:
        return failures
    for row in rows:
        if row["accessType"] != "volume_mount" or row["pattern"] != "sequential":
            continue
        dd_read = row.get("cacheEvidence", {}).get("sandboxDDRead")
        if not dd_read:
            failures.append(f"{row['key']} missing sandbox dd read proof")
            continue
        if not dd_read.get("ok"):
            failures.append(f"{row['key']} sandbox dd read failed: {dd_read.get('error') or dd_read}")
            continue
        if dd_read.get("bytesRead") != row["fileSizeBytes"]:
            failures.append(
                f"{row['key']} sandbox dd read {dd_read.get('bytesRead')} bytes, "
                f"expected {row['fileSizeBytes']}"
            )
    return failures


def cache_page_eviction_failures(args, rows):
    failures = []
    if not args.require_cache_page_eviction:
        return failures
    for row in rows:
        if row["accessType"] not in {"volume_mount", "workspace_fuse"}:
            continue
        evidence = row.get("cacheEvidence", {})
        required = []
        if args.evict_cache_pages_before_hot:
            required.append(("before hot read", evidence.get("preHotEviction")))
        if args.evict_cache_pages_before_remote_proof and args.require_remote_cache_read:
            required.append(("before remote raw proof", evidence.get("preRemoteEviction")))
        for label, proof in required:
            if not proof or not proof.get("ok"):
                failures.append(f"{row['key']} missing successful cache page eviction {label}: {proof}")
                continue
            if proof.get("bytesMatchExpected") is False:
                failures.append(
                    f"{row['key']} cache page eviction {label} did not see expected size: "
                    f"bytes={proof.get('totalBytes')} expected={row['fileSizeBytes']}"
                )
        if args.evict_mounted_file_before_hot:
            proof = evidence.get("preHotMountedEviction")
            if not proof or not proof.get("ok"):
                failures.append(f"{row['key']} missing successful mounted-file eviction before hot read: {proof}")
            elif proof.get("bytesMatchExpected") is False:
                failures.append(
                    f"{row['key']} mounted-file eviction before hot read did not see expected size: "
                    f"expected={row['fileSizeBytes']}"
                )
    return failures


def benchmark_methodology_failures(args, rows):
    failures = []
    if not args.strict_disk_cache_hit:
        return failures
    missing = []
    required_flags = {
        "evict_cache_pages_before_hot": args.evict_cache_pages_before_hot,
        "evict_mounted_file_before_hot": args.evict_mounted_file_before_hot,
        "require_cache_page_eviction": args.require_cache_page_eviction,
        "wait_cache_ready_after_prepare": args.wait_cache_ready_after_prepare,
    }
    if args.require_remote_cache_read:
        required_flags["evict_cache_pages_before_remote_proof"] = args.evict_cache_pages_before_remote_proof
    for name, enabled in required_flags.items():
        if not enabled:
            missing.append(name)
    if missing:
        failures.append(f"strict disk cache mode missing required flag(s): {', '.join(missing)}")

    for row in rows:
        if row["accessType"] not in {"volume_mount", "workspace_fuse"}:
            continue
        evidence = row.get("cacheEvidence", {})
        if row.get("samples", {}).get("cold"):
            failures.append(f"{row['key']} strict disk hot read had a pre-hot cold storage read")
        if not (evidence.get("preHotReady") or {}).get("ready"):
            failures.append(f"{row['key']} strict disk hot read missing pre-hot cache-ready proof")
        if not (evidence.get("preHotEviction") or {}).get("bytesMatchExpected"):
            failures.append(f"{row['key']} strict disk hot read missing exact-size page-cache eviction proof")
        if not (evidence.get("preHotMountedEviction") or {}).get("bytesMatchExpected"):
            failures.append(f"{row['key']} strict disk hot read missing exact-size mounted-file eviction proof")
        if args.require_remote_cache_read and not (evidence.get("preRemoteEviction") or {}).get("bytesMatchExpected"):
            failures.append(f"{row['key']} remote raw proof missing exact-size page-cache eviction proof")
    return failures


def hot_read_path_failures(args, rows):
    failures = []
    for row in rows:
        if row["accessType"] == "image_archive":
            for sample in row.get("samples", {}).get("hot", []):
                proof = sample.get("readPathProof")
                if not proof:
                    failures.append(f"{row['key']} hot read missing clip/image cache path diagnostics")
                    continue
                if not proof.get("ok"):
                    failures.append(
                        f"{row['key']} hot read did not prove CLIP -> embedded cache path: "
                        f"summary_count={proof.get('summaryCount')} "
                        f"read_bytes={proof.get('bytesRead')} "
                        f"embedded_bytes={proof.get('embeddedCacheBytes')} "
                        f"fd_bytes={proof.get('fdBytes')} "
                        f"registry_bytes={proof.get('registryBytes')} "
                        f"sources={proof.get('sourceBytes')} "
                        f"matched_lines={proof.get('matchedLines')}"
                    )
            continue
        if row["accessType"] not in {"volume_mount", "workspace_fuse"}:
            continue
        for sample in row.get("samples", {}).get("hot", []):
            proof = sample.get("readPathProof")
            if not proof:
                failures.append(f"{row['key']} hot read missing geesefs/cache path diagnostics")
                continue
            if not proof.get("ok"):
                failures.append(
                    f"{row['key']} hot read did not prove geesefs -> embedded cache path: "
                    f"external_hits={proof.get('externalPageHitLines')} "
                    f"external_hit_bytes={proof.get('externalPageHitBytes')} "
                    f"fuse_responses={proof.get('fuseResponseLines')} "
                    f"geesefs_summary={proof.get('geesefsSummary')} "
                    f"cache_summary={proof.get('cacheSummary')} "
                    f"no_content_cloud={proof.get('noContentCloudRead')} "
                    f"window_cloud={proof.get('noCloudReadInWindow')}"
                )
            elif (
                args.strict_disk_cache_hit
                and proof.get("externalPageLocalRegionHitLines", 0) <= 0
                and proof.get("externalPageReadIntoHitLines", 0) <= 0
                and not (
                    proof.get("externalPageMmapCacheHitLines", 0) > 0
                    and (row.get("cacheEvidence", {}).get("preHotMountedEviction") or {}).get("bytesMatchExpected")
                )
            ):
                failures.append(
                    f"{row['key']} strict hot read did not prove an embedded-cache read-into, local page-region, or evicted mmap-cache lookup; "
                    f"external_local_region_hits={proof.get('externalPageLocalRegionHitLines')} "
                    f"external_read_into_hits={proof.get('externalPageReadIntoHitLines', 0)} "
                    f"external_mmap_hits={proof.get('externalPageMmapCacheHitLines')} "
                    f"matched_lines={proof.get('matchedLines')}"
                )
    return failures


def main():
    args = parse_args()
    require_tools("kubectl")
    started_at = now_rfc3339()

    if args.install:
        install_overlay(args.namespace, args.timeout_seconds)
    if args.port_forward:
        start_http_port_forward_if_needed(args.namespace, args.gateway_url, args.timeout_seconds)
        start_grpc_port_forward_if_needed(args)
        start_localstack_port_forward_if_needed(args)
    else:
        wait_http(args.gateway_url, HEALTH_PATH, timeout_seconds=args.timeout_seconds)

    load_cached_token(args)
    token, workspace_id = authorize_benchmark(args)
    save_cached_token(args, token)
    args.token = token
    if workspace_id:
        log(f"Authorized workspace {workspace_id}")
    workspace = verify_workspace_storage(args, token)

    build_info = build_payload_image(args)
    write_sdk_config(args)
    if args.reset_workers:
        log("Deleting worker jobs before cache benchmark")
        delete_workers(args.namespace)
    cleanup_before = None
    if args.cleanup_cache_before_run:
        log("Cleaning refillable worker cache artifacts before cache benchmark")
        cleanup_before = cleanup_worker_cache_artifacts(args)

    redis_pod = find_redis_pod(args.namespace)
    disk_before = cache_disk_stats(args.namespace, args.cache_mount_path)
    hosts_before = cache_hosts_snapshot(args.namespace, redis_pod, args.cache_locality)

    sandbox, volume, prepare_ms = prepare_sandbox_runtime(args)
    manifest = build_info["manifest"]
    args.payload_manifest = manifest
    rows = []
    samples = []
    benchmark_evidence = {"image": {"imageId": getattr(sandbox, "image_id", "")}, "volume": {"volumeId": volume_external_id(volume)}}
    prepare_sample = None
    index = 1
    image_access_types = [access_type for access_type in args.access_type_list if access_type == "image_archive"]
    storage_access_types = [access_type for access_type in args.access_type_list if access_type != "image_archive"]

    print_sample_header()
    if image_access_types:
        batch_rows, batch_samples, batch_evidence, index = run_matrix(
            args, sandbox, token, workspace, volume, manifest, image_access_types, index
        )
        rows.extend(batch_rows)
        samples.extend(batch_samples)
        merge_evidence(benchmark_evidence, batch_evidence)

    if storage_access_types:
        prepare_sample = run_volume_prepare(args, sandbox, token, index, volume_external_id(volume))
        index += 1
        print_sample(prepare_sample)
        if not prepare_sample.get("ok"):
            raise SystemExit(f"volume prepare failed: {prepare_sample.get('error')}")
        if prepare_sample["manifest"] != manifest:
            raise SystemExit("volume prepare manifest did not match generated image manifest")
        if args.wait_storage_ready_after_prepare:
            log("Waiting for prepared storage files to become visible through worker geesefs mount")
            storage_ready = wait_storage_root_ready(
                args,
                sandbox,
                token,
                workspace,
                volume_external_id(volume),
                manifest,
                set(storage_access_types),
            )
            benchmark_evidence["storageReadyAfterPrepare"] = storage_ready
            if not storage_ready.get("ready"):
                raise SystemExit(f"prepared storage files did not become visible after volume prepare: {storage_ready}")
        if args.reset_workers_after_prepare:
            if args.wait_cache_ready_after_prepare:
                log("Waiting for prepared storage files to appear in embedded cache")
                for entry in manifest["files"]:
                    if entry.get("accessType") not in storage_access_types:
                        continue
                    log(
                        "Waiting for cache-ready storage file "
                        f"{entry['path']} ({entry['sizeMiB']} MiB, hash={entry['sha256']})"
                    )
                    ready = wait_cache_object_ready(args, entry["sha256"], entry["size"])
                    if not ready.get("ready"):
                        raise SystemExit(
                            "prepared storage file did not become cache-ready before worker reset: "
                            f"{entry['path']} hash={entry['sha256']} proof={ready}"
                        )
            log("Deleting worker jobs after volume prepare to clear geesefs/FUSE in-memory state")
            delete_workers(args.namespace)
            if args.settle_seconds > 0:
                time.sleep(args.settle_seconds)

        batch_rows, batch_samples, batch_evidence, index = run_matrix(
            args, sandbox, token, workspace, volume, manifest, storage_access_types, index
        )
        rows.extend(batch_rows)
        samples.extend(batch_samples)
        merge_evidence(benchmark_evidence, batch_evidence)

    logs = cache_log_counters(args.namespace, started_at, args.log_tail)
    known_hashes = collect_known_hashes(args, rows, logs, getattr(sandbox, "image_id", ""))
    disk_after = cache_disk_stats(args.namespace, args.cache_mount_path)
    hosts_after = cache_hosts_snapshot(args.namespace, redis_pod, args.cache_locality)
    print_table(rows)
    cleanup_after = None
    if args.cleanup_cache_after_run:
        log("Cleaning refillable worker cache artifacts after cache benchmark")
        cleanup_after = cleanup_worker_cache_artifacts(args)

    report = {
        "startedAt": started_at,
        "finishedAt": now_rfc3339(),
        "config": {
            "namespace": args.namespace,
            "gatewayUrl": args.gateway_url,
            "grpcAddr": grpc_addr(args),
            "imageUri": args.image_uri,
            "hostImageUri": build_info["hostImageUri"],
            "sourceImage": args.source_image,
            "payloadMode": "volume_generate" if args.generate_volume_payload else "image_layer",
            "runtimePythonVersion": args.runtime_python_version,
            "sizesMiB": args.sizes,
            "patterns": args.pattern_list,
            "accessTypes": args.access_type_list,
            "filePlan": args.file_plan,
            "fileCount": len(manifest["files"]),
            "iterations": args.iterations,
            "randomBlockBytes": args.random_block_bytes,
            "verifyReads": args.verify_reads,
            "requireVerifiedReads": args.require_verified_reads,
            "requireRemoteCacheRead": args.require_remote_cache_read,
            "requireReadPathProof": args.require_read_path_proof,
            "benchmarkMode": "strict_disk_cache_hit" if args.strict_disk_cache_hit else "warm_cache_hit",
            "strictDiskCacheHit": args.strict_disk_cache_hit,
            "resetWorkersBeforeHot": args.reset_workers_before_hot,
            "resetWorkersBeforeImageHot": args.reset_workers_before_image_hot,
            "evictCachePagesBeforeHot": args.evict_cache_pages_before_hot,
            "evictCachePagesBeforeRemoteProof": args.evict_cache_pages_before_remote_proof,
            "requireCachePageEviction": args.require_cache_page_eviction,
            "waitCacheReadyAfterPrepare": args.wait_cache_ready_after_prepare,
            "remoteCacheProofChunkBytes": args.remote_cache_proof_chunk_bytes,
            "remoteCacheProofConcurrency": args.remote_cache_proof_concurrency,
            "workerDDReads": args.worker_dd_reads,
            "sandboxDDReads": args.sandbox_dd_reads,
            "workerDDBlockSize": args.worker_dd_block_size,
            "workerDDLogWaitSeconds": args.worker_dd_log_wait_seconds,
            "cleanupCacheBeforeRun": args.cleanup_cache_before_run,
            "cleanupCacheAfterRun": args.cleanup_cache_after_run,
            "readPathLogWaitSeconds": args.read_path_log_wait_seconds,
            "sandboxCpu": args.sandbox_cpu,
            "sandboxMemory": args.sandbox_memory,
            "minHotMbps": args.min_hot_mbps,
            "minHotFileReadMbps": args.min_hot_file_read_mbps,
            "minHotFileReadSizeMiB": args.min_hot_file_read_size_mb,
            "minRemoteCacheMbps": args.min_remote_cache_mbps,
            "minRemoteCacheSocketMbps": args.min_remote_cache_socket_mbps,
            "minRemoteCacheSocketSizeMiB": args.min_remote_cache_socket_size_mb,
            "volumeName": args.volume_name,
            "volumeId": volume_external_id(volume),
            "volumeMountPath": args.volume_mount_path,
            "volumeSubdir": args.volume_subdir,
            "cacheMountPath": args.cache_mount_path,
            "workspaceName": workspace_name(workspace),
        },
        "workspace": workspace,
        "sandbox": {
            "stubId": getattr(sandbox, "stub_id", ""),
            "imageId": getattr(sandbox, "image_id", ""),
            "prepareMs": prepare_ms,
        },
        "volumePrepare": prepare_sample,
        "rows": rows,
        "samples": samples,
        "cacheEvidence": {
            "diskBefore": disk_before,
            "diskAfter": disk_after,
            "hostsBefore": hosts_before,
            "hostsAfter": hosts_after,
            "logs": logs,
            "knownHashes": known_hashes,
            "benchmarkEvidence": benchmark_evidence,
            "cleanupBeforeRun": cleanup_before,
            "cleanupAfterRun": cleanup_after,
        },
        "cluster": cluster_snapshot(args.namespace),
    }

    prepare_samples = [prepare_sample] if prepare_sample else []
    failures = [sample for sample in samples + prepare_samples if not sample.get("ok")]
    cache_errors = cache_error_counters(logs.get("counters", {}))
    if cache_errors:
        failures.append({"ok": False, "error": f"cache error counters were non-zero: {cache_errors}"})
    proof_failures = cache_proof_failures(rows, known_hashes)
    if proof_failures:
        failures.append({"ok": False, "error": "; ".join(proof_failures)})
    sample_failures = sample_correctness_failures(args, samples)
    if sample_failures:
        failures.append({"ok": False, "error": "; ".join(sample_failures)})
    hot_throughput_failures = throughput_failures(args, rows)
    if hot_throughput_failures:
        failures.append({"ok": False, "error": "; ".join(hot_throughput_failures)})
    remote_failures = remote_cache_read_failures(args, rows)
    if remote_failures:
        failures.append({"ok": False, "error": "; ".join(remote_failures)})
    if args.require_read_path_proof:
        path_failures = hot_read_path_failures(args, rows)
        if path_failures:
            failures.append({"ok": False, "error": "; ".join(path_failures)})
    dd_failures = worker_dd_read_failures(args, rows)
    if dd_failures:
        failures.append({"ok": False, "error": "; ".join(dd_failures)})
    sandbox_dd_failures = sandbox_dd_read_failures(args, rows)
    if sandbox_dd_failures:
        failures.append({"ok": False, "error": "; ".join(sandbox_dd_failures)})
    eviction_failures = cache_page_eviction_failures(args, rows)
    if eviction_failures:
        failures.append({"ok": False, "error": "; ".join(eviction_failures)})
    methodology_failures = benchmark_methodology_failures(args, rows)
    if methodology_failures:
        failures.append({"ok": False, "error": "; ".join(methodology_failures)})
    report["validationFailures"] = failures

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown_report(args.report, report)
    log(f"Wrote cache benchmark JSON to {output_path}")
    log(f"Wrote cache benchmark report to {args.report}")

    if failures:
        for failure in failures:
            error = failure.get("error") if isinstance(failure, dict) else failure
            if error:
                log(f"Validation failure: {error}")
        raise SystemExit(f"{len(failures)} cache benchmark run(s) failed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
