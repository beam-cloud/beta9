#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"

if ! is_gpu; then
  log_skip "NVIDIA checks apply only to gpu nodes"
  exit 0
fi

if ! have_cmd nvidia-smi; then
  log_fail "nvidia-smi is missing; install NVIDIA driver before continuing"
  exit 1
fi

log_pass "nvidia-smi command available"

expected_gpu_count="${GPU_COUNT:-8}"
expected_gpu_type="${GPU_TYPE:-A6000}"
gpu_count="$(nvidia-smi -L 2>/dev/null | grep -c '^GPU ' || true)"
record_fact gpu_count "$gpu_count"
if [ "$gpu_count" -eq "$expected_gpu_count" ]; then
  log_pass "detected $expected_gpu_count NVIDIA GPUs"
else
  log_fail "expected $expected_gpu_count NVIDIA GPUs, detected $gpu_count"
fi

if nvidia-smi -L | grep -qi "$expected_gpu_type"; then
  record_fact gpu_type "$expected_gpu_type"
  log_pass "detected $expected_gpu_type GPU model"
else
  log_fail "$expected_gpu_type GPU model was not detected"
fi

driver="$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -n1 || true)"
record_fact nvidia_driver_version "$driver"
[ -n "$driver" ] && log_pass "NVIDIA driver version: $driver" || log_warn "could not determine NVIDIA driver version"

if have_cmd docker; then
  if docker info >/dev/null 2>&1; then
    log_pass "docker daemon is reachable"
    if docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi >/tmp/beam-docker-nvidia-smi.out 2>/tmp/beam-docker-nvidia-smi.err; then
      log_pass "docker GPU smoke test passed"
    else
      log_warn "docker GPU smoke test failed; local k3s GPU scheduling can still work if containerd is configured"
    fi
  else
    log_warn "docker exists but daemon is not reachable"
  fi
else
  log_skip "docker GPU smoke test skipped because docker is unavailable"
fi

