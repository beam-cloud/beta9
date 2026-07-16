#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"

if have_cmd crictl; then
  log_pass "crictl command available"
else
  log_warn "crictl not found; k3s may still include it under /var/lib/rancher/k3s/data"
fi

if have_cmd docker; then
  log_pass "docker command available"
  if systemctl is-active --quiet docker 2>/dev/null; then
    log_pass "docker service is active"
  else
    log_warn "docker command exists but service is not active"
  fi
else
  if is_gpu; then
    log_warn "docker is not installed; required only for agent-backed worker-container mode, not local k3s pools"
  else
    log_skip "docker not required for role=$ROLE local k3s path"
  fi
fi

if [ -S /run/k3s/containerd/containerd.sock ] || [ -S /run/containerd/containerd.sock ]; then
  log_pass "containerd socket detected"
else
  log_warn "containerd socket not detected"
fi

if is_gpu; then
  if have_cmd nvidia-ctk; then
    log_pass "nvidia-ctk command available"
  else
    log_warn "nvidia-ctk missing; NVIDIA container toolkit may be incomplete"
  fi
else
  log_skip "NVIDIA runtime checks are skipped for role=$ROLE"
fi

