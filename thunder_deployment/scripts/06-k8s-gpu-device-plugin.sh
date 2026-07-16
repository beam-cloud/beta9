#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"
use_k3s_kubeconfig

if is_cpu; then
  log_skip "Kubernetes GPU device plugin checks skipped on cpu nodes"
  exit 0
fi

if is_gpu; then
  if have_cmd kubectl; then
    node="${K3S_NODE_NAME:-$(hostname -s)}"
    alloc="$(kubectl get node "$node" -o jsonpath='{.status.allocatable.nvidia\.com/gpu}' 2>/dev/null || true)"
    expected_gpu_count="${GPU_COUNT:-8}"
    if [ "$alloc" = "$expected_gpu_count" ]; then
      log_pass "node advertises nvidia.com/gpu=$expected_gpu_count"
    elif [ -n "$alloc" ]; then
      log_fail "node advertises nvidia.com/gpu=$alloc, expected $expected_gpu_count"
    else
      log_warn "could not read this node's GPU allocatable value; run this check from a kubeconfig-capable host or verify from master"
    fi
  else
    log_skip "kubectl unavailable on gpu node; master-side verification required"
  fi
  exit 0
fi

if is_master; then
  if ! have_cmd kubectl; then
    log_fail "kubectl is required on master to verify/install the NVIDIA runtime chart"
    exit 1
  fi

  if kubectl get runtimeclass nvidia >/dev/null 2>&1; then
    log_pass "nvidia RuntimeClass is installed"
  else
    log_warn "nvidia RuntimeClass is not installed"
  fi

  if kubectl -n kube-system get ds nvidia-device-plugin-daemonset >/dev/null 2>&1; then
    log_pass "NVIDIA device plugin daemonset is installed"
  else
    log_warn "NVIDIA device plugin daemonset is not installed"
  fi

  if kubectl -n kube-system get ds nvidia-ctk-installer >/dev/null 2>&1; then
    log_pass "NVIDIA container toolkit installer daemonset is installed"
  else
    log_warn "NVIDIA container toolkit installer daemonset is not installed"
  fi

  if is_install; then
    if ! have_cmd helm; then
      log_fail "helm is required to install infra/nvidia-runtimeclass"
      exit 1
    fi

    chart_dir="$(cd "$SCRIPT_DIR/.." && pwd)/infra/nvidia-runtimeclass"
    if ! helm -n kube-system status nvidia-runtimeclass >/dev/null 2>&1 \
      && kubectl get runtimeclass nvidia >/dev/null 2>&1; then
      kubectl delete runtimeclass nvidia || { log_fail "failed to remove unmanaged nvidia RuntimeClass"; exit 1; }
    fi
    helm upgrade --install nvidia-runtimeclass "$chart_dir" \
      --namespace kube-system \
      --create-namespace || { log_fail "failed to install NVIDIA runtime chart"; exit 1; }
  fi
fi

