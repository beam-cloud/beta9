#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"
use_k3s_kubeconfig

if ! is_master; then
  log_skip "Beta9 cluster readiness checks are master-side only"
  exit 0
fi

if ! have_cmd kubectl; then
  log_fail "kubectl is required for Beta9 readiness checks"
  exit 1
fi

if is_install; then
  if ! have_cmd helm; then
    log_fail "helm is required to install infra/beta9"
    exit 1
  fi

  chart_dir="$(cd "$SCRIPT_DIR/.." && pwd)/infra/beta9"
  helm repo add bjw-s-labs https://bjw-s-labs.github.io/helm-charts --force-update >/dev/null \
    || { log_fail "failed to configure bjw-s-labs Helm repo"; exit 1; }
  helm repo add bitnami https://charts.bitnami.com/bitnami --force-update >/dev/null \
    || { log_fail "failed to configure bitnami Helm repo"; exit 1; }
  helm repo add grafana https://grafana.github.io/helm-charts --force-update >/dev/null \
    || { log_fail "failed to configure grafana Helm repo"; exit 1; }
  helm repo add victoria-metrics https://victoriametrics.github.io/helm-charts --force-update >/dev/null \
    || { log_fail "failed to configure victoria-metrics Helm repo"; exit 1; }
  helm repo update || { log_fail "failed to update Helm repos"; exit 1; }
  helm dependency build "$chart_dir" || { log_fail "failed to build Beta9 chart dependencies"; exit 1; }
  helm upgrade --install beta9 "$chart_dir" \
    --namespace beta9 \
    --create-namespace \
    --wait \
    --timeout 15m || { log_fail "failed to install Beta9 chart"; exit 1; }
fi

if kubectl get namespace beta9 >/dev/null 2>&1; then
  log_pass "beta9 namespace exists"
else
  log_warn "beta9 namespace does not exist yet"
  exit 0
fi

if kubectl -n beta9 get deploy beta9-gateway >/dev/null 2>&1; then
  available="$(kubectl -n beta9 get deploy beta9-gateway -o jsonpath='{.status.availableReplicas}' 2>/dev/null || true)"
  if [ "${available:-0}" -ge 1 ]; then
    log_pass "beta9 gateway deployment has an available replica"
  else
    log_fail "beta9 gateway deployment is not available"
  fi
else
  log_warn "beta9 gateway deployment not found"
fi

kubectl -n beta9 get secret beta9-config-helm >/dev/null 2>&1 \
  && log_pass "beta9 Helm config secret exists" \
  || log_warn "beta9 Helm config secret not found"

kubectl -n beta9 get pods >/tmp/beam-beta9-pods.txt 2>/dev/null && log_pass "listed beta9 pods" || log_warn "could not list beta9 pods"

