#!/bin/bash
set -euo pipefail

NAMESPACE="${BENCH_NAMESPACE:-${KUBE_NAMESPACE:-beta9}}"
KUBE_CONTEXT="${KUBE_CONTEXT:-k3d-beta9}"
KUBECTL=(kubectl --context "${KUBE_CONTEXT}")

if [[ "${KUBE_CONTEXT}" != k3d-* ]]; then
  echo "refusing to delete workers outside a local k3d context: ${KUBE_CONTEXT}" >&2
  exit 1
fi

echo "Deleting worker jobs in namespace ${NAMESPACE} on context ${KUBE_CONTEXT}..."
"${KUBECTL[@]}" -n "${NAMESPACE}" delete job -l run.beam.cloud/role=worker --ignore-not-found=true

echo "Deleting redis keys..."
if "${KUBECTL[@]}" -n "${NAMESPACE}" get sts redis-master &> /dev/null; then
  replicas=$("${KUBECTL[@]}" -n "${NAMESPACE}" get sts redis-master -o jsonpath='{.spec.replicas}')
  for i in $(seq 0 $((replicas-1))); do
    "${KUBECTL[@]}" -n "${NAMESPACE}" exec "redis-master-$i" -- bash -c 'redis_cli=$(command -v redis-cli || command -v /opt/bitnami/redis/bin/redis-cli); for pattern in "workspace:*" "provider:*" "scheduler:*" "worker:*" "pod:*"; do for k in $($redis_cli keys "$pattern"); do $redis_cli -c del "$k" >/dev/null; done; done' &
  done
fi

wait
