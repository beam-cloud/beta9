#!/bin/bash
set -euo pipefail

NAMESPACE="${BENCH_NAMESPACE:-${KUBE_NAMESPACE:-beta9}}"

echo "Deleting worker jobs in namespace ${NAMESPACE}..."
kubectl -n "${NAMESPACE}" delete job -l run.beam.cloud/role=worker --ignore-not-found=true

echo "Deleting redis keys..."
if kubectl -n "${NAMESPACE}" get sts redis-master &> /dev/null; then
  replicas=$(kubectl -n "${NAMESPACE}" get sts redis-master -o jsonpath='{.spec.replicas}')
  for i in $(seq 0 $((replicas-1))); do
    kubectl -n "${NAMESPACE}" exec "redis-master-$i" -- bash -c 'redis_cli=$(command -v redis-cli || command -v /opt/bitnami/redis/bin/redis-cli); for pattern in "workspace:*" "provider:*" "scheduler:*" "worker:*" "pod:*"; do for k in $($redis_cli keys "$pattern"); do $redis_cli -c del "$k" >/dev/null; done; done' &
  done
fi

wait
