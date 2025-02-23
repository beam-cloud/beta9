#!/bin/bash

echo "Deleting worker jobs..."
kubectl delete job -l run.beam.cloud/role=worker

echo "Deleting redis keys..."
if kubectl get sts redis-master &> /dev/null; then
  replicas=$(kubectl get sts redis-master -o jsonpath='{.spec.replicas}')
  for i in $(seq 0 $((replicas-1))); do
    kubectl exec redis-master-$i -- bash -c 'for k in $(redis-cli keys provider:*); do redis-cli -c del $k; done' &
    kubectl exec redis-master-$i -- bash -c 'for k in $(redis-cli keys scheduler:worker:worker_index); do redis-cli -c del $k; done' &
    kubectl exec redis-master-$i -- bash -c 'for k in $(redis-cli keys scheduler:worker:state:*); do redis-cli -c del $k; done' &
    kubectl exec redis-master-$i -- bash -c 'for k in $(redis-cli keys scheduler:worker:requests:*); do redis-cli -c del $k; done' &
    kubectl exec redis-master-$i -- bash -c 'for k in $(redis-cli keys scheduler:container:*); do redis-cli -c del $k; done' &
    kubectl exec redis-master-$i -- bash -c 'for k in $(redis-cli keys worker:*); do redis-cli -c del $k; done' &
    kubectl exec redis-master-$i -- bash -c 'for k in $(redis-cli keys pod:*); do redis-cli -c del $k; done' &
  done
fi

wait
