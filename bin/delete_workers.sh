#!/bin/bash -x

echo "Deleting scheduler keys..."

replicas=$(kubectl get sts redis-cluster -o go-template='{{.spec.replicas}}')

for i in $(seq 0 $((replicas-1))); do
  kubectl exec redis-cluster-$i -- bash -c 'for k in $(redis-cli keys scheduler:worker:state:*); do redis-cli -c del $k; done' &
  kubectl exec redis-cluster-$i -- bash -c 'for k in $(redis-cli keys scheduler:worker:requests:*); do redis-cli -c del $k; done' &
  kubectl exec redis-cluster-$i -- bash -c 'for k in $(redis-cli keys scheduler:container:*); do redis-cli -c del $k; done' &
done

wait

echo "Deleting worker jobs..."
kubectl delete job -l run.beam.cloud/role=worker