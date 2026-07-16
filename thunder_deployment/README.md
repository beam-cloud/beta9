# Beam deployment notes

## beta0 setep

This deployment runs the open-source Beta9/Beam components from public sources and public container images. No private Beam registry access was needed.

Versions used:

- Gateway source release: `gateway-0.1.714`
  - `https://github.com/beam-cloud/beta9/archive/refs/tags/gateway-0.1.714.tar.gz`
  - image: `public.ecr.aws/n4e0e1y0/beta9-gateway:0.1.714`
- Worker source release: `worker-0.1.682`
  - `https://github.com/beam-cloud/beta9/archive/refs/tags/worker-0.1.682.tar.gz`
  - image: `public.ecr.aws/n4e0e1y0/beta9-worker:0.1.682`
- Helm chart: `infra/beta9`, chart/app version `0.1.714`
- Runner/base image registry: `public.ecr.aws/n4e0e1y0`

Useful release checks:

```bash
curl -fsSL -o /tmp/beta9-gateway-0.1.714.tar.gz \
  https://github.com/beam-cloud/beta9/archive/refs/tags/gateway-0.1.714.tar.gz
curl -fsSL -o /tmp/beta9-worker-0.1.682.tar.gz \
  https://github.com/beam-cloud/beta9/archive/refs/tags/worker-0.1.682.tar.gz
```

Install/update the chart from the master node path:

```bash
helm dependency build infra/beta9
helm upgrade --install beta9 infra/beta9 \
  --namespace beta9 \
  --create-namespace \
  --wait \
  --timeout 15m
```

The local deployment values expose the gateway on the master Tailscale IP using NodePorts:

- gRPC: `100.73.254.107:31993`
- HTTP: `http://100.73.254.107:31994`

Worker image storage is node-local hostPath (`/var/lib/beta9/images`) rather than a local-path PVC. A local-path PVC is pinned to the node where it was provisioned and blocks GPU worker pods from scheduling on a separate GPU node.

## node setup

This setup used one k3s master and one A6000 GPU worker:

- master: `beam-master-1`, Tailscale IP `100.73.254.107`
- worker: `worker-a6000-1`, Tailscale IP `100.92.182.92`
- GPU shape: `A6000`, 8 GPUs on the node, 1 GPU per A6000 worker pod

Provision the master:

```bash
./provision.exe \
  --host 100.73.254.107 \
  --role master \
  --mode install \
  --key ~/.ssh/hyperstack_staging.pem \
  --node-name beam-master-1 \
  --node-ip 100.73.254.107 \
  --k3s-tls-san 100.73.254.107 \
  --flannel-iface tailscale0 \
  --flannel-backend wireguard-native
```

Get the k3s join token from the master:

```bash
K3S_TOKEN="$(ssh -i ~/.ssh/hyperstack_staging.pem ubuntu@100.73.254.107 \
  'sudo cat /var/lib/rancher/k3s/server/node-token')"
```

Provision the A6000 worker:

```bash
./provision.exe \
  --host 100.92.182.92 \
  --role gpu \
  --mode install \
  --key ~/.ssh/master.pem \
  --node-name worker-a6000-1 \
  --node-ip 100.92.182.92 \
  --k3s-url https://100.73.254.107:6443 \
  --k3s-token "$K3S_TOKEN" \
  --flannel-iface tailscale0 \
  --gpu-type A6000 \
  --gpu-count 8
```

Important node details captured in the scripts:

- k3s node IPs are pinned to Tailscale IPs with `--node-ip`.
- flannel uses `tailscale0` and `wireguard-native`; plain VXLAN and host-gw were not sufficient in this mixed-network setup.
- reused nodes may contain stale RKE2/Cilium state. The GPU node provisioning path moves stale Cilium CNI configs aside, restores the k3s flannel CNI config, and ensures k3s CNI plugins are present under `/opt/cni/bin`.
- GPU nodes are labeled during join: `beta9.node-role=gpu`, `beta9.gpu=true`, `beta9.gpu-type=<type>`, `beta9.gpu-count=<count>`.

Validation commands:

```bash
kubectl get nodes -o wide
kubectl get node worker-a6000-1 \
  -o jsonpath='alloc_gpu={.status.allocatable.nvidia\.com/gpu} cap_gpu={.status.capacity.nvidia\.com/gpu}{"\n"}'
kubectl -n beta9 get pods -o wide
kubectl -n kube-system get pods -o wide | grep -E 'nvidia|coredns'
```

Expected final state for this setup:

- both nodes Ready
- `worker-a6000-1` advertises `nvidia.com/gpu=8`
- NVIDIA device plugin pod Running on `worker-a6000-1`
- Beta9 gateway Running on `beam-master-1`
- A6000 worker Running on `worker-a6000-1`

## thunder setup

