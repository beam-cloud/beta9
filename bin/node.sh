#!/bin/bash

INSTALL_K3S_VERSION="v1.28.5+k3s1"
PROVIDER_NAME=""
MACHINE_ID="{{.MachineId}}"
BETA9_TOKEN="{{.Beta9Token}}"
POOL_NAME="{{.PoolName}}"
TAILSCALE_CONTROL_URL="{{.ControlURL}}"
TAILSCALE_AUTH_KEY="{{.AuthKey}}"
GATEWAY_HOST="{{.GatewayHost}}"

K3S_DISABLE_COMPONENTS=""
{{range .DisableComponents}}
K3S_DISABLE_COMPONENTS="${K3S_DISABLE_COMPONENTS} --disable {{.}}"
{{end}}

# distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
#    && curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.repo | sudo tee /etc/yum.repos.d/nvidia-docker.repo

# # Configure nvidia container runtime
# yum-config-manager --disable amzn2-nvidia-470-branch amzn2-core
# yum remove -y libnvidia-container
# yum install -y nvidia-container-toolkit nvidia-container-runtime
# yum-config-manager --enable amzn2-nvidia-470-branch amzn2-core

# Install K3s
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=$INSTALL_K3S_VERSION INSTALL_K3S_EXEC="$K3S_DISABLE_COMPONENTS" sh -

# Wait for K3s to be up and running
while [ ! -f /etc/rancher/k3s/k3s.yaml ] || [ ! -f /var/lib/rancher/k3s/server/node-token ]; do
  sleep 1
done

# Create beta9 service account
kubectl create serviceaccount beta9
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: beta9-token
  annotations:
    kubernetes.io/service-account.name: beta9
type: kubernetes.io/service-account-token
EOF

cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: nvidia-device-plugin-daemonset
  namespace: kube-system
spec:
  selector:
    matchLabels:
      name: nvidia-device-plugin-ds
  updateStrategy:
    type: RollingUpdate
  template:
    metadata:
      labels:
        name: nvidia-device-plugin-ds
      annotations:
        scheduler.alpha.kubernetes.io/critical-pod: ""
    spec:
      tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
      priorityClassName: system-node-critical
      runtimeClassName: nvidia
      containers:
      - image: nvcr.io/nvidia/k8s-device-plugin:v0.14.3
        name: nvidia-device-plugin-ctr
        env:
        - name: FAIL_ON_INIT_ERROR
          value: "false"
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
        volumeMounts:
        - name: device-plugin
          mountPath: /var/lib/kubelet/device-plugins
      volumes:
      - name: device-plugin
        hostPath:
          path: /var/lib/kubelet/device-plugins
EOF

kubectl annotate secret beta9-token kubernetes.io/service-account.name=beta9
kubectl patch serviceaccount beta9 -p '{"secrets":[{"name":"beta9-token"}]}'
kubectl create clusterrolebinding beta9-admin-binding --clusterrole=cluster-admin --serviceaccount=default:beta9
kubectl create namespace beta9

curl -fsSL https://tailscale.com/install.sh | sh

tailscale up --authkey "$TAILSCALE_AUTH_KEY" --login-server "$TAILSCALE_CONTROL_URL" --accept-routes --hostname "$MACHINE_ID"

# Wait for Tailscale to establish a connection
until tailscale status --json | jq -e '.Peer[] | select(.TailscaleIPs != null) | any' >/dev/null 2>&1; do
  echo "Waiting for Tailscale to establish a connection..."
  sleep 1
done

TOKEN=$(kubectl get secret beta9-token -o jsonpath='{.data.token}' | base64 --decode)

# Determine how much cpu/memory is actually available
CPU_CORES=$(awk -v cores=$(grep -c ^processor /proc/cpuinfo) 'BEGIN{print cores * 1000}')
MEMORY=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)

# Register the node
HTTP_STATUS=$(curl -s -o response.json -w "%{http_code}" -X POST \
              -H "Content-Type: application/json" \
              -H "Authorization: Bearer $BETA9_TOKEN" \
              --data "$(jq -n \
                        --arg token "$TOKEN" \
                        --arg machineId "$MACHINE_ID" \
                        --arg cpu "$CPU_CORES" \
                        --arg memory "$MEMORY" \
                        --arg providerName "$PROVIDER_NAME" \
                        --arg poolName "$POOL_NAME" \
                        '{token: $token, machine_id: $machineId, cpu: $cpu, memory: $memory, provider_name: $providerName, pool_name: $poolName}')" \
              "http://$GATEWAY_HOST/api/v1/machine/register")

if [ $HTTP_STATUS -eq 200 ]; then
    CONFIG_JSON=$(jq '.config' response.json)
    kubectl create secret -n beta9 generic beta9-config --from-literal=config.json="$CONFIG_JSON"
else
    echo "Failed to register machine, status: $HTTP_STATUS"
    exit 1
fi  