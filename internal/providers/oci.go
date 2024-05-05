package providers

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/core"
)

type OCIProvider struct {
	name           string
	computeClient  core.ComputeClient
	networkClient  core.VirtualNetworkClient
	clusterName    string
	appConfig      types.AppConfig
	providerRepo   repository.ProviderRepository
	providerConfig types.OCIProviderConfig
	tailscale      *network.Tailscale
	workerRepo     repository.WorkerRepository
}

const (
	ociReconcileInterval  time.Duration = 5 * time.Second
	ociBootVolumeSizeInGB int64         = 1000
)

func NewOCIProvider(appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*OCIProvider, error) {
	configProvider := common.NewRawConfigurationProvider(appConfig.Providers.OCIConfig.Tenancy,
		appConfig.Providers.OCIConfig.UserId, appConfig.Providers.OCIConfig.Region, appConfig.Providers.OCIConfig.FingerPrint,
		appConfig.Providers.OCIConfig.PrivateKey, common.String(appConfig.Providers.OCIConfig.PrivateKeyPassword),
	)

	computeClient, err := core.NewComputeClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}

	networkClient, err := core.NewVirtualNetworkClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}

	return &OCIProvider{
		name:           "oci",
		computeClient:  computeClient,
		networkClient:  networkClient,
		clusterName:    appConfig.ClusterName,
		appConfig:      appConfig,
		providerRepo:   providerRepo,
		providerConfig: appConfig.Providers.OCIConfig,
		tailscale:      tailscale,
		workerRepo:     workerRepo,
	}, nil
}

func (p *OCIProvider) getAvailableInstances() ([]Instance, error) {
	return []Instance{{
		Type: "VM.GPU.A10.1",
		Spec: InstanceSpec{Cpu: 15 * 1000, Memory: 240 * 1024, Gpu: "T4", GpuCount: 1},
	}}, nil
}

func (p *OCIProvider) Name() string {
	return p.name
}

func (p *OCIProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	availableInstances, err := p.getAvailableInstances()
	if err != nil {
		return "", err
	}

	instance, err := selectInstance(availableInstances, compute.Cpu, compute.Memory, compute.Gpu, compute.GpuCount)
	if err != nil {
		return "", err
	}

	gatewayHost, err := p.tailscale.GetHostnameForService("gateway-http")
	if err != nil {
		return "", err
	}

	machineId := machineId()
	populatedUserData, err := populateUserData(userDataConfig{
		AuthKey:           p.appConfig.Tailscale.AuthKey,
		ControlURL:        p.appConfig.Tailscale.ControlURL,
		GatewayHost:       gatewayHost,
		Beta9Token:        token,
		K3sVersion:        k3sVersion,
		DisableComponents: []string{"traefik"},
		MachineId:         machineId,
		PoolName:          poolName,
	}, ociUserDataTemplate)
	if err != nil {
		return "", err
	}

	sshPublicKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCkOEyPxGVNmoqW10QxR4uc3le0CGMQhXfLXbMLBFrdaEwaKarYdHTRZjHmI21LtkiXqY2KEH6UJBpx2uUozMU+2ur+gnIW8Itsi6NAQkIkawAW4MvTxBi2++6PbvQxaL7QHZqigFFRV9n/lt0l2QfMcClE14azS+Sn+WwzzJKGCslDUa6OEnD3evJqCGncvwNxkIVfGyeJz2Wh1fZK18jIlFj4efNBVeqx0RyE+SBd1h31QsLpMm5vBvrMXjqA/FGfNgQe25TQ8gA7Rgu9NZkcIExlIt/NQl2jhA7oWmjti5+JbYSJfm7J+309MVf9b0YjouupICDhs2ZFG2XzdDUf ssh-key-2024-03-20"

	log.Printf("<provider %s>: Selected shape <%s> for compute request: %+v\n", p.Name(), instance.Type, compute)
	encodedUserData := base64.StdEncoding.EncodeToString([]byte(populatedUserData))
	displayName := fmt.Sprintf("%s-%s-%s", p.clusterName, poolName, machineId)
	launchDetails := core.LaunchInstanceDetails{
		DisplayName:        common.String(displayName),
		AvailabilityDomain: common.String(p.providerConfig.AvailabilityDomain),
		Shape:              common.String(instance.Type),
		ShapeConfig: &core.LaunchInstanceShapeConfigDetails{
			Ocpus: common.Float32(float32(instance.Spec.Cpu / 1000.0)),
		},
		CompartmentId: common.String(p.providerConfig.CompartmentId),
		CreateVnicDetails: &core.CreateVnicDetails{
			AssignPublicIp: common.Bool(true),
			SubnetId:       common.String(p.providerConfig.SubnetId),
		},

		Metadata: map[string]string{
			"user_data":           encodedUserData,
			"ssh_authorized_keys": sshPublicKey,
		},
		SourceDetails: core.InstanceSourceViaImageDetails{
			BootVolumeSizeInGBs: common.Int64(ociBootVolumeSizeInGB),
			ImageId:             common.String(p.providerConfig.ImageId),
		},
	}

	response, err := p.computeClient.LaunchInstance(ctx, core.LaunchInstanceRequest{
		LaunchInstanceDetails: launchDetails,
	})
	if err != nil {
		return "", err
	}

	instanceId := *response.Instance.Id
	log.Printf("<provider %s>: Provisioned machine ID: %s\n", p.Name(), instanceId)

	err = p.providerRepo.AddMachine(string(types.ProviderOCI), poolName, machineId, &types.ProviderMachineState{
		Cpu:      instance.Spec.Cpu,
		Memory:   instance.Spec.Memory,
		Gpu:      instance.Spec.Gpu,
		GpuCount: instance.Spec.GpuCount,
	})
	if err != nil {
		return "", err
	}

	return machineId, nil
}

func (p *OCIProvider) TerminateMachine(ctx context.Context, poolName, instanceId string) error {
	if instanceId == "" {
		return fmt.Errorf("invalid instance ID")
	}

	request := core.TerminateInstanceRequest{
		InstanceId: common.String(instanceId),
	}

	_, err := p.computeClient.TerminateInstance(ctx, request)
	return err
}

func (p *OCIProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	tagFilters := map[string]string{
		"tag.Beta9ClusterName": p.clusterName,
		"tag.Beta9PoolName":    poolName,
	}

	// Initialize an empty map to hold the machine IDs and their corresponding OCI instance IDs
	machines := make(map[string]string)

	request := core.ListInstancesRequest{
		CompartmentId:  common.String(p.providerConfig.CompartmentId),
		LifecycleState: core.InstanceLifecycleStateRunning,
	}

	// Iterate through all instances in the compartment
	var page *string
	for {
		request.Page = page
		response, err := p.computeClient.ListInstances(ctx, request)
		if err != nil {
			return nil, err
		}

		for _, instance := range response.Items {
			// Check if the instance matches the tag filters
			match := true
			for key, value := range tagFilters {
				if instance.FreeformTags[key] != value {
					match = false
					break
				}
			}

			if match {
				machineId, exists := instance.FreeformTags["Beta9MachineId"]
				if exists {
					machines[machineId] = *instance.Id
				}
			}
		}

		if response.OpcNextPage == nil {
			break
		}

		page = response.OpcNextPage
	}

	return machines, nil
}

func (p *OCIProvider) Reconcile(ctx context.Context, poolName string) {
	ticker := time.NewTicker(ociReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			machines, err := p.listMachines(ctx, poolName)
			if err != nil {
				log.Println("<provider %s>: Unable to list machines: ", p.Name(), err)
				continue
			}

			for machineId, instanceId := range machines {
				func() {
					err := p.providerRepo.SetMachineLock(string(types.ProviderEC2), poolName, machineId)
					if err != nil {
						return
					}
					defer p.providerRepo.RemoveMachineLock(string(types.ProviderEC2), poolName, machineId)

					_, err = p.providerRepo.GetMachine(string(types.ProviderEC2), poolName, machineId)
					if err != nil {
						p.removeMachine(ctx, poolName, machineId, instanceId)
						return
					}

					// // See if there is a worker associated with this machine
					// _, err = p.workerRepo.GetWorkerById(machine.WorkerId)
					// if err != nil {
					// 	_, ok := err.(*types.ErrWorkerNotFound)

					// 	if ok {
					// 		p.removeMachine(ctx, poolName, machineId, instanceId)
					// 		return
					// 	}

					// 	return
					// }
				}()
			}
		}
	}
}

func (p *OCIProvider) removeMachine(ctx context.Context, poolName, machineId, instanceId string) {
	err := p.TerminateMachine(ctx, poolName, instanceId)
	if err != nil {
		log.Printf("Unable to terminate machine <machineId: %s>: %+v\n", machineId, err)
		return
	}

	log.Printf("Terminated machine <machineId: %s> due to inactivity\n", machineId)
}

const ociUserDataTemplate string = `#!/bin/bash

INSTALL_K3S_VERSION="{{.K3sVersion}}"
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

curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
  && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

apt-get update &&  apt-get install -y nvidia-container-toolkit nvidia-container-runtime jq nvidia-driver-470-server

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

# Register the node
HTTP_STATUS=$(curl -s -o response.json -w "%{http_code}" -X POST \
              -H "Content-Type: application/json" \
              -H "Authorization: Bearer $BETA9_TOKEN" \
              --data "$(jq -n \
                        --arg token "$TOKEN" \
                        --arg machineId "$MACHINE_ID" \
                        --arg providerName "oci" \
                        --arg poolName "$POOL_NAME" \
                        '{token: $token, machine_id: $machineId, provider_name: $providerName, pool_name: $poolName}')" \
              "http://$GATEWAY_HOST/api/v1/machine/register")

if [ $HTTP_STATUS -eq 200 ]; then
    CONFIG_JSON=$(jq '.config' response.json)
    kubectl create secret -n beta9 generic beta9-config --from-literal=config.json="$CONFIG_JSON"
else
    echo "Failed to register machine, status: $HTTP_STATUS"
    exit 1
fi
`
