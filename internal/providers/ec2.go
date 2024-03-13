package providers

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

type EC2Provider struct {
	client         *ec2.Client
	clusterName    string
	appConfig      types.AppConfig
	providerConfig types.EC2ProviderConfig
	providerRepo   repository.ProviderRepository
	tailscale      *network.Tailscale
	workerRepo     repository.WorkerRepository
}

const (
	instanceComputeBufferPercent float64       = 10.0
	k3sVersion                   string        = "v1.28.5+k3s1"
	ec2ReconcileInterval         time.Duration = 5 * time.Second
)

func NewEC2Provider(appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*EC2Provider, error) {
	credentials := credentials.NewStaticCredentialsProvider(appConfig.Providers.EC2Config.AWSAccessKey, appConfig.Providers.EC2Config.AWSSecretKey, "")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(appConfig.Providers.EC2Config.AWSRegion),
		config.WithCredentialsProvider(credentials),
	)
	if err != nil {
		log.Printf("Unable to load AWS config: %v", err)
		return nil, err
	}

	return &EC2Provider{
		client:         ec2.NewFromConfig(cfg),
		clusterName:    appConfig.ClusterName,
		appConfig:      appConfig,
		providerConfig: appConfig.Providers.EC2Config,
		providerRepo:   providerRepo,
		tailscale:      tailscale,
		workerRepo:     workerRepo,
	}, nil
}

type InstanceSpec struct {
	Cpu      int64
	Memory   int64
	Gpu      string
	GpuCount uint32
}

type Instance struct {
	Type string
	Spec InstanceSpec
}

func (p *EC2Provider) selectInstance(requiredCpu int64, requiredMemory int64, requiredGpuType string, requiredGpuCount uint32) (*Instance, error) {
	// TODO: make instance selection more dynamic / don't rely on hardcoded values
	// We can load desired instances from the worker pool config, and then use the DescribeInstances
	// api to return valid instance types
	availableInstances := []Instance{
		{"g4dn.xlarge", InstanceSpec{4 * 1000, 16 * 1024, "T4", 1}},
		{"g4dn.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, "T4", 1}},
		{"g4dn.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, "T4", 1}},
		{"g4dn.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, "T4", 1}},
		{"g4dn.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, "T4", 1}},
		{"g4dn.12xlarge", InstanceSpec{48 * 1000, 192 * 1024, "T4", 4}},
		{"g4dn.metal", InstanceSpec{96 * 1000, 384 * 1024, "T4", 8}},

		{"g5.xlarge", InstanceSpec{4 * 1000, 16 * 1024, "A10G", 1}},
		{"g5.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, "A10G", 1}},
		{"g5.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, "A10G", 1}},
		{"g5.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, "A10G", 1}},
		{"g5.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, "A10G", 1}},

		{"m6i.large", InstanceSpec{2 * 1000, 8 * 1024, "", 0}},
		{"m6i.xlarge", InstanceSpec{4 * 1000, 16 * 1024, "", 0}},
		{"m6i.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, "", 0}},
		{"m6i.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, "", 0}},
		{"m6i.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, "", 0}},
		{"m6i.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, "", 0}},
	}

	// Apply compute buffer
	bufferedCpu := int64(float64(requiredCpu) * (1 + instanceComputeBufferPercent/100))
	bufferedMemory := int64(float64(requiredMemory) * (1 + instanceComputeBufferPercent/100))

	meetsRequirements := func(spec InstanceSpec) bool {
		return spec.Cpu >= bufferedCpu && spec.Memory >= bufferedMemory && spec.Gpu == requiredGpuType && spec.GpuCount >= requiredGpuCount
	}

	// Find the smallest instance that meets or exceeds the requirements
	var selectedInstance *Instance = nil
	for _, instance := range availableInstances {
		if meetsRequirements(instance.Spec) {
			selectedInstance = &instance
			break
		}
	}

	if selectedInstance == nil {
		return nil, fmt.Errorf("no suitable instance type found for CPU=%d, Memory=%d, GPU=%s", requiredCpu, requiredMemory, requiredGpuType)
	}

	return selectedInstance, nil
}

func (p *EC2Provider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	instance, err := p.selectInstance(compute.Cpu, compute.Memory, compute.Gpu, compute.GpuCount) // NOTE: CPU cores -> millicores, memory -> megabytes
	if err != nil {
		return "", err
	}

	// TODO: come up with a way to not hardcode the service name (possibly look up in config)
	gatewayHost, err := p.tailscale.GetHostnameForService("gateway-http")
	if err != nil {
		return "", err
	}

	machineId := MachineId()
	populatedUserData, err := populateUserData(userDataConfig{
		AuthKey:           p.appConfig.Tailscale.AuthKey,
		ControlURL:        p.appConfig.Tailscale.ControlURL,
		GatewayHost:       gatewayHost,
		Beta9Token:        token,
		K3sVersion:        k3sVersion,
		DisableComponents: []string{"traefik"},
		MachineId:         machineId,
		PoolName:          poolName,
	})
	if err != nil {
		return "", err
	}

	log.Printf("Selected instance type <%s> for compute request: %+v\n", instance.Type, compute)
	encodedUserData := base64.StdEncoding.EncodeToString([]byte(populatedUserData))
	input := &ec2.RunInstancesInput{
		ImageId:      aws.String(p.providerConfig.AMI),
		InstanceType: awsTypes.InstanceType(instance.Type),
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		UserData:     aws.String(encodedUserData),
		SubnetId:     p.providerConfig.SubnetId,
	}

	result, err := p.client.RunInstances(ctx, input)
	if err != nil {
		return "", err
	}

	if len(result.Instances) == 0 {
		return "", errors.New("instance not created")
	}

	instanceId := *result.Instances[0].InstanceId
	instanceName := fmt.Sprintf("%s-%s-%s", p.clusterName, poolName, machineId)

	_, err = p.client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{instanceId},
		Tags: []awsTypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(instanceName),
			},
			{
				Key:   aws.String("Beta9ClusterName"),
				Value: aws.String(p.clusterName),
			},
			{
				Key:   aws.String("Beta9PoolName"),
				Value: aws.String(poolName),
			},
			{
				Key:   aws.String("Beta9MachineId"),
				Value: aws.String(machineId),
			},
		},
	})

	if err != nil {
		return "", fmt.Errorf("failed to tag the instance: %w", err)
	}

	err = p.providerRepo.AddMachine(string(types.ProviderEC2), poolName, machineId, &types.ProviderMachineState{
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

func (p *EC2Provider) TerminateMachine(ctx context.Context, poolName, instanceId string) error {
	if instanceId == "" {
		return errors.New("invalid instance ID")
	}

	input := &ec2.TerminateInstancesInput{
		InstanceIds: []string{instanceId},
	}

	_, err := p.client.TerminateInstances(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

func (p *EC2Provider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	input := &ec2.DescribeInstancesInput{
		Filters: []awsTypes.Filter{
			{
				Name:   aws.String("tag:Beta9ClusterName"),
				Values: []string{p.clusterName},
			},
			{
				Name:   aws.String("tag:Beta9PoolName"),
				Values: []string{poolName},
			},
			{
				Name:   aws.String("instance-state-name"),
				Values: []string{"running"},
			},
		},
	}

	machines := make(map[string]string) // Map instance ID to Beta9MachineId
	paginator := ec2.NewDescribeInstancesPaginator(p.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				var machineId string
				for _, tag := range instance.Tags {
					if *tag.Key == "Beta9MachineId" {
						machineId = *tag.Value
						break
					}
				}

				machines[machineId] = *instance.InstanceId
			}
		}
	}

	return machines, nil
}

func (p *EC2Provider) Reconcile(ctx context.Context, poolName string) {
	ticker := time.NewTicker(ec2ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			machines, err := p.listMachines(ctx, poolName)
			if err != nil {
				log.Println("Error listing machines: ", err)
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

func (p *EC2Provider) removeMachine(ctx context.Context, poolName, machineId, instanceId string) {
	err := p.TerminateMachine(ctx, poolName, instanceId)
	if err != nil {
		log.Printf("Unable to terminate machine <machineId: %s>: %+v\n", machineId, err)
		return
	}

	log.Printf("Terminated machine <machineId: %s> due to inactivity\n", machineId)
}

type userDataConfig struct {
	AuthKey           string
	ControlURL        string
	GatewayHost       string
	Beta9Token        string
	K3sVersion        string
	DisableComponents []string
	MachineId         string
	PoolName          string
}

func populateUserData(config userDataConfig) (string, error) {
	t, err := template.New("userdata").Parse(userDataTemplate)
	if err != nil {
		return "", fmt.Errorf("error parsing user data template: %w", err)
	}

	var populatedTemplate bytes.Buffer
	if err := t.Execute(&populatedTemplate, config); err != nil {
		return "", fmt.Errorf("error executing user data template: %w", err)
	}

	return populatedTemplate.String(), nil
}

const userDataTemplate string = `
#!/bin/bash

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

distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
   && curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.repo | sudo tee /etc/yum.repos.d/nvidia-docker.repo

# Configure nvidia container runtime
yum-config-manager --disable amzn2-nvidia-470-branch amzn2-core
yum remove -y libnvidia-container
yum install -y nvidia-container-toolkit nvidia-container-runtime
yum-config-manager --enable amzn2-nvidia-470-branch amzn2-core

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
                        --arg providerName "ec2" \
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
