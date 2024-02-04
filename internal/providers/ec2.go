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
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

type EC2Provider struct {
	client         *ec2.Client
	clusterName    string
	appConfig      types.AppConfig
	providerConfig types.EC2ProviderConfig
	tailscaleRepo  repository.TailscaleRepository
}

const (
	instanceComputeBufferPercent float64 = 10.0
	k3sVersion                   string  = "v1.28.5+k3s1"
)

func NewEC2Provider(appConfig types.AppConfig, tailscaleRepo repository.TailscaleRepository) (*EC2Provider, error) {
	credentials := credentials.NewStaticCredentialsProvider(appConfig.Providers.EC2Config.AWSAccessKeyID, appConfig.Providers.EC2Config.AWSSecretAccessKey, "")

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
		tailscaleRepo:  tailscaleRepo,
	}, nil
}

type InstanceSpec struct {
	Cpu    int64
	Memory int64
	Gpu    string
}

func (p *EC2Provider) selectInstanceType(requiredCpu int64, requiredMemory int64, requiredGpu string) (string, error) {
	// TODO: make instance selection more dynamic / don't rely on hardcoded values
	// We can load desired instances from the worker pool config, and then use the DescribeInstances
	// api to return valid instance types
	availableInstances := []struct {
		Type string
		Spec InstanceSpec
	}{
		{"g4dn.xlarge", InstanceSpec{4 * 1000, 16 * 1024, "T4"}},
		{"g4dn.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, "T4"}},
		{"g4dn.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, "T4"}},
		{"g4dn.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, "T4"}},
		{"g4dn.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, "T4"}},

		{"g5.xlarge", InstanceSpec{4 * 1000, 16 * 1024, "A10G"}},
		{"g5.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, "A10G"}},
		{"g5.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, "A10G"}},
		{"g5.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, "A10G"}},
		{"g5.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, "A10G"}},

		{"m6i.large", InstanceSpec{2 * 1000, 8 * 1024, ""}},
		{"m6i.xlarge", InstanceSpec{4 * 1000, 16 * 1024, ""}},
		{"m6i.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, ""}},
		{"m6i.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, ""}},
		{"m6i.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, ""}},
		{"m6i.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, ""}},
	}

	// Apply compute buffer
	bufferedCpu := int64(float64(requiredCpu) * (1 + instanceComputeBufferPercent/100))
	bufferedMemory := int64(float64(requiredMemory) * (1 + instanceComputeBufferPercent/100))

	meetsRequirements := func(spec InstanceSpec) bool {
		return spec.Cpu >= bufferedCpu && spec.Memory >= bufferedMemory && spec.Gpu == requiredGpu
	}

	// Find the smallest instance that meets or exceeds the requirements
	var selectedInstance string
	for _, instance := range availableInstances {
		if meetsRequirements(instance.Spec) {
			selectedInstance = instance.Type
			break
		}
	}

	if selectedInstance == "" {
		return "", fmt.Errorf("no suitable instance type found for CPU=%d, Memory=%d, GPU=%s", requiredCpu, requiredMemory, requiredGpu)
	}

	return selectedInstance, nil
}

func (p *EC2Provider) ProvisionMachine(ctx context.Context, poolName string, compute types.ProviderComputeRequest) (string, error) {
	// NOTE: CPU cores -> millicores, memory -> megabytes
	instanceType, err := p.selectInstanceType(compute.Cpu, compute.Cpu, compute.Gpu)
	if err != nil {
		return "", err
	}

	gatewayHost, err := p.tailscaleRepo.GetHostnameForService("gateway-http")
	if err != nil {
		return "", err
	}

	// TODO: remove once we sort out connection issues
	roleArn := "arn:aws:iam::187248174200:instance-profile/beta-dev-k3s-instance-profile"

	machineId := MachineId()
	populatedUserData, err := populateUserData(userDataConfig{
		AuthKey:     p.appConfig.Tailscale.AuthKey,
		ControlURL:  p.appConfig.Tailscale.ControlURL,
		GatewayHost: gatewayHost,

		// TODO: replace with single-use token
		Beta9Token:        "AYnhx9tTvla5KdLEPWApabnsG5nPUX8KeNzLK2z2CGtxsTrzid8c5l0lE6P-cx-o4-2kx8scBkpT0gt-p1EufA==",
		K3sVersion:        k3sVersion,
		DisableComponents: []string{"traefik"},
		MachineId:         machineId,
		PoolName:          poolName,
	})
	if err != nil {
		return "", err
	}

	log.Printf("Selected instance type <%s> for compute request: %+v\n", instanceType, compute)
	encodedUserData := base64.StdEncoding.EncodeToString([]byte(populatedUserData))
	input := &ec2.RunInstancesInput{
		ImageId:            aws.String(p.providerConfig.AMI),
		InstanceType:       awsTypes.InstanceType(instanceType),
		MinCount:           aws.Int32(1),
		MaxCount:           aws.Int32(1),
		UserData:           aws.String(encodedUserData),
		SubnetId:           p.providerConfig.SubnetId,
		IamInstanceProfile: &awsTypes.IamInstanceProfileSpecification{Arn: &roleArn},
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

	return machineId, nil
}

func (p *EC2Provider) TerminateMachine(ctx context.Context, poolName, id string) error {
	if id == "" {
		return errors.New("invalid instance ID")
	}

	input := &ec2.TerminateInstancesInput{
		InstanceIds: []string{id},
	}

	_, err := p.client.TerminateInstances(ctx, input)
	if err != nil {
		log.Printf("Error terminating EC2 instance %s: %v", id, err)
		return err
	}

	return nil
}

func (p *EC2Provider) ListMachines(ctx context.Context, poolName string) ([]string, error) {
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

	var machines []string
	paginator := ec2.NewDescribeInstancesPaginator(p.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("Error fetching instances page: %v", err)
			return nil, err
		}

		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				machines = append(machines, *instance.InstanceId)
			}
		}
	}

	return machines, nil
}

func (p *EC2Provider) Reconcile(ctx context.Context, poolName string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			machines, err := p.ListMachines(ctx, poolName)
			if err != nil {
				log.Printf("Error listing machines: %v\n", err)
				continue
			}

			for _, machine := range machines {
				log.Printf("Machine: %+v\n", machine)
			}
		}
	}
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

# Install K3s
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=$INSTALL_K3S_VERSION INSTALL_K3S_EXEC="{{range .DisableComponents}} --disable {{.}}{{end}}" sh -    

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
kubectl annotate secret beta9-token kubernetes.io/service-account.name=beta9
kubectl patch serviceaccount beta9 -p '{"secrets":[{"name":"beta9-token"}]}'
kubectl create clusterrolebinding beta9-admin-binding --clusterrole=cluster-admin --serviceaccount=default:beta9
kubectl create namespace beta9

curl -fsSL https://tailscale.com/install.sh | sh
wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq &&\
    chmod +x /usr/bin/yq

tailscale up --authkey {{.AuthKey}} --login-server {{.ControlURL}} --accept-routes --hostname {{.MachineId}}

TOKEN=$(kubectl get secret beta9-token -o jsonpath='{.data.token}' | base64 --decode)

# Register the node
HTTP_STATUS=$(curl -s -o response.json -w "%{http_code}" -X POST \
              -H "Content-Type: application/json" \
              -H "Authorization: Bearer $BETA9_TOKEN" \
              -d '{"token":"'$TOKEN'", "machine_id":"{{.MachineId}}", "provider_name":"ec2", "pool_name": "{{.PoolName}}"}' \
              http://{{.GatewayHost}}/api/v1/machine/register)

if [ $HTTP_STATUS -eq 200 ]; then
    CONFIG_YAML=$(jq '.config' response.json | yq e -P -)
    kubectl create secret generic beta9-config --from-literal=config.yaml="$CONFIG_YAML"
else
    echo "Failed to register machine, status: $HTTP_STATUS"
    exit 1
fi
`
