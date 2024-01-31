package providers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/beam-cloud/beta9/internal/types"
)

type EC2Provider struct {
	client *ec2.Client
	config types.EC2ProviderConfig
}

const (
	instanceNamePrefix           string  = "beta9"
	instanceComputeBufferPercent float64 = 10.0
)

func NewEC2Provider(appConfig types.AppConfig) (*EC2Provider, error) {
	credentials := credentials.NewStaticCredentialsProvider(appConfig.Providers.EC2Config.AWSAccessKeyID, appConfig.Providers.EC2Config.AWSSecretAccessKey, "")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(appConfig.Providers.EC2Config.AWSRegion),
		config.WithCredentialsProvider(credentials),
	)
	if err != nil {
		log.Printf("Error loading AWS configuration: %v", err)
		return nil, err
	}

	return &EC2Provider{
		client: ec2.NewFromConfig(cfg),
		config: appConfig.Providers.EC2Config,
	}, nil
}

type InstanceSpec struct {
	Cpu    int64
	Memory int64
	Gpu    string
}

func (p *EC2Provider) selectInstanceType(requiredCpu int64, requiredMemory int64, requiredGpu string) (string, error) {
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

func (p *EC2Provider) ProvisionMachine(ctx context.Context, compute ComputeRequest, poolName string, machineId string) error {
	// NOTE: CPU cores -> millicores, memory -> megabytes
	instanceType, err := p.selectInstanceType(compute.Cpu, compute.Cpu, compute.Gpu)
	if err != nil {
		return err
	}

	log.Printf("Selected instance type <%s> for compute request: %+v\n", instanceType, compute)

	encodedUserData := base64.StdEncoding.EncodeToString([]byte(userData))
	input := &ec2.RunInstancesInput{
		ImageId:      aws.String(p.config.AMI),
		InstanceType: awsTypes.InstanceType(instanceType),
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		UserData:     aws.String(encodedUserData),
		SubnetId:     p.config.SubnetId,
	}

	result, err := p.client.RunInstances(ctx, input)
	if err != nil {
		return err
	}

	if len(result.Instances) == 0 {
		return errors.New("instance not created")
	}

	instanceID := *result.Instances[0].InstanceId
	instanceName := fmt.Sprintf("%s-%s-%s", instanceNamePrefix, poolName, machineId)

	_, err = p.client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{instanceID},
		Tags: []awsTypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(instanceName),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to tag the instance: %w", err)
	}

	return nil
}

func (p *EC2Provider) TerminateMachine(ctx context.Context, id string) error {
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

func (p *EC2Provider) ListMachines() error {
	return nil
}

var userData string = `
#!/bin/bash

INSTALL_K3S_VERSION="v1.28.5+k3s1"

# Install K3s
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=$INSTALL_K3S_VERSION INSTALL_K3S_EXEC=" --disable traefik" sh -    

# Wait for K3s to start, create the kubeconfig file, and the node token
while [ ! -f /etc/rancher/k3s/k3s.yaml ] || [ ! -f /var/lib/rancher/k3s/server/node-token ]
do
  sleep 1
done

KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl apply -f https://raw.githubusercontent.com/beam-cloud/beta9/ll/remote-machines/manifests/k3s/agent.yaml

`
