package providers

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type EC2Provider struct {
	*ExternalProvider
	client         *ec2.Client
	providerConfig types.EC2ProviderConfig
}

func NewEC2Provider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*EC2Provider, error) {
	cfg, err := common.GetAWSConfig(appConfig.Providers.EC2.AWSAccessKey, appConfig.Providers.EC2.AWSSecretKey, appConfig.Providers.EC2.AWSRegion, "")
	if err != nil {
		return nil, err
	}

	ec2Provider := &EC2Provider{
		client:         ec2.NewFromConfig(cfg),
		providerConfig: appConfig.Providers.EC2,
	}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderEC2),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     ec2Provider.listMachines,
		TerminateMachineFunc: ec2Provider.TerminateMachine,
	})
	ec2Provider.ExternalProvider = baseProvider

	return ec2Provider, nil
}

func (p *EC2Provider) getAvailableInstances() ([]Instance, error) {
	// TODO: make instance selection more dynamic / don't rely on hardcoded values
	// We can load desired instances from the worker pool config, and then use the DescribeInstances
	// api to return valid instance types
	return []Instance{
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

		{"g6.xlarge", InstanceSpec{4 * 1000, 16 * 1024, "G6", 1}},
		{"g6.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, "G6", 1}},
		{"g6.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, "G6", 1}},
		{"g6.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, "G6", 1}},
		{"g6.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, "G6", 1}},

		{"m7i.large", InstanceSpec{2 * 1000, 8 * 1024, "", 0}},
		{"m7i.xlarge", InstanceSpec{4 * 1000, 16 * 1024, "", 0}},
		{"m7i.2xlarge", InstanceSpec{8 * 1000, 32 * 1024, "", 0}},
		{"m7i.4xlarge", InstanceSpec{16 * 1000, 64 * 1024, "", 0}},
		{"m7i.8xlarge", InstanceSpec{32 * 1000, 128 * 1024, "", 0}},
		{"m7i.12xlarge", InstanceSpec{48 * 1000, 192 * 1024, "", 0}},
		{"m7i.16xlarge", InstanceSpec{64 * 1000, 256 * 1024, "", 0}},
	}, nil
}

func (p *EC2Provider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	availableInstances, err := p.getAvailableInstances()
	if err != nil {
		return "", err
	}

	instance, err := selectInstance(availableInstances, compute.Cpu, compute.Memory, compute.Gpu, compute.GpuCount) // NOTE: CPU cores -> millicores, memory -> megabytes
	if err != nil {
		return "", err
	}

	machineId := MachineId()
	cloudInitData, err := generateCloudInitData(userDataConfig{
		TailscaleAuth:     p.AppConfig.Tailscale.AuthKey,
		TailscaleUrl:      p.AppConfig.Tailscale.ControlURL,
		RegistrationToken: token,
		MachineId:         machineId,
		PoolName:          poolName,
		ProviderName:      p.Name,
	}, ec2UserDataTemplate)
	if err != nil {
		return "", err
	}

	log.Info().Str("provider", p.Name).Str("instance_type", instance.Type).Str("compute_request", fmt.Sprintf("%+v", compute)).Msg("selected instance type")
	input := &ec2.RunInstancesInput{
		ImageId:      aws.String(p.providerConfig.AMI),
		InstanceType: awsTypes.InstanceType(instance.Type),
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		UserData:     aws.String(cloudInitData),
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
	instanceName := fmt.Sprintf("%s-%s-%s", p.ClusterName, poolName, machineId)

	_, err = p.client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{instanceId},
		Tags: []awsTypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(instanceName),
			},
			{
				Key:   aws.String("Beta9ClusterName"),
				Value: aws.String(p.ClusterName),
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

	err = p.ProviderRepo.AddMachine(string(types.ProviderEC2), poolName, machineId, &types.ProviderMachineState{
		Cpu:               instance.Spec.Cpu,
		Memory:            instance.Spec.Memory,
		Gpu:               instance.Spec.Gpu,
		GpuCount:          instance.Spec.GpuCount,
		RegistrationToken: token,
		AutoConsolidate:   true,
	})

	if err != nil {
		return "", err
	}

	return machineId, nil
}

func (p *EC2Provider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
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

	err = p.ProviderRepo.RemoveMachine(p.Name, poolName, machineId)
	if err != nil {
		log.Error().Str("provider", p.Name).Str("machine_id", machineId).Err(err).Msg("unable to remove machine state")
		return err
	}

	log.Info().Str("provider", p.Name).Str("machine_id", machineId).Msg("terminated machine")
	return nil
}

func (p *EC2Provider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	input := &ec2.DescribeInstancesInput{
		Filters: []awsTypes.Filter{
			{
				Name:   aws.String("tag:Beta9ClusterName"),
				Values: []string{p.ClusterName},
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

const ec2UserDataTemplate string = `#!/bin/bash
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
   && curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.repo | tee /etc/yum.repos.d/nvidia-docker.repo

# Configure nvidia container runtime
yum-config-manager --disable amzn2-nvidia-470-branch amzn2-core
yum remove -y libnvidia-container
yum install -y nvidia-container-toolkit nvidia-container-runtime
yum-config-manager --enable amzn2-nvidia-470-branch amzn2-core

curl -L -o agent https://release.beam.cloud/agent/agent && \
chmod +x agent && \
./agent --token "{{.RegistrationToken}}" --machine-id "{{.MachineId}}" \
--tailscale-url "{{.TailscaleUrl}}" \
--tailscale-auth "{{.TailscaleAuth}}" \
--pool-name "{{.PoolName}}" \
--provider-name "{{.ProviderName}}"
`
