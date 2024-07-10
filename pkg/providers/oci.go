package providers

import (
	"context"
	"fmt"
	"log"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/core"
)

type OCIProvider struct {
	*ExternalProvider
	computeClient  core.ComputeClient
	networkClient  core.VirtualNetworkClient
	providerConfig types.OCIProviderConfig
}

const (
	ociBootVolumeSizeInGB int64 = 250 // TODO: make this configurable in OCIProviderConfig
)

func NewOCIProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*OCIProvider, error) {
	configProvider := common.NewRawConfigurationProvider(appConfig.Providers.OCI.Tenancy,
		appConfig.Providers.OCI.UserId, appConfig.Providers.OCI.Region, appConfig.Providers.OCI.FingerPrint,
		appConfig.Providers.OCI.PrivateKey, common.String(appConfig.Providers.OCI.PrivateKeyPassword),
	)

	computeClient, err := core.NewComputeClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}

	networkClient, err := core.NewVirtualNetworkClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}

	ociProvider := &OCIProvider{
		computeClient:  computeClient,
		networkClient:  networkClient,
		providerConfig: appConfig.Providers.OCI,
	}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderOCI),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     ociProvider.listMachines,
		TerminateMachineFunc: ociProvider.TerminateMachine,
	})

	ociProvider.ExternalProvider = baseProvider

	return ociProvider, nil
}

func (p *OCIProvider) getAvailableInstances() ([]Instance, error) {
	return []Instance{{
		Type: "VM.GPU.A10.1",
		Spec: InstanceSpec{Cpu: 15 * 1000, Memory: 240 * 1024, Gpu: "A10G", GpuCount: 1},
	}}, nil
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

	machineId := MachineId()
	cloudInitData, err := generateCloudInitData(userDataConfig{
		TailscaleAuth:     p.AppConfig.Tailscale.AuthKey,
		TailscaleUrl:      p.AppConfig.Tailscale.ControlURL,
		RegistrationToken: token,
		MachineId:         machineId,
		PoolName:          poolName,
		ProviderName:      p.Name,
	}, ociUserDataTemplate)
	if err != nil {
		return "", err
	}

	log.Printf("<provider %s>: Selected shape <%s> for compute request: %+v\n", p.Name, instance.Type, compute)

	displayName := fmt.Sprintf("%s-%s-%s", p.ClusterName, poolName, machineId)
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
			"user_data": cloudInitData,
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

	log.Printf("<provider %s>: Provisioned machine ID: %s\n", p.Name, instanceId)

	err = p.ProviderRepo.AddMachine(string(types.ProviderOCI), poolName, machineId, &types.ProviderMachineState{
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

func (p *OCIProvider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
	if instanceId == "" {
		return fmt.Errorf("invalid instance ID")
	}

	request := core.TerminateInstanceRequest{
		InstanceId: common.String(instanceId),
	}

	_, err := p.computeClient.TerminateInstance(ctx, request)
	if err != nil {
		return err
	}

	err = p.ProviderRepo.RemoveMachine(p.Name, poolName, machineId)
	if err != nil {
		log.Printf("<provider %s>: Unable to remove machine state <machineId: %s>: %+v\n", p.Name, machineId, err)
		return err
	}

	log.Printf("<provider %s>: Terminated machine <machineId: %s> due to inactivity\n", p.Name, machineId)
	return nil
}

func (p *OCIProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	tagFilters := map[string]string{
		"tag.Beta9ClusterName": p.ClusterName,
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

const ociUserDataTemplate string = `#!/bin/bash
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
  && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

apt-get update &&  apt-get install -y nvidia-container-toolkit nvidia-container-runtime jq nvidia-driver-470-server

curl -L -o agent https://release.beam.cloud/agent/agent && \
chmod +x agent && \\
./agent --token "{{.RegistrationToken}}" --machine-id "{{.MachineId}}" \
--tailscale-url "{{.TailscaleUrl}}" \
--tailscale-auth "{{.TailscaleAuth}}" \
--pool-name "{{.PoolName}}" \
--provider-name "{{.ProviderName}}"
`
