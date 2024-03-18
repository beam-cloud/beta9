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
	"github.com/oracle/oci-go-sdk/common/auth"
	"github.com/oracle/oci-go-sdk/core"
)

type OCIProvider struct {
	computeClient core.ComputeClient
	networkClient core.VirtualNetworkClient
	clusterName   string
	appConfig     types.AppConfig
	providerRepo  repository.ProviderRepository
	tailscale     *network.Tailscale
	workerRepo    repository.WorkerRepository
}

const (
	ociReconcileInterval time.Duration = 5 * time.Second
)

func NewOCIProvider(appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*OCIProvider, error) {
	provider, err := auth.InstancePrincipalConfigurationProviderForRegion(common.RegionIAD)
	if err != nil {
		log.Printf("Unable to create OCI config provider: %v", err)
		return nil, err
	}

	computeClient, err := core.NewComputeClientWithConfigurationProvider(provider)
	if err != nil {
		log.Printf("Unable to create OCI Compute client: %v", err)
		return nil, err
	}

	networkClient, err := core.NewVirtualNetworkClientWithConfigurationProvider(provider)
	if err != nil {
		log.Printf("Unable to create OCI Virtual Network client: %v", err)
		return nil, err
	}

	return &OCIProvider{
		computeClient: computeClient,
		networkClient: networkClient,
		clusterName:   appConfig.ClusterName,
		appConfig:     appConfig,
		providerRepo:  providerRepo,
		tailscale:     tailscale,
		workerRepo:    workerRepo,
	}, nil
}

// OCI does not have a direct method to select instances like AWS. You need to define your logic based on available shapes in OCI.
func (p *OCIProvider) selectInstance(requiredCpu int64, requiredMemory int64, requiredGpuType string, requiredGpuCount uint32) (*Instance, error) {
	// This function needs to be implemented based on the specific requirements and available shapes in OCI.
	// For now, return a placeholder instance. You should replace this logic with actual instance selection logic.
	return &Instance{
		Type: "VM.Standard2.1",
		Spec: InstanceSpec{Cpu: 1, Memory: 15 * 1024, Gpu: "A10G", GpuCount: 1},
	}, nil
}

func (p *OCIProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	instance, err := p.selectInstance(compute.Cpu, compute.Memory, compute.Gpu, compute.GpuCount)
	if err != nil {
		return "", err
	}

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

	log.Printf("Selected shape <%s> for compute request: %+v\n", instance.Type, compute)
	encodedUserData := base64.StdEncoding.EncodeToString([]byte(populatedUserData))

	// Example launch details, replace with actual details like imageId, compartmentId, subnetId, etc.
	launchDetails := core.LaunchInstanceDetails{
		DisplayName:   common.String("test-instance"),
		Shape:         common.String(instance.Type),
		CompartmentId: common.String("beam-dev"), // Replace with actual compartment OCID
		// Add additional required fields like ImageId, SubnetId, etc.
		Metadata: map[string]string{
			"user_data": encodedUserData,
		},
		SourceDetails: core.InstanceSourceViaImageDetails{},
	}

	request := core.LaunchInstanceRequest{
		LaunchInstanceDetails: launchDetails,
	}

	response, err := p.computeClient.LaunchInstance(ctx, request)
	if err != nil {
		return "", err
	}

	instanceId := *response.Instance.Id
	log.Printf("Provisioned machine ID: %s\n", instanceId)

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
		CompartmentId:  common.String("<compartment_ocid>"),
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

func (p *OCIProvider) removeMachine(ctx context.Context, poolName, machineId, instanceId string) {
	err := p.TerminateMachine(ctx, poolName, instanceId)
	if err != nil {
		log.Printf("Unable to terminate machine <machineId: %s>: %+v\n", machineId, err)
		return
	}

	log.Printf("Terminated machine <machineId: %s> due to inactivity\n", machineId)
}
