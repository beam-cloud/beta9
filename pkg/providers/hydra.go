package providers

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type HydraProvider struct {
	*ExternalProvider
}

func NewHydraProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*HydraProvider, error) {
	hydraProvider := &HydraProvider{}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderHydra),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     hydraProvider.listMachines,
		TerminateMachineFunc: hydraProvider.TerminateMachine,
	})
	hydraProvider.ExternalProvider = baseProvider

	return hydraProvider, nil
}

func (p *HydraProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	return "", types.NewProviderNotImplemented()
}

func (p *HydraProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	return nil, types.NewProviderNotImplemented()
}

func (p *HydraProvider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
	return types.NewProviderNotImplemented()
}
