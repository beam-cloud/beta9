package providers

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type CrusoeProvider struct {
	*ExternalProvider
}

func NewCrusoeProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*CrusoeProvider, error) {
	crusoeProvider := &CrusoeProvider{}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderCrusoe),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     crusoeProvider.listMachines,
		TerminateMachineFunc: crusoeProvider.TerminateMachine,
	})
	crusoeProvider.ExternalProvider = baseProvider

	return crusoeProvider, nil
}

func (p *CrusoeProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	return "", types.NewProviderNotImplemented()
}

func (p *CrusoeProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	return nil, types.NewProviderNotImplemented()
}

func (p *CrusoeProvider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
	return types.NewProviderNotImplemented()
}
