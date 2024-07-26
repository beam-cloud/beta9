package providers

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type GenericProvider struct {
	*ExternalProvider
}

func NewGenericProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*GenericProvider, error) {
	genericProvider := &GenericProvider{}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderGeneric),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     genericProvider.listMachines,
		TerminateMachineFunc: genericProvider.TerminateMachine,
	})
	genericProvider.ExternalProvider = baseProvider

	return genericProvider, nil
}

func (p *GenericProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	return "", types.NewProviderNotImplemented()
}

func (p *GenericProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	return nil, types.NewProviderNotImplemented()
}

func (p *GenericProvider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
	return types.NewProviderNotImplemented()
}
