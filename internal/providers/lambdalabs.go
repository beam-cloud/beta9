package providers

import (
	"context"

	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

type LambdaLabsProvider struct {
	*ExternalProvider
}

func NewLambdaLabsProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*LambdaLabsProvider, error) {
	lambdaLabsProvider := &LambdaLabsProvider{}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:         string(types.ProviderEC2),
		ClusterName:  appConfig.ClusterName,
		AppConfig:    appConfig,
		TailScale:    tailscale,
		ProviderRepo: providerRepo,
		WorkerRepo:   workerRepo,
		// ListMachinesFunc:     lambdaLabsProvider.listMachines,
		// TerminateMachineFunc: lambdaLabsProvider.TerminateMachine,
	})
	lambdaLabsProvider.ExternalProvider = baseProvider

	return lambdaLabsProvider, nil
}
