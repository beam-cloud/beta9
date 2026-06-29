package gatewayservices

import (
	"context"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func (gws *GatewayService) durableDiskPlacementRepos() abstractions.DurableDiskPlacementRepos {
	if gws == nil {
		return abstractions.DurableDiskPlacementRepos{}
	}
	return abstractions.DurableDiskPlacementRepos{
		BackendRepo:    gws.backendRepo,
		ComputeRepo:    gws.computeRepo,
		WorkerRepo:     gws.workerRepo,
		WorkerPoolRepo: gws.workerPoolRepo,
	}
}

func (gws *GatewayService) configureDurableDiskPlacement(ctx context.Context, workspace *types.Workspace, stubConfig *types.StubConfigV1) error {
	return abstractions.ConfigureDurableDiskPlacement(ctx, gws.durableDiskPlacementRepos(), workspace, stubConfig)
}

func (gws *GatewayService) configureUnavailablePrivatePoolFallback(ctx context.Context, workspace *types.Workspace, stubConfig *types.StubConfigV1) error {
	return abstractions.ConfigureUnavailablePrivatePoolFallback(ctx, gws.durableDiskPlacementRepos(), workspace, stubConfig)
}
