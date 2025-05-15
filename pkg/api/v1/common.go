package apiv1

import (
	"context"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

type CommonClients struct {
	containerRepo repository.ContainerRepository
	backendRepo   repository.BackendRepository
	scheduler     scheduler.Scheduler
	redisClient   *common.RedisClient
}

func stopDeployments(ctx context.Context, deployments []types.DeploymentWithRelated, clients CommonClients) error {
	for _, deployment := range deployments {
		// Stop scheduled job
		if deployment.StubType == types.StubTypeScheduledJobDeployment {
			if scheduledJob, err := clients.backendRepo.GetScheduledJob(ctx, deployment.Id); err == nil {
				clients.backendRepo.DeleteScheduledJob(ctx, scheduledJob)
			}
		}

		// Stop active containers
		containers, err := clients.containerRepo.GetActiveContainersByStubId(deployment.Stub.ExternalId)
		if err == nil {
			for _, container := range containers {
				clients.scheduler.Stop(&types.StopContainerArgs{ContainerId: container.ContainerId})
			}
		}

		// Disable deployment
		deployment.Active = false
		_, err = clients.backendRepo.UpdateDeployment(ctx, deployment.Deployment)
		if err != nil {
			return errors.New("failed to disable deployment")
		}

		// Publish reload instance event
		eventBus := common.NewEventBus(clients.redisClient)
		eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
			"stub_id":   deployment.Stub.ExternalId,
			"stub_type": deployment.StubType,
			"timestamp": time.Now().Unix(),
		}})
	}

	return nil
}
