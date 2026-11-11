package pod

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/rs/zerolog/log"
)

// deleteKeepWarmLock removes a container's keep-warm lock once the container
// is gone. A sandbox or VM that never expires stores its lock without a TTL,
// so the lock would otherwise stay in Redis forever.
func (i *podInstance) deleteKeepWarmLock(containerId string) {
	if i.ContainerRepo == nil || i.Workspace == nil || i.Stub == nil || containerId == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Zero seconds deletes the lock.
	setPodKeepWarmLock(ctx, i.ContainerRepo, i.Workspace.Name, i.Stub.ExternalId, containerId, 0)
}

func setPodKeepWarmLock(ctx context.Context, containerRepo repository.ContainerRepository, workspaceName, stubId, containerId string, keepWarmSeconds int) {
	if containerRepo == nil {
		return
	}

	if err := containerRepo.SetPodKeepWarmLock(ctx, workspaceName, stubId, containerId, keepWarmSeconds); err != nil {
		log.Error().
			Err(err).
			Str("stub_id", stubId).
			Str("container_id", containerId).
			Int("keep_warm_seconds", keepWarmSeconds).
			Msg("failed to update pod keep-warm lock")
	}
}
