package pod

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/rs/zerolog/log"
)

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
