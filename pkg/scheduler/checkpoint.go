package scheduler

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const checkpointHandoffRetryDelay = time.Second

func (s *Scheduler) checkpointReady(request *types.ContainerRequest) bool {
	if request == nil || !request.CheckpointEnabled || request.Checkpoint != nil || request.StubId == "" {
		return true
	}

	s.attachLatestCheckpoint(request)
	if request.Checkpoint != nil {
		return true
	}

	containers, err := s.containerRepo.GetActiveContainersByStubId(request.StubId)
	if err != nil {
		return false
	}

	for _, container := range containers {
		if container.ContainerId != request.ContainerId && container.Status == types.ContainerStatusStopping {
			return false
		}
	}
	return true
}

func (s *Scheduler) attachLatestCheckpoint(request *types.ContainerRequest) {
	if request == nil || !request.CheckpointEnabled || request.Checkpoint != nil {
		return
	}
	if checkpoint := s.latestAvailableCheckpoint(request.StubId); checkpoint != nil {
		s.attachCheckpoint(request, checkpoint)
	}
}

func (s *Scheduler) latestAvailableCheckpoint(stubID string) *types.Checkpoint {
	if s == nil || s.backendRepo == nil || stubID == "" {
		return nil
	}

	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	checkpoint, err := s.backendRepo.GetLatestCheckpointByStubId(ctx, stubID)
	if err != nil || checkpoint == nil || checkpoint.Status != string(types.CheckpointStatusAvailable) {
		return nil
	}
	return checkpoint
}

func (s *Scheduler) attachCheckpoint(request *types.ContainerRequest, checkpoint *types.Checkpoint) {
	if request == nil || checkpoint == nil {
		return
	}
	if request.Checkpoint == nil || request.Checkpoint.CheckpointId != checkpoint.CheckpointId {
		requestLog(log.Info(), request).
			Str("checkpoint_id", checkpoint.CheckpointId).
			Msg("adding checkpoint to request")
	}
	request.Checkpoint = checkpoint
}
