package task

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// ContainerIsRunning distinguishes a stopped container from a repository outage.
// A lost gateway heartbeat alone must not cause another execution of user code.
func ContainerIsRunning(containers repository.ContainerRepository, containerID string) (bool, error) {
	container, err := containers.GetContainerState(containerID)
	if (&types.ErrContainerStateNotFound{}).From(err) {
		return false, nil
	}
	return container != nil && container.Status == types.ContainerStatusRunning, err
}

// KeepFinalizationAlive transfers heartbeat ownership to the accepted request.
// The returned stop function joins the renewer so cleanup cannot race a refresh.
func KeepFinalizationAlive(ctx context.Context, taskID string, refresh func(context.Context) error) (func(), error) {
	if err := refresh(ctx); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(types.TaskHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := refresh(ctx); err != nil && ctx.Err() == nil {
					log.Error().Err(err).Str("task_id", taskID).Msg("failed to renew task finalization heartbeat")
				}
			}
		}
	}()
	return func() { cancel(); <-done }, nil
}
