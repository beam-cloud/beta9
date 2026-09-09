package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	// evictionKillTimeout bounds how long a killed victim may take to release
	// its resources before the incoming container proceeds regardless.
	evictionKillTimeout = 60 * time.Second
	// evictionPollInterval is how often victims are checked for finalization.
	evictionPollInterval = 250 * time.Millisecond
)

// evictForRequest stops the evictable containers the scheduler chose as
// victims for request and waits until they have been finalized, so the
// incoming container never competes with them for GPU memory. Victims get
// request.EvictDrainSeconds after SIGTERM to finish in-flight work; anything
// still running after that is killed.
func (s *Worker) evictForRequest(ctx context.Context, request *types.ContainerRequest) {
	if request == nil || len(request.EvictContainerIds) == 0 {
		return
	}

	drain := time.Duration(request.EvictDrainSeconds) * time.Second
	victims := make([]string, 0, len(request.EvictContainerIds))
	for _, victimID := range request.EvictContainerIds {
		if victimID == "" || victimID == request.ContainerId {
			continue
		}
		if s.evictContainer(victimID, drain, request.ContainerId) {
			victims = append(victims, victimID)
		}
	}
	if len(victims) == 0 {
		return
	}

	log.Info().
		Str("container_id", request.ContainerId).
		Strs("evict_container_ids", victims).
		Dur("drain", drain).
		Msg("waiting for evicted containers before starting request")
	if remaining := s.waitForContainersFinalized(ctx, victims, drain+evictionKillTimeout); len(remaining) > 0 {
		log.Warn().
			Str("container_id", request.ContainerId).
			Strs("evict_container_ids", remaining).
			Msg("evicted containers did not finalize in time; starting request anyway")
	}
}

// evictContainer begins evicting one local container: it records the reason,
// sends SIGTERM, and escalates to SIGKILL once the drain window passes. It
// reports whether the container was present and still running. Calling it
// again for a container already being evicted is a no-op.
func (s *Worker) evictContainer(containerID string, drain time.Duration, forContainerID string) bool {
	instance, exists := s.containerInstances.Get(containerID)
	if !exists || instance == nil {
		return false
	}
	if exitCode, _ := instance.lifecycleState(); exitCode >= 0 {
		return false
	}
	instance.setStopReason(types.StopContainerReasonEvicted)
	s.containerInstances.Set(containerID, instance)
	s.cancelContainer(containerID)

	// Own the stop escalation so the heartbeat-observed STOPPING path does
	// not kill the victim on the worker's generic grace period instead of its
	// own drain window.
	if !instance.StopEscalationStarted.CompareAndSwap(false, true) {
		return true
	}

	attrs := map[string]string{
		types.EventAttrGracePeriodSeconds: fmt.Sprintf("%d", int64(drain/time.Second)),
	}
	if forContainerID != "" {
		attrs["evicted_for_container_id"] = forContainerID
	}
	s.recordContainerEvent(context.Background(), instance.Request, types.EventContainerEventSchema{
		ID:          types.ContainerEventWorkerEvicted,
		ContainerID: containerID,
		Reason:      string(types.StopContainerReasonEvicted),
		Source:      types.EventSourceWorkerEviction.String(),
		Message:     types.EventMessageEvicted.String(),
		Attrs:       attrs,
	})
	log.Info().Str("container_id", containerID).Dur("drain", drain).Msg("evicting container")
	if err := s.stopContainer(containerID, false); err != nil && !runtimeContainerNotFound(err) {
		log.Warn().Str("container_id", containerID).Err(err).Msg("failed to send graceful stop to evicted container")
	}

	go s.escalateEviction(containerID, drain)
	return true
}

// escalateEviction kills an evicted container that is still present once its
// drain window has passed.
func (s *Worker) escalateEviction(containerID string, drain time.Duration) {
	workerCtx := s.ctx
	if workerCtx == nil {
		workerCtx = context.Background()
	}
	if remaining := s.waitForContainersFinalized(workerCtx, []string{containerID}, drain); len(remaining) == 0 {
		return
	}
	if workerCtx.Err() != nil {
		return
	}
	log.Info().Str("container_id", containerID).Dur("drain", drain).Msg("evicted container still running after drain window; killing")
	if err := s.stopContainer(containerID, true); err != nil && !runtimeContainerNotFound(err) {
		log.Warn().Str("container_id", containerID).Err(err).Msg("failed to kill evicted container")
	}
}

// waitForContainersFinalized blocks until every listed container has been
// removed from the worker's instance table, the timeout passes, or ctx ends.
// It returns the containers that are still present.
func (s *Worker) waitForContainersFinalized(ctx context.Context, containerIDs []string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(evictionPollInterval)
	defer ticker.Stop()
	for {
		remaining := make([]string, 0, len(containerIDs))
		for _, id := range containerIDs {
			if _, exists := s.containerInstances.Get(id); exists {
				remaining = append(remaining, id)
			}
		}
		if len(remaining) == 0 {
			return nil
		}
		if timeout <= 0 || !time.Now().Before(deadline) {
			return remaining
		}
		select {
		case <-ctx.Done():
			return remaining
		case <-ticker.C:
		}
	}
}
