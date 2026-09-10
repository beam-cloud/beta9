package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// evictionKillTimeout bounds how long a killed victim may take to release its resources.
var evictionKillTimeout = 60 * time.Second

// evictionRecheckInterval is a safety net behind the release notification:
// victims are re-checked at least this often even if no signal arrives.
const evictionRecheckInterval = time.Second

// maxPreemptionDrain caps a victim's drain when a serverless request is
// waiting on its resources; an endpoint's drain_seconds does not extend it.
var maxPreemptionDrain = 10 * time.Second

// ErrEvictionIncomplete is returned when victims chosen for a request were
// still holding their resources after the drain and kill windows passed.
var ErrEvictionIncomplete = errors.New("evicted containers did not release their resources in time")

// evictForRequest stops the request's victims and waits until they are
// finalized. A victim still present after the drain and kill windows fails the
// request rather than letting it start on held resources.
func (s *Worker) evictForRequest(ctx context.Context, request *types.ContainerRequest) error {
	if request == nil || len(request.EvictContainerIds) == 0 {
		return nil
	}

	drain := min(time.Duration(request.EvictDrainSeconds)*time.Second, maxPreemptionDrain)
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
		return nil
	}

	log.Info().
		Str("container_id", request.ContainerId).
		Strs("evict_container_ids", victims).
		Dur("drain", drain).
		Msg("waiting for evicted containers before starting request")
	waitStart := time.Now()
	remaining := s.waitForContainersFinalized(ctx, victims, drain+evictionKillTimeout)
	metrics.RecordWorkerStartupPhase("evict_wait", time.Since(waitStart), request, map[string]string{
		"victims":  fmt.Sprintf("%d", len(victims)),
		"released": fmt.Sprintf("%t", len(remaining) == 0),
	})
	if len(remaining) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	log.Error().
		Str("container_id", request.ContainerId).
		Strs("evict_container_ids", remaining).
		Msg("evicted containers did not finalize in time; failing request instead of starting on held resources")
	s.cancelContainer(request.ContainerId)
	return fmt.Errorf("%w: %v", ErrEvictionIncomplete, remaining)
}

// evictContainer sends SIGTERM and escalates to SIGKILL after the drain
// window. It reports whether the container is present and must be waited for;
// a container already exiting is waited for but not signalled again.
func (s *Worker) evictContainer(containerID string, drain time.Duration, forContainerID string) bool {
	instance, exists := s.containerInstances.Get(containerID)
	if !exists || instance == nil {
		return false
	}
	if exitCode, _ := instance.lifecycleState(); exitCode >= 0 {
		return true
	}
	instance.setStopReason(types.StopContainerReasonEvicted)
	s.containerInstances.Set(containerID, instance)
	s.cancelContainer(containerID)

	// Own the escalation so the STOPPING heartbeat path does not kill on the generic grace.
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
	if drain == 0 {
		// Immediate preemption needs one runtime signal. A synchronous TERM
		// followed by KILL adds a second runsc invocation before GPU cleanup.
		if err := s.stopContainer(containerID, true); err != nil && !runtimeContainerNotFound(err) {
			log.Warn().Str("container_id", containerID).Err(err).Msg("failed to kill evicted container")
		}
		return true
	}
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

// waitForContainersFinalized waits on the instance table's removal signal
// until every container is gone, the timeout passes or ctx ends. It returns
// the containers still present.
func (s *Worker) waitForContainersFinalized(ctx context.Context, containerIDs []string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	recheck := time.NewTicker(evictionRecheckInterval)
	defer recheck.Stop()
	for {
		// Take the signal before checking so a removal between the check and
		// the wait cannot be missed.
		removed := s.containerInstances.Removed()
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
		case <-removed:
		case <-recheck.C:
		}
	}
}
