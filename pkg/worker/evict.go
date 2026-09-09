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

// evictionKillTimeout bounds how long a killed victim may take to release its
// resources after its drain window. Victims still present after this fail the
// incoming request rather than letting it start on top of them. It is a
// variable so tests can shorten it.
var evictionKillTimeout = 60 * time.Second

// evictionRecheckInterval is a safety net behind the release notification:
// victims are re-checked at least this often even if no signal arrives.
const evictionRecheckInterval = time.Second

// maxPreemptionDrain caps how long a victim may keep its resources after a
// serverless request has been placed on them. An endpoint's drain_seconds
// governs graceful retirement by its controller; preemption is the platform's
// deadline and a model's own drain preference does not extend it.
var maxPreemptionDrain = 10 * time.Second

// ErrEvictionIncomplete is returned when victims chosen for a request were
// still holding their resources after the drain and kill windows passed.
var ErrEvictionIncomplete = errors.New("evicted containers did not release their resources in time")

// evictForRequest stops the evictable containers the scheduler chose as
// victims for request and waits until they have been finalized, so the
// incoming container never competes with them for GPU memory. Victims get
// request.EvictDrainSeconds after SIGTERM to finish in-flight work; anything
// still running after that is killed.
//
// If any victim is still present once the kill window has also passed, the
// scheduler-granted capacity is not actually free. The request's startup
// context is cancelled so the caller fails it through the usual pre-start
// path, and ErrEvictionIncomplete is returned.
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

// evictContainer begins evicting one local container: it records the reason,
// sends SIGTERM, and escalates to SIGKILL once the drain window passes. It
// reports whether the container is present, and therefore must be waited for
// before the incoming request starts. A container that already has a terminal
// exit code is still finalizing (its GPU and instance entry are released after
// the exit code is recorded), so it counts as a victim to wait for but is not
// signalled again. Calling it again for a container already being evicted is
// a no-op.
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
// It wakes on the instance table's removal signal, so the incoming request
// resumes as soon as the last victim's resources are released, with a slow
// re-check as a safety net. It returns the containers still present.
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
