package worker

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// borrowedContainers reports the worker's containers on borrowed CPU tier
// capacity and whether it runs nothing else.
func (s *Worker) borrowedContainers() (ids []string, onlyBorrowed bool) {
	onlyBorrowed = true
	if s.containerInstances == nil {
		return
	}
	s.containerInstances.Range(func(id string, instance *ContainerInstance) bool {
		if instance == nil || !instance.Request.Borrowed() {
			onlyBorrowed = false
		} else {
			ids = append(ids, id)
		}
		return true
	})
	return
}

// mayReclaimBorrowed reports whether reclaimBorrowedCapacity could do anything
// for some set of containers. It is false for a worker that is not draining,
// holds no pool headroom and either never spins down or has just been sent a
// request, which is every pass of the request loop on a busy worker. Finding
// the borrowed containers parses the stub config of every container the worker
// holds, so the loop asks this first: on a worker holding thousands, that
// search for each request received is the rate it can take requests at.
func (s *Worker) mayReclaimBorrowed(lastContainerRequest, now time.Time) bool {
	if s.reclaiming.Load() {
		return false
	}
	if s.draining || s.poolHeadroom.Load() {
		return true
	}
	return !s.persistent && now.Sub(lastContainerRequest).Seconds() > defaultWorkerSpindownTimeS
}

// reclaimBorrowedCapacity evicts borrowed containers when the worker is
// draining, when it holds its pool's minimum free capacity, or when borrowed
// work alone has kept it up past the idle spindown timeout (it starts draining
// first).
func (s *Worker) reclaimBorrowedCapacity(lastContainerRequest, now time.Time) {
	if !s.mayReclaimBorrowed(lastContainerRequest, now) {
		return
	}
	ids, onlyBorrowed := s.borrowedContainers()
	if len(ids) == 0 || s.reclaiming.Load() {
		return
	}
	if !s.draining && !(onlyBorrowed && s.poolHeadroom.Load()) {
		if s.persistent || !onlyBorrowed || now.Sub(lastContainerRequest).Seconds() <= defaultWorkerSpindownTimeS {
			return
		}
		// The refreshed keepalive may report headroom; the next pass then evicts without draining.
		if s.setWorkerKeepAlive() != nil || s.poolHeadroom.Load() || s.disableScheduling() != nil {
			return
		}
		s.draining, s.drainStartedAt = true, now
	}
	if !s.reclaiming.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.reclaiming.Store(false)
		ctx, cancel := context.WithTimeout(s.ctx, borrowedReleaseTimeout)
		defer cancel()
		// Evicting like a request's victims stops borrowed VMs durably.
		if err := s.evictForRequest(ctx, &types.ContainerRequest{EvictContainerIds: ids}); err != nil {
			log.Warn().Err(err).Str("worker_id", s.workerId).Msg("borrowed containers have not released their capacity yet")
		}
	}()
}
