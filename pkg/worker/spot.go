package worker

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// spotContainers reports the worker's Spot containers and whether it runs nothing else.
func (s *Worker) spotContainers() (ids []string, onlySpot bool) {
	onlySpot = true
	if s.containerInstances == nil {
		return
	}
	s.containerInstances.Range(func(id string, instance *ContainerInstance) bool {
		if instance == nil || instance.Request.CPUTier() != types.CPUTierSpot {
			onlySpot = false
		} else {
			ids = append(ids, id)
		}
		return true
	})
	return
}

// reclaimSpotCapacity evicts Spot containers when the worker is draining, when
// it holds its pool's minimum free capacity, or when Spot alone has kept it up
// past the idle spindown timeout (it starts draining first).
func (s *Worker) reclaimSpotCapacity(lastContainerRequest, now time.Time) {
	ids, onlySpot := s.spotContainers()
	if len(ids) == 0 || s.reclaimingSpot.Load() {
		return
	}
	if !s.draining && !(onlySpot && s.poolHeadroom.Load()) {
		if s.persistent || !onlySpot || now.Sub(lastContainerRequest).Seconds() <= defaultWorkerSpindownTimeS {
			return
		}
		// The refreshed keepalive may report headroom; the next pass then evicts without draining.
		if s.setWorkerKeepAlive() != nil || s.poolHeadroom.Load() || s.disableScheduling() != nil {
			return
		}
		s.draining, s.drainStartedAt = true, now
	}
	if !s.reclaimingSpot.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.reclaimingSpot.Store(false)
		ctx, cancel := context.WithTimeout(s.ctx, spotReleaseTimeout)
		defer cancel()
		// Evicting like a request's victims stops Spot VMs durably.
		if err := s.evictForRequest(ctx, &types.ContainerRequest{EvictContainerIds: ids}); err != nil {
			log.Warn().Err(err).Str("worker_id", s.workerId).Msg("Spot containers have not released their capacity yet")
		}
	}()
}
