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

// Spot work must not hold pool headroom or keep an idle or draining worker
// alive. It is evicted like a replacement request's victims, so Spot VMs stop
// durably instead of being killed.
func (s *Worker) reclaimSpotCapacity(last, now time.Time) {
	ids, onlySpot := s.spotContainers()
	if len(ids) == 0 || s.spotScaleIn.Load() {
		return
	}
	if !s.draining && !(onlySpot && s.poolHeadroom.Load()) {
		if s.persistent || !onlySpot || now.Sub(last).Seconds() <= defaultWorkerSpindownTimeS {
			return
		}
		if s.setWorkerKeepAlive() != nil || s.poolHeadroom.Load() || s.disableScheduling() != nil {
			return
		}
		s.draining, s.drainStartedAt = true, now
	}
	if !s.spotScaleIn.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.spotScaleIn.Store(false)
		ctx, cancel := context.WithTimeout(s.ctx, spotReleaseTimeout)
		defer cancel()
		if err := s.evictForRequest(ctx, &types.ContainerRequest{EvictContainerIds: ids, EvictDrainSeconds: types.SpotDrainSeconds}); err != nil {
			log.Warn().Err(err).Str("worker_id", s.workerId).Msg("Spot scale-in is waiting for durable release")
		}
	}()
}
