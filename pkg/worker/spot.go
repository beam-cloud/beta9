package worker

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

func (s *Worker) spotContainers() (ids []string, onlySpot bool) {
	onlySpot = true
	if s.containerInstances == nil {
		return
	}
	s.containerInstances.Range(func(id string, instance *ContainerInstance) bool {
		if instance == nil || instance.Request == nil || instance.Request.CPUTier() != types.CPUTierSpot {
			onlySpot = false
		} else {
			ids = append(ids, id)
		}
		return true
	})
	return
}

// Borrowed work must not pin an otherwise idle or retiring worker. Reuse the
// same eviction/durability barrier as replacement requests; never kill a VM
// merely because its durable stop is slow or unavailable.
func (s *Worker) reclaimSpotForScaleIn(last, now time.Time) {
	ids, onlySpot := s.spotContainers()
	if len(ids) == 0 || s.spotScaleIn.Load() {
		return
	}
	if !s.draining {
		if s.persistent || !onlySpot || now.Sub(last).Seconds() <= defaultWorkerSpindownTimeS {
			return
		}
		if err := s.setWorkerKeepAlive(); err != nil || s.poolHeadroom.Load() {
			return
		}
		if err := s.disableScheduling(); err != nil {
			return
		}
		s.draining, s.drainStartedAt = true, now
	}
	if !s.spotScaleIn.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.spotScaleIn.Store(false)
		ctx, cancel := context.WithTimeout(s.ctx, 3*time.Minute)
		defer cancel()
		if err := s.evictForRequest(ctx, &types.ContainerRequest{EvictContainerIds: ids, EvictDrainSeconds: 30}); err != nil {
			log.Warn().Err(err).Str("worker_id", s.workerId).Msg("Spot scale-in is waiting for durable release")
		}
	}()
}
