package repository

import (
	"context"
	"errors"
	"sort"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// ErrEvictionVictimsChanged is returned when the evictable containers chosen to
// make room for a request were stopped, evicted, or moved before the
// placement committed. The caller should re-read the worker and retry.
var ErrEvictionVictimsChanged = errors.New("eviction victims changed before placement committed")

// ErrInsufficientEvictableCapacity is returned when a request does not fit in
// a worker's free capacity and its evictable containers cannot cover the
// shortfall either.
var ErrInsufficientEvictableCapacity = errors.New("unable to schedule container, worker out of free and evictable capacity")

// evictionVictim is a running evictable container the scheduler will stop to
// make room for a non-evictable request.
type evictionVictim struct {
	containerID  string
	stateKey     string
	cpu          int64
	memory       int64
	gpu          uint32
	drainSeconds uint32
}

// selectEvictionVictims picks the evictable containers on a worker that cover
// a capacity shortfall. Deficits at or below zero mean the request already
// fits on that axis. Candidates are ordered by EvictOrder (lower first, so
// prefill replicas go before decode/serve), then newest first so the replicas
// that have been serving longest keep their warm caches. Callers hold the
// worker lease; the Lua schedule script re-validates each victim.
func (r *WorkerRedisRepository) selectEvictionVictims(ctx context.Context, workerID string, deficitCPU, deficitMemory, deficitGPU int64) ([]evictionVictim, error) {
	if deficitCPU <= 0 && deficitMemory <= 0 && deficitGPU <= 0 {
		return nil, nil
	}

	keys, err := r.rdb.SMembers(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(workerID)).Result()
	if err != nil {
		return nil, err
	}

	type candidate struct {
		evictionVictim
		order     int32
		startedAt int64
	}
	candidates := make([]candidate, 0)
	for _, key := range keys {
		state, exists, err := r.getIndexedContainerState(ctx, workerID, key)
		if err != nil {
			return nil, err
		}
		if !exists || state.WorkerId != workerID || !state.Evictable || state.Evicting || state.Status != types.ContainerStatusRunning {
			continue
		}
		containerID := state.ContainerId
		if containerID == "" {
			containerID = containerIDFromStateKey(key)
		}
		candidates = append(candidates, candidate{
			evictionVictim: evictionVictim{
				containerID:  containerID,
				stateKey:     key,
				cpu:          state.Cpu,
				memory:       capacityMemoryForRequest(&types.ContainerRequest{Memory: state.Memory}),
				gpu:          gpuCountForCapacity(state.Gpu, nil, state.GpuCount),
				drainSeconds: state.DrainSeconds,
			},
			order:     state.EvictOrder,
			startedAt: state.StartedAt,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].order != candidates[j].order {
			return candidates[i].order < candidates[j].order
		}
		if candidates[i].startedAt != candidates[j].startedAt {
			return candidates[i].startedAt > candidates[j].startedAt
		}
		return candidates[i].containerID < candidates[j].containerID
	})

	victims := make([]evictionVictim, 0)
	for _, c := range candidates {
		if deficitCPU <= 0 && deficitMemory <= 0 && deficitGPU <= 0 {
			break
		}
		helps := (deficitCPU > 0 && c.cpu > 0) || (deficitMemory > 0 && c.memory > 0) || (deficitGPU > 0 && c.gpu > 0)
		if !helps {
			continue
		}
		victims = append(victims, c.evictionVictim)
		deficitCPU -= c.cpu
		deficitMemory -= c.memory
		deficitGPU -= int64(c.gpu)
	}
	if deficitCPU > 0 || deficitMemory > 0 || deficitGPU > 0 {
		return nil, ErrInsufficientEvictableCapacity
	}
	return victims, nil
}
