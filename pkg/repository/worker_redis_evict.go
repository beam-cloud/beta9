package repository

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

// ErrEvictionVictimsChanged means the chosen victims changed before the placement committed.
var ErrEvictionVictimsChanged = errors.New("eviction victims changed before placement committed")

var ErrInsufficientEvictableCapacity = errors.New("unable to schedule container, worker out of free and evictable capacity")

// requestMayEvict reports whether a request may displace evictable containers.
// Evictable and opportunistic requests never do: they only fill idle capacity.
func requestMayEvict(request *types.ContainerRequest) bool {
	return request != nil && !request.Evictable && !request.OpportunisticOnly
}

// reclaimable is the one definition of capacity a non-evictable request may
// take: an evictable container on the worker that is pending or running and
// not already being evicted. Advertisement, victim selection and the Lua
// re-validation all use it.
func reclaimable(state *types.ContainerState, workerID string) bool {
	return state != nil && state.Evictable && !state.Evicting && state.WorkerId == workerID &&
		(state.Status == types.ContainerStatusPending || state.Status == types.ContainerStatusRunning)
}

// evictionVictim is a reclaimable container the scheduler will stop to make
// room for a non-evictable request.
type evictionVictim struct {
	containerID  string
	stateKey     string
	cpu          int64
	memory       int64
	gpu          uint32
	drainSeconds uint32
}

// indexedContainerFields is the subset of a container state hash the
// scheduler reads for capacity and victim selection.
var indexedContainerFields = []string{"container_id", "status", "worker_id", "evictable", "evicting",
	"cpu", "memory", "gpu", "gpu_count", "drain_seconds", "evict_order", "started_at"}

// indexedContainerStates loads the worker's containers in one pipelined round trip.
func (r *WorkerRedisRepository) indexedContainerStates(ctx context.Context, workerID string) ([]*types.ContainerState, error) {
	indexKey := common.RedisKeys.SchedulerContainerWorkerIndex(workerID)
	keys, err := r.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.HMGet(ctx, key, indexedContainerFields...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	states := make([]*types.ContainerState, 0, len(keys))
	var stale []interface{}
	for i, cmd := range cmds {
		state, ok := containerStateFromFields(cmd.Val())
		if !ok || state.WorkerId != workerID {
			stale = append(stale, keys[i])
			continue
		}
		if state.ContainerId == "" {
			state.ContainerId = containerIDFromStateKey(keys[i])
		}
		states = append(states, state)
	}
	if len(stale) > 0 {
		if err := r.rdb.SRem(ctx, indexKey, stale...).Err(); err != nil {
			return nil, err
		}
	}
	return states, nil
}

// containerStateFromFields decodes an HMGET of indexedContainerFields. ok is
// false when the hash no longer exists (every field nil).
func containerStateFromFields(values []interface{}) (*types.ContainerState, bool) {
	if len(values) != len(indexedContainerFields) {
		return nil, false
	}
	str := func(i int) string { s, _ := values[i].(string); return s }
	i64 := func(i int) int64 { v, _ := strconv.ParseInt(str(i), 10, 64); return v }
	boolean := func(i int) bool { return str(i) == "1" || str(i) == "true" }
	exists := false
	for _, v := range values {
		if v != nil {
			exists = true
			break
		}
	}
	if !exists {
		return nil, false
	}
	return &types.ContainerState{
		ContainerId:  str(0),
		Status:       types.ContainerStatus(str(1)),
		WorkerId:     str(2),
		Evictable:    boolean(3),
		Evicting:     boolean(4),
		Cpu:          i64(5),
		Memory:       i64(6),
		Gpu:          str(7),
		GpuCount:     uint32(i64(8)),
		DrainSeconds: uint32(i64(9)),
		EvictOrder:   int32(i64(10)),
		StartedAt:    i64(11),
	}, true
}

// selectEvictionVictims picks reclaimable containers covering a shortfall:
// lowest EvictOrder first, then not yet running, then newest.
func (r *WorkerRedisRepository) selectEvictionVictims(ctx context.Context, workerID string, deficitCPU, deficitMemory, deficitGPU int64) ([]evictionVictim, error) {
	if deficitCPU <= 0 && deficitMemory <= 0 && deficitGPU <= 0 {
		return nil, nil
	}
	states, err := r.indexedContainerStates(ctx, workerID)
	if err != nil {
		return nil, err
	}
	return chooseVictims(states, workerID, deficitCPU, deficitMemory, deficitGPU)
}

func chooseVictims(states []*types.ContainerState, workerID string, deficitCPU, deficitMemory, deficitGPU int64) ([]evictionVictim, error) {
	type candidate struct {
		evictionVictim
		order     int32
		running   bool
		startedAt int64
	}
	candidates := make([]candidate, 0)
	for _, state := range states {
		if !reclaimable(state, workerID) {
			continue
		}
		candidates = append(candidates, candidate{
			evictionVictim: evictionVictim{
				containerID:  state.ContainerId,
				stateKey:     common.RedisKeys.SchedulerContainerState(state.ContainerId),
				cpu:          state.Cpu,
				memory:       capacityMemoryForRequest(&types.ContainerRequest{Memory: state.Memory}),
				gpu:          gpuCountForCapacity(state.Gpu, nil, state.GpuCount),
				drainSeconds: state.DrainSeconds,
			},
			order:     state.EvictOrder,
			running:   state.Status == types.ContainerStatusRunning,
			startedAt: state.StartedAt,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].order != candidates[j].order {
			return candidates[i].order < candidates[j].order
		}
		if candidates[i].running != candidates[j].running {
			return !candidates[i].running
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

// requestDependencies assigns each request only the victims whose capacity it
// consumes: idle capacity first, so a request that fits what is free carries
// no eviction dependency.
func requestDependencies(queued []queuedContainerRequest, free [3]int64, victims []evictionVictim) (map[string][]string, map[string]uint32) {
	deps := make(map[string][]string, len(queued))
	drains := make(map[string]uint32, len(queued))
	remaining := make([][3]int64, len(victims))
	for i, v := range victims {
		remaining[i] = [3]int64{v.cpu, v.memory, int64(v.gpu)}
	}
	// Requests that may not evict are placed first: they were validated to
	// fit idle capacity and must never be charged to a victim.
	order := make([]*types.ContainerRequest, 0, len(queued))
	for _, q := range queued {
		if !requestMayEvict(q.request) {
			order = append(order, q.request)
		}
	}
	for _, q := range queued {
		if requestMayEvict(q.request) {
			order = append(order, q.request)
		}
	}
	for _, request := range order {
		need := [3]int64{request.Cpu, capacityMemoryForRequest(request),
			int64(gpuCountForCapacity(request.Gpu, request.GpuRequest, request.GpuCount))}
		for axis := range need {
			take := min(need[axis], max(free[axis], 0))
			free[axis] -= take
			need[axis] -= take
		}
		if !requestMayEvict(request) {
			continue
		}
		for i := range victims {
			if need[0] <= 0 && need[1] <= 0 && need[2] <= 0 {
				break
			}
			used := false
			for axis := range need {
				if need[axis] > 0 && remaining[i][axis] > 0 {
					take := min(need[axis], remaining[i][axis])
					remaining[i][axis] -= take
					need[axis] -= take
					used = true
				}
			}
			if used {
				deps[request.ContainerId] = append(deps[request.ContainerId], victims[i].containerID)
				drains[request.ContainerId] = max(drains[request.ContainerId], victims[i].drainSeconds)
			}
		}
	}
	return deps, drains
}
