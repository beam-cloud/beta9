package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type WorkerRedisRepository struct {
	rdb    *common.RedisClient
	lock   *common.RedisLock
	config types.WorkerConfig
}

func NewWorkerRedisRepository(r *common.RedisClient, config types.WorkerConfig) WorkerRepository {
	lock := common.NewRedisLock(r)
	return &WorkerRedisRepository{rdb: r, lock: lock, config: config}
}

// AddWorker adds or updates a worker
func (r *WorkerRedisRepository) AddWorker(worker *types.Worker) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(worker.Id), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(worker.Id))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	indexKey := common.RedisKeys.SchedulerWorkerIndex()

	// Cache worker state key in index so we don't have to scan for it
	err = r.rdb.SAdd(context.TODO(), indexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add worker state key to index <%v>: %w", indexKey, err)
	}

	worker.ResourceVersion = 0
	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(worker)).Err()
	if err != nil {
		return fmt.Errorf("failed to add worker state <%s>: %v", stateKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, r.config.CleanupPendingWorkerAgeLimit).Err()
	if err != nil {
		return fmt.Errorf("failed to set worker state ttl <%v>: %w", stateKey, err)
	}

	return nil
}

func (r *WorkerRedisRepository) RemoveWorker(workerId string) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))

	stateKey := common.RedisKeys.SchedulerWorkerState(workerId)
	res, err := r.rdb.Exists(context.TODO(), stateKey).Result()
	if err != nil {
		return err
	}

	exists := res > 0
	if !exists {
		return &types.ErrWorkerNotFound{WorkerId: workerId}
	}

	// Remove worker state from index
	indexKey := common.RedisKeys.SchedulerWorkerIndex()
	err = r.rdb.SRem(context.TODO(), indexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove worker state key from index <%v>: %w", indexKey, err)
	}

	err = r.rdb.Del(context.TODO(), stateKey).Err()
	if err != nil {
		return err
	}

	err = r.rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(workerId)).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *WorkerRedisRepository) UpdateWorkerStatus(workerId string, status types.WorkerStatus) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))

	stateKey := common.RedisKeys.SchedulerWorkerState(workerId)
	worker, err := r.getWorkerFromKey(stateKey)
	if err != nil {
		return err
	}

	// Update worker status
	worker.Status = status
	worker.ResourceVersion++
	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(worker)).Err()
	if err != nil {
		return fmt.Errorf("failed to update worker status <%s>: %v", stateKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.WorkerStateTtlS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set worker state ttl <%v>: %w", stateKey, err)
	}

	return nil
}

func (r *WorkerRedisRepository) SetWorkerKeepAlive(workerId string) error {
	stateKey := common.RedisKeys.SchedulerWorkerState(workerId)

	// Set TTL on state key
	err := r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.WorkerStateTtlS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set worker state ttl <%v>: %w", stateKey, err)
	}

	return nil
}

func (r *WorkerRedisRepository) ToggleWorkerAvailable(workerId string) error {
	return r.UpdateWorkerStatus(workerId, types.WorkerStatusAvailable)
}

// getWorkers retrieves a list of worker objects from the Redis store that match a given pattern.
// If useLock is set to true, a lock will be acquired for each worker and released after retrieval.
// If you can afford to not have the most up-to-date worker information, you can set useLock to false.
func (r *WorkerRedisRepository) getWorkers(useLock bool) ([]*types.Worker, error) {
	workers := []*types.Worker{}

	keys, err := r.rdb.SMembers(context.TODO(), common.RedisKeys.SchedulerWorkerIndex()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve worker state keys: %v", err)
	}

	if !useLock {
		workers, err := r.getWorkersFromKeys(keys)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve worker state keys: %v", err)
		}

		return workers, nil
	}

	for _, key := range keys {
		workerId := strings.Split(key, ":")[3]

		err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
		if err != nil {
			continue
		}

		w, err := r.getWorkerFromKey(key)
		if err != nil {
			r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))
			continue
		}

		r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))
		workers = append(workers, w)
	}

	return workers, nil
}

func (r *WorkerRedisRepository) GetWorkerById(workerId string) (*types.Worker, error) {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return nil, err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))

	// Check if the worker key exists
	key := common.RedisKeys.SchedulerWorkerState(workerId)
	exists, err := r.rdb.Exists(context.TODO(), key).Result()
	if err != nil {
		return nil, err
	}

	if exists == 0 {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerId}
	}

	return r.getWorkerFromKey(key)
}

func (r *WorkerRedisRepository) getWorkersFromKeys(keys []string) ([]*types.Worker, error) {
	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(keys))

	// Fetch all workers at once using a pipeline
	for i, key := range keys {
		cmds[i] = pipe.HGetAll(context.TODO(), key)
	}

	_, err := pipe.Exec(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to execute pipeline: %v", err)
	}

	var workers []*types.Worker
	for i, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil || len(res) == 0 {
			// If there is an error or the result is empty, remove the key from the index
			indexKey := common.RedisKeys.SchedulerWorkerIndex()
			r.rdb.SRem(context.TODO(), indexKey, keys[i]).Err()
			continue
		}

		workerId := strings.Split(keys[i], ":")[len(strings.Split(keys[i], ":"))-1]
		worker := &types.Worker{Id: workerId}

		if err = common.ToStruct(res, worker); err != nil {
			return nil, fmt.Errorf("failed to deserialize worker state <%v>: %v", keys[i], err)
		}

		workers = append(workers, worker)
	}

	return workers, nil
}

func (r *WorkerRedisRepository) getWorkerFromKey(key string) (*types.Worker, error) {
	workerId := strings.Split(key, ":")[len(strings.Split(key, ":"))-1]
	worker := &types.Worker{
		Id: workerId,
	}

	res, err := r.rdb.HGetAll(context.TODO(), key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get worker <%s>: %v", key, err)
	}

	if err = common.ToStruct(res, worker); err != nil {
		return nil, fmt.Errorf("failed to deserialize worker state <%v>: %v", key, err)
	}

	return worker, nil
}

func (r *WorkerRedisRepository) GetGpuCounts() (map[string]int, error) {
	gpuCounts := map[string]int{}
	gpuTypes := types.AllGPUTypes()
	for _, gpuType := range gpuTypes {
		gpuCounts[gpuType.String()] = 0
	}

	workers, err := r.getWorkers(false)
	if err != nil {
		return nil, err
	}

	for _, w := range workers {
		if w.Gpu != "" {
			gpuCounts[w.Gpu] += int(w.TotalGpuCount)
		}
	}

	return gpuCounts, nil
}

func (r *WorkerRedisRepository) GetFreeGpuCounts() (map[string]int, error) {
	gpuCounts := map[string]int{}
	gpuTypes := types.AllGPUTypes()
	for _, gpuType := range gpuTypes {
		gpuCounts[gpuType.String()] = 0
	}

	workers, err := r.getWorkers(false)
	if err != nil {
		return nil, err
	}

	for _, w := range workers {
		if w.Gpu != "" {
			gpuCounts[w.Gpu] += int(w.FreeGpuCount)
		}
	}

	return gpuCounts, nil
}

func (r *WorkerRedisRepository) GetGpuAvailability() (map[string]bool, error) {
	gpuAvailability := map[string]bool{}
	gpuTypes := types.AllGPUTypes()
	for _, gpuType := range gpuTypes {
		if gpuType == types.GPU_ANY {
			continue
		}

		gpuAvailability[gpuType.String()] = false
	}

	gpuCounts, err := r.GetGpuCounts()
	if err != nil {
		return nil, err
	}

	for gpuType, count := range gpuCounts {
		if gpuType == types.GPU_ANY.String() {
			continue
		}

		gpuAvailability[gpuType] = count > 0
	}

	return gpuAvailability, nil
}

func (r *WorkerRedisRepository) GetAllWorkers() ([]*types.Worker, error) {
	workers, err := r.getWorkers(true)
	if err != nil {
		return nil, err
	}

	return workers, nil
}

func (r *WorkerRedisRepository) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	workers, err := r.getWorkers(false)
	if err != nil {
		return nil, err
	}

	poolWorkers := []*types.Worker{}
	for _, w := range workers {
		if w.PoolName == poolName {
			poolWorkers = append(poolWorkers, w)
		}
	}

	return poolWorkers, nil
}

func (r *WorkerRedisRepository) CordonAllPendingWorkersInPool(poolName string) error {
	workers, err := r.GetAllWorkersInPool(poolName)
	if err != nil {
		return err
	}

	for _, w := range workers {
		if w.Status != types.WorkerStatusPending {
			continue
		}

		err := r.UpdateWorkerStatus(w.Id, types.WorkerStatusDisabled)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *WorkerRedisRepository) GetAllWorkersOnMachine(machineId string) ([]*types.Worker, error) {
	workers, err := r.getWorkers(false)
	if err != nil {
		return nil, err
	}

	machineWorkers := []*types.Worker{}
	for _, w := range workers {
		if w.MachineId == machineId {
			machineWorkers = append(machineWorkers, w)
		}
	}

	return machineWorkers, nil
}

func (r *WorkerRedisRepository) UpdateWorkerCapacity(worker *types.Worker, request *types.ContainerRequest, CapacityUpdateType types.CapacityUpdateType) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(worker.Id), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(worker.Id))

	key := common.RedisKeys.SchedulerWorkerState(worker.Id)

	// Retrieve current worker capacity
	currentWorker, err := r.getWorkerFromKey(key)
	if err != nil {
		return fmt.Errorf("failed to get worker state <%v>: %v", key, err)
	}

	sourceWorker := currentWorker // worker from the Redis store
	if sourceWorker == nil {
		sourceWorker = worker // worker from the argument
	}

	updatedWorker := &types.Worker{}
	if err := common.CopyStruct(sourceWorker, updatedWorker); err != nil {
		return fmt.Errorf("failed to copy worker struct: %v", err)
	}

	if updatedWorker.ResourceVersion != worker.ResourceVersion {
		return errors.New("invalid worker resource version")
	}

	switch CapacityUpdateType {
	case types.AddCapacity:
		updatedWorker.FreeCpu = updatedWorker.FreeCpu + request.Cpu
		updatedWorker.FreeMemory = updatedWorker.FreeMemory + request.Memory

		if request.Gpu != "" {
			updatedWorker.FreeGpuCount += request.GpuCount
		}

	case types.RemoveCapacity:
		updatedWorker.FreeCpu = updatedWorker.FreeCpu - request.Cpu
		updatedWorker.FreeMemory = updatedWorker.FreeMemory - request.Memory

		if request.Gpu != "" {
			updatedWorker.FreeGpuCount -= request.GpuCount
		}

		if updatedWorker.FreeCpu < 0 || updatedWorker.FreeMemory < 0 || updatedWorker.FreeGpuCount < 0 {
			return errors.New("unable to schedule container, worker out of cpu, memory, or gpu")
		}

	default:
		return errors.New("invalid capacity update type")
	}

	// Update the worker capacity with the new values
	updatedWorker.ResourceVersion++
	err = r.rdb.HSet(context.TODO(), key, common.ToSlice(updatedWorker)).Err()
	if err != nil {
		return fmt.Errorf("failed to update worker capacity <%s>: %v", key, err)
	}

	return nil
}

func (r *WorkerRedisRepository) ScheduleContainerRequest(worker *types.Worker, request *types.ContainerRequest) error {
	// Serialize the ContainerRequest -> JSON
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to serialize request: %w", err)
	}

	// Update the worker capacity first
	err = r.UpdateWorkerCapacity(worker, request, types.RemoveCapacity)
	if err != nil {
		return err
	}

	// Push the serialized ContainerRequest
	err = r.rdb.RPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), requestJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to push request: %w", err)
	}

	log.Info().Str("container_id", request.ContainerId).Str("worker_id", worker.Id).Msg("request for container added")

	return nil
}

func (r *WorkerRedisRepository) AddContainerToWorker(workerId string, containerId string) error {
	containerStateKey := common.RedisKeys.SchedulerContainerState(containerId)

	err := r.rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(workerId), containerStateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add container to worker container index: %w", err)
	}

	log.Info().Str("container_id", containerId).Str("worker_id", workerId).Msg("container added to worker")
	return nil
}

func (r *WorkerRedisRepository) RemoveContainerFromWorker(workerId string, containerId string) error {
	containerStateKey := common.RedisKeys.SchedulerContainerState(containerId)

	err := r.rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(workerId), containerStateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove container from worker container index: %w", err)
	}

	log.Info().Str("container_id", containerId).Str("worker_id", workerId).Msg("container removed from worker")
	return nil
}

func (r *WorkerRedisRepository) GetNextContainerRequest(workerId string) (*types.ContainerRequest, error) {
	queueLength, err := r.rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(workerId)).Result()
	if err != nil {
		return nil, err
	}

	if queueLength == 0 {
		return nil, nil
	}

	requestJSON, err := r.rdb.LPop(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(workerId)).Bytes()
	if err != nil {
		return nil, err
	}

	var request types.ContainerRequest = types.ContainerRequest{}
	err = json.Unmarshal(requestJSON, &request)
	if err != nil {
		return nil, err
	}

	return &request, nil

}

func (r *WorkerRedisRepository) GetId() string {
	return uuid.New().String()[:8]
}

func (r *WorkerRedisRepository) SetContainerResourceValues(workerId string, containerId string, usage types.ContainerResourceUsage) error {
	key := common.RedisKeys.WorkerContainerResourceUsage(workerId, containerId)

	err := r.rdb.HSet(context.TODO(), key, common.ToSlice(usage)).Err()
	if err != nil {
		return fmt.Errorf("failed to set container resource usage: %w", err)
	}

	err = r.rdb.Expire(context.TODO(), key, 2*types.ContainerResourceUsageEmissionInterval).Err()
	if err != nil {
		return fmt.Errorf("failed to set container resource usage expiration: %w", err)
	}

	return nil
}

func (r *WorkerRedisRepository) SetImagePullLock(workerId, imageId string) (string, error) {
	lockKey := common.RedisKeys.WorkerImageLock(workerId, imageId)
	err := r.lock.Acquire(context.TODO(), lockKey, common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return "", err
	}

	token, err := r.lock.Token(lockKey)
	if err != nil {
		return "", err
	}

	return token, nil
}

func (r *WorkerRedisRepository) RemoveImagePullLock(workerId, imageId, token string) error {
	return r.lock.ReleaseWithToken(common.RedisKeys.WorkerImageLock(workerId, imageId), token)
}

func (r *WorkerRedisRepository) GetContainerIps(networkPrefix string) ([]string, error) {
	containerIps, err := r.rdb.SMembers(context.TODO(), common.RedisKeys.WorkerNetworkIpIndex(networkPrefix)).Result()
	if err != nil {
		return nil, err
	}

	return containerIps, nil
}

func (r *WorkerRedisRepository) GetContainerIp(networkPrefix string, containerId string) (string, error) {
	containerIp, err := r.rdb.Get(context.TODO(), common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, containerId)).Result()
	if err != nil {
		return "", err
	}

	return containerIp, nil
}

func (r *WorkerRedisRepository) SetContainerIp(networkPrefix string, containerId, containerIp string) error {
	err := r.rdb.Set(context.TODO(), common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, containerId), containerIp, 0).Err()
	if err != nil {
		return err
	}

	err = r.rdb.SAdd(context.TODO(), common.RedisKeys.WorkerNetworkIpIndex(networkPrefix), containerIp).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *WorkerRedisRepository) SetNetworkLock(networkPrefix string, ttl, retries int) (string, error) {
	lockKey := common.RedisKeys.WorkerNetworkLock(networkPrefix)

	err := r.lock.Acquire(context.TODO(), lockKey, common.RedisLockOptions{TtlS: ttl, Retries: retries})
	if err != nil {
		return "", err
	}

	token, err := r.lock.Token(lockKey)
	if err != nil {
		return "", err
	}

	return token, nil
}

func (r *WorkerRedisRepository) RemoveNetworkLock(networkPrefix string, token string) error {
	return r.lock.ReleaseWithToken(common.RedisKeys.WorkerNetworkLock(networkPrefix), token)
}

func (r *WorkerRedisRepository) RemoveContainerIp(networkPrefix string, containerId string) error {
	containerIp, err := r.rdb.Get(context.TODO(), common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, containerId)).Result()
	if err != nil {
		return err
	}

	err = r.rdb.Del(context.TODO(), common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, containerId), containerIp).Err()
	if err != nil {
		return err
	}

	err = r.rdb.SRem(context.TODO(), common.RedisKeys.WorkerNetworkIpIndex(networkPrefix), containerIp).Err()
	if err != nil {
		return err
	}

	return nil
}
