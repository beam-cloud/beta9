package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
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

	// If this worker is assigned to a remote provider (e.g. a bare-metal node, associate the worker state keys with that machine)
	if worker.MachineId != "" {
		machineIndexKey := common.RedisKeys.ProviderMachineWorkerIndex(worker.MachineId)
		err = r.rdb.SAdd(context.TODO(), machineIndexKey, stateKey).Err()
		if err != nil {
			return fmt.Errorf("failed to add worker id key to index <%v>: %w", machineIndexKey, err)
		}
	}

	worker.ResourceVersion = 0
	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(worker)).Err()
	if err != nil {
		return fmt.Errorf("failed to add worker state <%s>: %v", stateKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, r.config.AddWorkerTimeout).Err()
	if err != nil {
		return fmt.Errorf("failed to set worker state ttl <%v>: %w", stateKey, err)
	}

	return nil
}

func (r *WorkerRedisRepository) RemoveWorker(worker *types.Worker) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(worker.Id), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(worker.Id))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	res, err := r.rdb.Exists(context.TODO(), stateKey).Result()
	if err != nil {
		return err
	}

	exists := res > 0
	if !exists {
		return &types.ErrWorkerNotFound{WorkerId: worker.Id}
	}

	// Remove worker state from index
	indexKey := common.RedisKeys.SchedulerWorkerIndex()
	err = r.rdb.SRem(context.TODO(), indexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove worker state key from index <%v>: %w", indexKey, err)
	}

	if worker.MachineId != "" {
		machineIndexKey := common.RedisKeys.ProviderMachineWorkerIndex(worker.MachineId)
		err = r.rdb.SRem(context.TODO(), machineIndexKey, stateKey).Err()
		if err != nil {
			return fmt.Errorf("failed to remove worker id key from index <%v>: %w", machineIndexKey, err)
		}
	}

	err = r.rdb.Del(context.TODO(), stateKey).Err()
	if err != nil {
		return err
	}

	err = r.rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *WorkerRedisRepository) WorkerKeepAlive(workerId string) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))
	stateKey := common.RedisKeys.SchedulerWorkerState(workerId)

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.WorkerStateTtlS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set worker state ttl <%v>: %w", stateKey, err)
	}

	return nil
}

func (r *WorkerRedisRepository) ToggleWorkerAvailable(workerId string) error {
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

	// Make worker available by setting status
	worker.ResourceVersion++
	worker.Status = types.WorkerStatusAvailable
	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(worker)).Err()
	if err != nil {
		return fmt.Errorf("failed to toggle worker state <%s>: %v", stateKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.WorkerStateTtlS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set worker state ttl <%v>: %w", stateKey, err)
	}

	return nil
}

// getWorkers retrieves a list of worker objects from the Redis store that match a given pattern.
// It uses the SCAN command to iterate over the keys in the Redis database, matching the pattern provided.
// If useLock is set to true, a lock will be acquired for each worker and released after retrieval.
// If you can afford to not have the most up-to-date worker information, you can set useLock to false.
func (r *WorkerRedisRepository) getWorkers(useLock bool) ([]*types.Worker, error) {
	workers := []*types.Worker{}

	keys, err := r.rdb.SMembers(context.TODO(), common.RedisKeys.SchedulerWorkerIndex()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve worker state keys: %v", err)
	}

	for _, key := range keys {
		workerId := strings.Split(key, ":")[2]

		if useLock {
			err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
			if err != nil {
				continue
			}
		}

		w, err := r.getWorkerFromKey(key)
		if err != nil {
			if useLock {
				r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))
			}
			continue
		}

		if useLock {
			r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))
		}

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

func (r *WorkerRedisRepository) GetAllWorkers() ([]*types.Worker, error) {
	workers, err := r.getWorkers(true)
	if err != nil {
		return nil, err
	}

	return workers, nil
}

func (r *WorkerRedisRepository) GetAllWorkersInPool(poolId string) ([]*types.Worker, error) {
	workers, err := r.getWorkers(false)
	if err != nil {
		return nil, err
	}

	poolWorkers := []*types.Worker{}
	for _, w := range workers {
		if w.PoolId == poolId {
			poolWorkers = append(poolWorkers, w)
		}
	}

	return poolWorkers, nil
}

func (r *WorkerRedisRepository) UpdateWorkerCapacity(worker *types.Worker, request *types.ContainerRequest, CapacityUpdateType types.CapacityUpdateType) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(worker.Id), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(worker.Id))

	key := common.RedisKeys.SchedulerWorkerState(worker.Id)

	// Retrieve current worker capacity
	w, err := r.getWorkerFromKey(key)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get worker state <%v>: %v", key, err)
	}

	updatedWorker := &types.Worker{}
	if w != nil {
		// Populate updated worker with values from database
		common.CopyStruct(w, updatedWorker)
	} else {
		// Populate updated worker with values from function parameter
		common.CopyStruct(worker, updatedWorker)
	}

	if updatedWorker.ResourceVersion != worker.ResourceVersion {
		return errors.New("invalid worker resource version")
	}

	switch CapacityUpdateType {
	case types.AddCapacity:
		updatedWorker.Cpu = updatedWorker.Cpu + request.Cpu
		updatedWorker.Memory = updatedWorker.Memory + request.Memory

		if request.Gpu != "" {
			updatedWorker.GpuCount += request.GpuCount
		}

	case types.RemoveCapacity:
		updatedWorker.Cpu = updatedWorker.Cpu - request.Cpu
		updatedWorker.Memory = updatedWorker.Memory - request.Memory

		if request.Gpu != "" {
			updatedWorker.GpuCount -= request.GpuCount
		}

		if updatedWorker.Cpu < 0 || updatedWorker.Memory < 0 || updatedWorker.GpuCount < 0 {
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
		log.Println(err)
		return err
	}

	// Push the serialized ContainerRequest
	err = r.rdb.RPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), requestJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to push request: %w", err)
	}

	log.Printf("Request for container %s added to worker: %s\n", request.ContainerId, worker.Id)

	return nil
}

func (r *WorkerRedisRepository) AddContainerRequestToWorker(workerId string, containerId string, request *types.ContainerRequest) error {
	// Serialize the ContainerRequest -> JSON
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to serialize request: %w", err)
	}

	err = r.rdb.Set(context.TODO(), common.RedisKeys.WorkerContainerRequest(workerId, containerId), requestJSON, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set request: %w", err)
	}

	log.Printf("Request for container %s added to worker: %s\n", containerId, workerId)

	return nil
}

func (r *WorkerRedisRepository) RemoveContainerRequestFromWorker(workerId string, containerId string) error {
	err := r.rdb.Del(context.TODO(), common.RedisKeys.WorkerContainerRequest(workerId, containerId)).Err()
	if err != nil {
		return fmt.Errorf("failed to set request: %w", err)
	}

	log.Printf("Removed container %s from worker %s\n", containerId, workerId)
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

func (r *WorkerRedisRepository) SetImagePullLock(workerId, imageId string) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.WorkerImageLock(workerId, imageId), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	return nil
}

func (r *WorkerRedisRepository) RemoveImagePullLock(workerId, imageId string) error {
	return r.lock.Release(common.RedisKeys.WorkerImageLock(workerId, imageId))
}
