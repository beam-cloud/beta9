package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
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

const (
	schedulerWorkerLockTTL      = 10
	schedulerWorkerLockRetries  = 20
	schedulerWorkerLockInterval = 50 * time.Millisecond
	schedulerAssignmentIDField  = "schedule_assignment_id"
)

var schedulerWorkerLockOptions = common.RedisLockOptions{
	TtlS:          schedulerWorkerLockTTL,
	Retries:       schedulerWorkerLockRetries,
	RetryInterval: schedulerWorkerLockInterval,
}

var setWorkerNetworkContainerIPScript = redis.NewScript(`
local container_key = KEYS[1]
local index_key = KEYS[2]
local ref_counts_key = KEYS[3]
local owner_key = KEYS[4]
local ip = ARGV[1]
local container_id = ARGV[2]
local owner_key_prefix = ARGV[3]

local old_ip = redis.call("GET", container_key)
if old_ip == ip then
	redis.call("SADD", index_key, ip)
	redis.call("HSET", ref_counts_key, ip, 1)
	redis.call("SET", owner_key, container_id)
	return 1
end

local existing_owner = redis.call("GET", owner_key)
if existing_owner and existing_owner ~= false and existing_owner ~= "" and existing_owner ~= container_id then
	return redis.error_reply("ip address already reserved by " .. existing_owner)
end

if old_ip and old_ip ~= false and old_ip ~= "" then
	local old_owner_key = owner_key_prefix .. old_ip
	local old_owner = redis.call("GET", old_owner_key)
	if old_owner == container_id then
		redis.call("DEL", old_owner_key)
	end
	redis.call("HDEL", ref_counts_key, old_ip)
	redis.call("SREM", index_key, old_ip)
end

redis.call("SET", container_key, ip)
redis.call("SADD", index_key, ip)
redis.call("HSET", ref_counts_key, ip, 1)
redis.call("SET", owner_key, container_id)
return 1
`)

var removeWorkerNetworkContainerIPScript = redis.NewScript(`
local container_key = KEYS[1]
local index_key = KEYS[2]
local ref_counts_key = KEYS[3]
local container_id = ARGV[1]
local owner_key_prefix = ARGV[2]

local ip = redis.call("GET", container_key)
if not ip or ip == false or ip == "" then
	return 0
end

redis.call("DEL", container_key)
local owner_key = owner_key_prefix .. ip
local owner = redis.call("GET", owner_key)
if not owner or owner == false or owner == "" or owner == container_id then
	redis.call("DEL", owner_key)
	redis.call("HDEL", ref_counts_key, ip)
	redis.call("SREM", index_key, ip)
end
return 1
`)

var moveWorkerNetworkContainerIPScript = redis.NewScript(`
local from_key = KEYS[1]
local to_key = KEYS[2]
local index_key = KEYS[3]
local ref_counts_key = KEYS[4]
local owner_key = KEYS[5]
local ip = ARGV[1]
local from_container_id = ARGV[2]
local to_container_id = ARGV[3]

local current_ip = redis.call("GET", from_key)
if current_ip ~= ip then
	return redis.error_reply("source container does not own requested ip")
end

local owner = redis.call("GET", owner_key)
if owner ~= from_container_id then
	return redis.error_reply("ip owner mismatch")
end

local to_ip = redis.call("GET", to_key)
if to_ip and to_ip ~= false and to_ip ~= "" and to_ip ~= ip then
	return redis.error_reply("destination container already has a different ip")
end

redis.call("DEL", from_key)
redis.call("SET", to_key, ip)
redis.call("SET", owner_key, to_container_id)
redis.call("SADD", index_key, ip)
redis.call("HSET", ref_counts_key, ip, 1)
return 1
`)

var drainWorkerRequestsScript = redis.NewScript(`
local requests = redis.call("LRANGE", KEYS[1], 0, -1)
if #requests > 0 then
	redis.call("DEL", KEYS[1])
end
return requests
`)

var adjustWorkerCapacityScript = redis.NewScript(`
local status = redis.call("HGET", KEYS[1], "status")
if not status then
    return {-1}
end
if ARGV[5] == "1" and status ~= "available" then
    return {-2, status}
end

local free_cpu = tonumber(redis.call("HGET", KEYS[1], "free_cpu") or "0") + tonumber(ARGV[1])
local free_memory = tonumber(redis.call("HGET", KEYS[1], "free_memory") or "0") + tonumber(ARGV[2])
local free_gpu = tonumber(redis.call("HGET", KEYS[1], "gpu_count") or "0") + tonumber(ARGV[3])
if free_cpu < 0 or free_memory < 0 or free_gpu < 0 then
    return {-3}
end

local total_cpu = tonumber(redis.call("HGET", KEYS[1], "total_cpu") or "0")
local total_memory = tonumber(redis.call("HGET", KEYS[1], "total_memory") or "0")
local total_gpu = tonumber(redis.call("HGET", KEYS[1], "total_gpu_count") or "0")
if total_cpu > 0 and free_cpu > total_cpu then free_cpu = total_cpu end
if total_memory > 0 and free_memory > total_memory then free_memory = total_memory end
if total_gpu > 0 and free_gpu > total_gpu then free_gpu = total_gpu end

redis.call("HSET", KEYS[1],
    "free_cpu", free_cpu,
    "free_memory", free_memory,
    "gpu_count", free_gpu)
local version = redis.call("HINCRBY", KEYS[1], "resource_version", ARGV[4])
return {1, redis.call("HGET", KEYS[1], "machine_id") or "", free_cpu, free_memory, free_gpu, version}
`)

func NewWorkerRedisRepository(r *common.RedisClient, config types.WorkerConfig) WorkerRepository {
	lock := common.NewRedisLock(r)
	return &WorkerRedisRepository{rdb: r, lock: lock, config: config}
}

// AddWorker adds or updates a worker
func (r *WorkerRedisRepository) AddWorker(worker *types.Worker) error {
	ctx := context.TODO()
	err := r.lock.Acquire(ctx, common.RedisKeys.SchedulerWorkerLock(worker.Id), schedulerWorkerLockOptions)
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(worker.Id))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	var oldWorker *types.Worker
	if existingWorker, err := r.getWorkerFromKey(stateKey); err == nil {
		oldWorker = existingWorker
	} else {
		var notFound *types.ErrWorkerNotFound
		if !errors.As(err, &notFound) {
			return err
		}
	}

	worker.ResourceVersion = 0

	pipe := r.rdb.TxPipeline()
	pipe.HSet(ctx, stateKey, common.ToSlice(worker))
	pipe.Expire(ctx, stateKey, r.config.CleanupPendingWorkerAgeLimit)
	for _, indexKey := range r.workerIndexKeys(worker) {
		pipe.SAdd(ctx, indexKey, stateKey)
	}
	if oldWorker != nil {
		if oldWorker.PoolName != "" && oldWorker.PoolName != worker.PoolName {
			pipe.SRem(ctx, common.RedisKeys.SchedulerWorkerPoolIndex(oldWorker.PoolName), stateKey)
		}
		if oldWorker.MachineId != "" && oldWorker.MachineId != worker.MachineId {
			pipe.SRem(ctx, common.RedisKeys.SchedulerWorkerMachineIndex(oldWorker.MachineId), stateKey)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to add worker state and indexes <%s>: %w", stateKey, err)
	}

	return nil
}

func (r *WorkerRedisRepository) RemoveWorker(workerId string) error {
	ctx := context.TODO()
	err := r.lock.Acquire(ctx, common.RedisKeys.SchedulerWorkerLock(workerId), schedulerWorkerLockOptions)
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))

	stateKey := common.RedisKeys.SchedulerWorkerState(workerId)
	res, err := r.rdb.Exists(ctx, stateKey).Result()
	if err != nil {
		return err
	}

	exists := res > 0
	if !exists {
		return &types.ErrWorkerNotFound{WorkerId: workerId}
	}

	worker, err := r.getWorkerFromKey(stateKey)
	if err != nil {
		return err
	}

	requeued, err := r.requeueWorkerRequests(ctx, workerId)
	if err != nil {
		return err
	}

	if err := r.removeWorkerIndexEntries(ctx, stateKey, worker); err != nil {
		return err
	}

	err = r.rdb.Del(ctx, stateKey).Err()
	if err != nil {
		return err
	}

	if requeued > 0 {
		log.Info().Str("worker_id", workerId).Int("request_count", requeued).Msg("requeued requests from removed worker")
	}

	return nil
}

func (r *WorkerRedisRepository) requeueWorkerRequests(ctx context.Context, workerId string) (int, error) {
	queueKey := common.RedisKeys.SchedulerWorkerRequests(workerId)
	result, err := drainWorkerRequestsScript.Run(ctx, r.rdb, []string{queueKey}).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to drain worker request queue <%s>: %w", queueKey, err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected worker request drain result: %T", result)
	}
	if len(items) == 0 {
		return 0, nil
	}

	rawItems := make([]string, 0, len(items))
	for _, item := range items {
		raw, ok := item.(string)
		if !ok {
			return 0, fmt.Errorf("unexpected worker request type: %T", item)
		}
		rawItems = append(rawItems, raw)
	}

	pipe := r.rdb.Pipeline()
	now := time.Now()
	for _, raw := range rawItems {
		var request types.ContainerRequest
		if err := json.Unmarshal([]byte(raw), &request); err != nil {
			_ = r.restoreWorkerRequests(ctx, workerId, rawItems)
			return 0, fmt.Errorf("failed to deserialize queued request for worker <%s>: %w", workerId, err)
		}

		request.RetryCount++
		request.Timestamp = now
		jsonData, err := json.Marshal(&request)
		if err != nil {
			_ = r.restoreWorkerRequests(ctx, workerId, rawItems)
			return 0, fmt.Errorf("failed to serialize requeued request for worker <%s>: %w", workerId, err)
		}

		pipe.ZAdd(ctx, common.RedisKeys.SchedulerContainerRequests(), redis.Z{
			Score:  float64(now.UnixNano()),
			Member: jsonData,
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		_ = r.restoreWorkerRequests(ctx, workerId, rawItems)
		return 0, fmt.Errorf("failed to requeue drained requests for worker <%s>: %w", workerId, err)
	}

	metrics.RecordSchedulerBacklogDepth(r.rdb.ZCard(ctx, common.RedisKeys.SchedulerContainerRequests()).Val())
	return len(items), nil
}

func (r *WorkerRedisRepository) restoreWorkerRequests(ctx context.Context, workerId string, requests []string) error {
	if len(requests) == 0 {
		return nil
	}

	values := make([]interface{}, 0, len(requests))
	for _, request := range requests {
		values = append(values, request)
	}

	return r.rdb.RPush(ctx, common.RedisKeys.SchedulerWorkerRequests(workerId), values...).Err()
}

func (r *WorkerRedisRepository) UpdateWorkerStatus(workerId string, status types.WorkerStatus) error {
	return r.updateWorkerStatus(workerId, status, false)
}

func (r *WorkerRedisRepository) updateWorkerStatus(workerId string, status types.WorkerStatus, reconcileCapacity bool) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), schedulerWorkerLockOptions)
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))

	stateKey := common.RedisKeys.SchedulerWorkerState(workerId)
	worker, err := r.getWorkerFromKey(stateKey)
	if err != nil {
		return err
	}

	if reconcileCapacity && status == types.WorkerStatusAvailable {
		if err := r.reconcileWorkerCapacity(context.TODO(), worker); err != nil {
			return err
		}
	}

	ctx := context.TODO()
	pipe := r.rdb.TxPipeline()
	if reconcileCapacity {
		pipe.HSet(ctx, stateKey,
			"status", string(status),
			"free_cpu", worker.FreeCpu,
			"free_memory", worker.FreeMemory,
			"gpu_count", worker.FreeGpuCount,
		)
	} else {
		pipe.HSet(ctx, stateKey, "status", string(status))
	}
	pipe.HIncrBy(ctx, stateKey, "resource_version", 1)
	pipe.Expire(ctx, stateKey, time.Duration(types.WorkerStateTtlS)*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to update worker state <%v>: %w", stateKey, err)
	}

	return nil
}

type workerReservedCapacity struct {
	cpu    int64
	memory int64
	gpu    uint32
}

func (r *WorkerRedisRepository) reconcileWorkerCapacity(ctx context.Context, worker *types.Worker) error {
	if worker == nil {
		return nil
	}

	usage, err := r.getWorkerReservedCapacity(ctx, worker.Id)
	if err != nil {
		return err
	}

	if worker.TotalCpu > 0 {
		worker.FreeCpu = maxInt64(worker.TotalCpu-usage.cpu, 0)
	}
	if worker.TotalMemory > 0 {
		worker.FreeMemory = maxInt64(worker.TotalMemory-usage.memory, 0)
	}
	if worker.TotalGpuCount > 0 {
		if usage.gpu >= worker.TotalGpuCount {
			worker.FreeGpuCount = 0
		} else {
			worker.FreeGpuCount = worker.TotalGpuCount - usage.gpu
		}
	}

	return nil
}

func (r *WorkerRedisRepository) getWorkerReservedCapacity(ctx context.Context, workerId string) (workerReservedCapacity, error) {
	var usage workerReservedCapacity
	queuedContainerIDs := map[string]struct{}{}

	queuedRequests, err := r.rdb.LRange(ctx, common.RedisKeys.SchedulerWorkerRequests(workerId), 0, -1)
	if err != nil {
		return usage, fmt.Errorf("failed to list queued requests for worker <%s>: %w", workerId, err)
	}

	for _, rawRequest := range queuedRequests {
		var request types.ContainerRequest
		if err := json.Unmarshal([]byte(rawRequest), &request); err != nil {
			return usage, fmt.Errorf("failed to deserialize queued request for worker <%s>: %w", workerId, err)
		}
		if request.ContainerId != "" {
			queuedContainerIDs[request.ContainerId] = struct{}{}
		}
		usage.addRequest(&request)
	}

	containerStateKeys, err := r.rdb.SMembers(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(workerId)).Result()
	if err != nil {
		return usage, fmt.Errorf("failed to list active containers for worker <%s>: %w", workerId, err)
	}

	for _, key := range containerStateKeys {
		state, exists, err := r.getIndexedContainerState(ctx, workerId, key)
		if err != nil {
			return usage, err
		}
		if !exists || state.Status == types.ContainerStatusStopping {
			continue
		}
		containerID := state.ContainerId
		if containerID == "" {
			containerID = containerIDFromStateKey(key)
		}
		if _, queued := queuedContainerIDs[containerID]; queued {
			continue
		}

		usage.addContainerState(state)
	}

	return usage, nil
}

func containerIDFromStateKey(key string) string {
	prefix := common.RedisKeys.SchedulerContainerState("")
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}

func (r *WorkerRedisRepository) getIndexedContainerState(ctx context.Context, workerId string, key string) (*types.ContainerState, bool, error) {
	res, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, false, fmt.Errorf("failed to get indexed container state <%s> for worker <%s>: %w", key, workerId, err)
	}
	if len(res) == 0 {
		if err := r.rdb.SRem(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(workerId), key).Err(); err != nil {
			return nil, false, fmt.Errorf("failed to remove stale container index entry <%s> for worker <%s>: %w", key, workerId, err)
		}
		return nil, false, nil
	}

	state := &types.ContainerState{}
	if err := common.ToStruct(res, state); err != nil {
		return nil, false, fmt.Errorf("failed to deserialize indexed container state <%s> for worker <%s>: %w", key, workerId, err)
	}

	return state, true, nil
}

func (c *workerReservedCapacity) addRequest(request *types.ContainerRequest) {
	if request == nil {
		return
	}

	c.cpu += request.Cpu
	c.memory += capacityMemoryForRequest(request)
	c.gpu += gpuCountForCapacity(request.Gpu, request.GpuRequest, request.GpuCount)
}

func (c *workerReservedCapacity) addContainerState(state *types.ContainerState) {
	if state == nil {
		return
	}

	c.cpu += state.Cpu
	c.memory += capacityMemoryForRequest(&types.ContainerRequest{Memory: state.Memory})
	c.gpu += gpuCountForCapacity(state.Gpu, nil, state.GpuCount)
}

func gpuCountForCapacity(gpu string, gpuRequest []string, gpuCount uint32) uint32 {
	if gpu == "" && len(gpuRequest) == 0 {
		return 0
	}
	if gpuCount == 0 {
		return 1
	}
	return gpuCount
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (r *WorkerRedisRepository) SetWorkerKeepAlive(workerId string, keepAlive types.WorkerKeepAlive) error {
	ctx := context.TODO()
	err := r.lock.Acquire(ctx, common.RedisKeys.SchedulerWorkerLock(workerId), schedulerWorkerLockOptions)
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.SchedulerWorkerLock(workerId))

	stateKey := common.RedisKeys.SchedulerWorkerState(workerId)
	worker, err := r.getWorkerFromKey(stateKey)
	if err != nil {
		return err
	}

	oldMachineID := worker.MachineId
	machineID := strings.TrimSpace(keepAlive.MachineId)
	if machineID != "" {
		worker.MachineId = machineID
	}
	pipe := r.rdb.TxPipeline()
	if worker.MachineId != oldMachineID {
		pipe.HSet(ctx, stateKey, "machine_id", worker.MachineId)
		pipe.HIncrBy(ctx, stateKey, "resource_version", 1)
	}
	pipe.Expire(ctx, stateKey, time.Duration(types.WorkerStateTtlS)*time.Second)
	if oldMachineID != "" && oldMachineID != worker.MachineId {
		pipe.SRem(ctx, common.RedisKeys.SchedulerWorkerMachineIndex(oldMachineID), stateKey)
	}
	for _, indexKey := range r.workerIndexKeys(worker) {
		pipe.SAdd(ctx, indexKey, stateKey)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to set worker keepalive <%v>: %w", stateKey, err)
	}
	return nil
}

func (r *WorkerRedisRepository) ToggleWorkerAvailable(workerId string) error {
	return r.updateWorkerStatus(workerId, types.WorkerStatusAvailable, true)
}

func (r *WorkerRedisRepository) workerIndexKeys(worker *types.Worker) []string {
	keys := []string{common.RedisKeys.SchedulerWorkerIndex()}
	if worker == nil {
		return keys
	}
	if worker.PoolName != "" {
		keys = append(keys, common.RedisKeys.SchedulerWorkerPoolIndex(worker.PoolName))
	}
	if worker.MachineId != "" {
		keys = append(keys, common.RedisKeys.SchedulerWorkerMachineIndex(worker.MachineId))
	}
	return keys
}

func (r *WorkerRedisRepository) removeWorkerIndexEntries(ctx context.Context, stateKey string, worker *types.Worker) error {
	for _, indexKey := range r.workerIndexKeys(worker) {
		if err := r.rdb.SRem(ctx, indexKey, stateKey).Err(); err != nil {
			return fmt.Errorf("failed to remove worker state key from index <%v>: %w", indexKey, err)
		}
	}
	return nil
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
		workers, err := r.getWorkersFromKeys(keys, common.RedisKeys.SchedulerWorkerIndex())
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
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerWorkerLock(workerId), schedulerWorkerLockOptions)
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

func (r *WorkerRedisRepository) getWorkersFromKeys(keys []string, cleanupIndexKeys ...string) ([]*types.Worker, error) {
	if len(keys) == 0 {
		return []*types.Worker{}, nil
	}

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
			r.removeMissingWorkerFromIndexes(context.TODO(), keys[i], cleanupIndexKeys...)
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

func (r *WorkerRedisRepository) removeMissingWorkerFromIndexes(ctx context.Context, stateKey string, indexKeys ...string) {
	indexSet := map[string]struct{}{common.RedisKeys.SchedulerWorkerIndex(): {}}
	for _, indexKey := range indexKeys {
		if indexKey != "" {
			indexSet[indexKey] = struct{}{}
		}
	}
	for indexKey := range indexSet {
		_ = r.rdb.SRem(ctx, indexKey, stateKey).Err()
	}
}

func (r *WorkerRedisRepository) filterIndexedWorkers(ctx context.Context, workers []*types.Worker, indexKey string, keep func(*types.Worker) bool) []*types.Worker {
	filtered := make([]*types.Worker, 0, len(workers))
	for _, worker := range workers {
		if keep(worker) {
			filtered = append(filtered, worker)
			continue
		}
		_ = r.rdb.SRem(ctx, indexKey, common.RedisKeys.SchedulerWorkerState(worker.Id)).Err()
	}
	return filtered
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
	if len(res) == 0 {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerId}
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

func (r *WorkerRedisRepository) GetPreemptibleGpus() []string {
	preemptibleGpus := []string{}
	for _, pool := range r.config.Pools {
		if pool.GPUType == "" {
			continue
		}

		// FIXME: This should really use the commented out code below instead of checking the node selector
		// if pool.Preemptable {
		// 	preemptibleGpus = append(preemptibleGpus, pool.GPUType)
		// }
		for _, v := range pool.JobSpec.NodeSelector {
			if strings.Contains(v, "spot") {
				preemptibleGpus = append(preemptibleGpus, pool.GPUType)
			}
		}
	}

	return preemptibleGpus
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

	preemptibleGpus := r.GetPreemptibleGpus()
	for _, gpuType := range preemptibleGpus {
		gpuAvailability[gpuType] = true
	}

	return gpuAvailability, nil
}

func (r *WorkerRedisRepository) GetAllWorkers() ([]*types.Worker, error) {
	workers, err := r.getWorkers(false)
	if err != nil {
		return nil, err
	}

	return workers, nil
}

func (r *WorkerRedisRepository) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	indexKey := common.RedisKeys.SchedulerWorkerPoolIndex(poolName)
	keys, err := r.rdb.SMembers(context.TODO(), indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve worker pool index <%s>: %w", indexKey, err)
	}

	workers, err := r.getWorkersFromKeys(keys, indexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve worker state keys from pool index <%s>: %w", indexKey, err)
	}
	return r.filterIndexedWorkers(context.TODO(), workers, indexKey, func(worker *types.Worker) bool {
		return worker.PoolName == poolName
	}), nil
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
	indexKey := common.RedisKeys.SchedulerWorkerMachineIndex(machineId)
	keys, err := r.rdb.SMembers(context.TODO(), indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve worker machine index <%s>: %w", indexKey, err)
	}

	workers, err := r.getWorkersFromKeys(keys, indexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve worker state keys from machine index <%s>: %w", indexKey, err)
	}
	return r.filterIndexedWorkers(context.TODO(), workers, indexKey, func(worker *types.Worker) bool {
		return worker.MachineId == machineId
	}), nil
}

func capacityMemoryForRequest(request *types.ContainerRequest) int64 {
	if request.Memory <= 0 {
		return request.Memory
	}

	// Runtime cgroups use a 1.25x hard memory limit over the requested soft
	// memory. Capacity accounting must reserve that hard limit; otherwise a
	// worker can be packed past its pod limit before accounting says it is full.
	return (request.Memory*125 + 99) / 100
}

func (r *WorkerRedisRepository) UpdateWorkerCapacity(worker *types.Worker, request *types.ContainerRequest, capacityUpdateType types.CapacityUpdateType) error {
	if worker == nil || request == nil {
		return errors.New("worker and container request are required")
	}

	direction := int64(1)
	if capacityUpdateType == types.RemoveCapacity {
		direction = -1
	} else if capacityUpdateType != types.AddCapacity {
		return errors.New("invalid capacity update type")
	}
	result, err := r.adjustWorkerCapacity(
		worker.Id,
		direction*request.Cpu,
		direction*capacityMemoryForRequest(request),
		direction*int64(gpuCountForCapacity(request.Gpu, request.GpuRequest, request.GpuCount)),
		1,
		false,
	)
	if err != nil {
		return err
	}
	worker.FreeCpu = result.freeCPU
	worker.FreeMemory = result.freeMemory
	worker.FreeGpuCount = uint32(result.freeGPU)
	worker.ResourceVersion = result.resourceVersion
	worker.MachineId = result.machineID
	return nil
}

type workerCapacityResult struct {
	machineID       string
	freeCPU         int64
	freeMemory      int64
	freeGPU         int64
	resourceVersion int64
}

func (r *WorkerRedisRepository) adjustWorkerCapacity(workerID string, cpu, memory, gpu, version int64, requireAvailable bool) (workerCapacityResult, error) {
	requireAvailableArg := "0"
	if requireAvailable {
		requireAvailableArg = "1"
	}
	value, err := adjustWorkerCapacityScript.Run(
		context.TODO(),
		r.rdb,
		[]string{common.RedisKeys.SchedulerWorkerState(workerID)},
		cpu,
		memory,
		gpu,
		version,
		requireAvailableArg,
	).Result()
	if err != nil {
		return workerCapacityResult{}, err
	}
	items, ok := value.([]interface{})
	if !ok || len(items) == 0 {
		return workerCapacityResult{}, fmt.Errorf("unexpected worker capacity result: %T", value)
	}
	code, ok := items[0].(int64)
	if !ok {
		return workerCapacityResult{}, fmt.Errorf("unexpected worker capacity status: %T", items[0])
	}
	switch code {
	case -1:
		return workerCapacityResult{}, &types.ErrWorkerNotFound{WorkerId: workerID}
	case -2:
		status := "unknown"
		if len(items) > 1 {
			status, _ = items[1].(string)
		}
		return workerCapacityResult{}, fmt.Errorf("worker <%s> is not available: %s", workerID, status)
	case -3:
		return workerCapacityResult{}, errors.New("unable to schedule container, worker out of cpu, memory, or gpu")
	case 1:
		if len(items) != 6 {
			return workerCapacityResult{}, fmt.Errorf("unexpected worker capacity result length: %d", len(items))
		}
		machineID, _ := items[1].(string)
		freeCPU, cpuOK := items[2].(int64)
		freeMemory, memoryOK := items[3].(int64)
		freeGPU, gpuOK := items[4].(int64)
		resourceVersion, versionOK := items[5].(int64)
		if !cpuOK || !memoryOK || !gpuOK || !versionOK {
			return workerCapacityResult{}, errors.New("invalid worker capacity values")
		}
		return workerCapacityResult{
			machineID:       machineID,
			freeCPU:         freeCPU,
			freeMemory:      freeMemory,
			freeGPU:         freeGPU,
			resourceVersion: resourceVersion,
		}, nil
	default:
		return workerCapacityResult{}, fmt.Errorf("unknown worker capacity status: %d", code)
	}
}

func (r *WorkerRedisRepository) ScheduleContainerRequest(worker *types.Worker, request *types.ContainerRequest) error {
	return r.ScheduleContainerRequests(worker, []*types.ContainerRequest{request})
}

type queuedContainerRequest struct {
	request    *types.ContainerRequest
	payload    []byte
	stateKey   string
	assignment string
	push       *redis.IntCmd
}

func (r *WorkerRedisRepository) ScheduleContainerRequests(worker *types.Worker, requests []*types.ContainerRequest) error {
	if worker == nil {
		return errors.New("cannot schedule requests without a worker")
	}
	if len(requests) == 0 {
		return nil
	}

	var cpu, memory, gpu int64
	queued := make([]queuedContainerRequest, 0, len(requests))
	for _, request := range requests {
		if request == nil {
			return errors.New("cannot schedule a nil container request")
		}
		cpu += request.Cpu
		memory += capacityMemoryForRequest(request)
		gpu += int64(gpuCountForCapacity(request.Gpu, request.GpuRequest, request.GpuCount))
		queued = append(queued, queuedContainerRequest{
			request:    request,
			stateKey:   common.RedisKeys.SchedulerContainerState(request.ContainerId),
			assignment: uuid.NewString(),
		})
	}

	capacity, err := r.adjustWorkerCapacity(worker.Id, -cpu, -memory, -gpu, int64(len(requests)), true)
	if err != nil {
		return err
	}
	releaseCapacity := func() error {
		_, err := r.adjustWorkerCapacity(worker.Id, cpu, memory, gpu, 1, false)
		return err
	}

	for index := range queued {
		queuedRequest := *queued[index].request
		queuedRequest.Timestamp = time.Now()
		queuedRequest.MachineId = capacity.machineID
		payload, err := json.Marshal(&queuedRequest)
		if err != nil {
			return errors.Join(fmt.Errorf("failed to serialize request: %w", err), releaseCapacity())
		}
		queued[index].payload = payload
	}

	ctx := context.TODO()
	queueKey := common.RedisKeys.SchedulerWorkerRequests(worker.Id)
	workerContainerIndexKey := common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)
	pipe := r.rdb.TxPipeline()
	for index := range queued {
		item := &queued[index]
		item.push = pipe.RPush(ctx, queueKey, item.payload)
		pipe.HSet(ctx, item.stateKey, "worker_id", worker.Id, "machine_id", capacity.machineID, "gpu", item.request.Gpu, schedulerAssignmentIDField, item.assignment)
		pipe.SAdd(ctx, workerContainerIndexKey, item.stateKey)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		errs := []error{fmt.Errorf("failed to push requests: %w", err)}
		for _, item := range queued {
			err := r.cleanupScheduledContainerRequest(ctx, scheduleCleanup{
				queue:   queueKey,
				index:   workerContainerIndexKey,
				state:   item.stateKey,
				payload: item.payload,
				queued:  item.push.Err() == nil,
				token:   item.assignment,
				worker:  worker.Id,
			})
			if err != nil {
				errs = append(errs, err)
			}
		}
		if err := releaseCapacity(); err != nil {
			errs = append(errs, fmt.Errorf("failed to restore worker capacity after queue push error: %w", err))
		}
		return errors.Join(errs...)
	}

	worker.FreeCpu = capacity.freeCPU
	worker.FreeMemory = capacity.freeMemory
	worker.FreeGpuCount = uint32(capacity.freeGPU)
	worker.ResourceVersion = capacity.resourceVersion
	worker.MachineId = capacity.machineID
	for _, item := range queued {
		item.request.MachineId = capacity.machineID
	}
	metrics.RecordWorkerQueueDepth(worker.Id, r.rdb.LLen(ctx, common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())

	log.Info().
		Str("worker_id", worker.Id).
		Str("pool_name", worker.PoolName).
		Str("machine_id", worker.MachineId).
		Int("request_count", len(requests)).
		Msg("container requests added")

	return nil
}

type scheduleCleanup struct {
	queue, index, state string
	payload             []byte
	queued              bool
	token, worker       string
}

var cleanupScheduledContainerRequestScript = redis.NewScript(`
if ARGV[2] == "1" then
	redis.call("LREM", KEYS[1], -1, ARGV[1])
end

local token = redis.pcall("HGET", KEYS[3], "schedule_assignment_id")
local worker = redis.pcall("HGET", KEYS[3], "worker_id")
if type(token) == "table" or type(worker) == "table" then
	redis.call("SREM", KEYS[2], KEYS[3])
	return 1
end

if worker == ARGV[4] and token ~= ARGV[3] then
	return 1
end

redis.call("SREM", KEYS[2], KEYS[3])
if worker == ARGV[4] then
	redis.call("HDEL", KEYS[3], "worker_id", "machine_id", "gpu", "schedule_assignment_id")
end
return 1
`)

func (r *WorkerRedisRepository) cleanupScheduledContainerRequest(ctx context.Context, cleanup scheduleCleanup) error {
	removeQueuedArg := "0"
	if cleanup.queued {
		removeQueuedArg = "1"
	}
	if err := cleanupScheduledContainerRequestScript.Run(ctx, r.rdb, []string{cleanup.queue, cleanup.index, cleanup.state}, string(cleanup.payload), removeQueuedArg, cleanup.token, cleanup.worker).Err(); err != nil {
		return fmt.Errorf("failed to clean up scheduled container request: %w", err)
	}
	return nil
}

func (r *WorkerRedisRepository) AddContainerToWorker(workerId string, containerId string) error {
	containerStateKey := common.RedisKeys.SchedulerContainerState(containerId)

	ctx := context.TODO()
	pipe := r.rdb.TxPipeline()
	pipe.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(workerId), containerStateKey)
	pipe.HDel(ctx, containerStateKey, schedulerAssignmentIDField)
	if worker, err := r.getWorkerFromKey(common.RedisKeys.SchedulerWorkerState(workerId)); err == nil && worker != nil {
		pipe.HSet(ctx, containerStateKey, "worker_id", worker.Id, "machine_id", worker.MachineId)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to add container to worker container index: %w", err)
	}

	return nil
}

func (r *WorkerRedisRepository) RemoveContainerFromWorker(workerId string, containerId string) error {
	containerStateKey := common.RedisKeys.SchedulerContainerState(containerId)

	err := r.rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(workerId), containerStateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove container from worker container index: %w", err)
	}

	return nil
}

func (r *WorkerRedisRepository) GetNextContainerRequest(workerId string) (*types.ContainerRequest, error) {
	requests, err := r.GetNextContainerRequests(workerId, 1)
	if err != nil || len(requests) == 0 {
		return nil, err
	}
	return requests[0], nil
}

func (r *WorkerRedisRepository) GetNextContainerRequests(workerId string, limit int) ([]*types.ContainerRequest, error) {
	if limit <= 0 {
		return nil, nil
	}

	ctx := context.TODO()
	queueKey := common.RedisKeys.SchedulerWorkerRequests(workerId)
	encoded, err := r.rdb.LPopCount(ctx, queueKey, limit).Result()
	if err == redis.Nil {
		metrics.RecordWorkerQueueDepth(workerId, 0)
		metrics.RecordWorkerQueueEmptyPoll(workerId)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	requests := make([]*types.ContainerRequest, 0, len(encoded))
	var decodeErr error
	for _, raw := range encoded {
		request := &types.ContainerRequest{}
		if err := json.Unmarshal([]byte(raw), request); err != nil {
			decodeErr = errors.Join(decodeErr, err)
			continue
		}
		requests = append(requests, request)
	}
	if decodeErr != nil {
		return nil, errors.Join(decodeErr, r.RequeueContainerRequests(workerId, requests))
	}

	queueLength, err := r.rdb.LLen(ctx, queueKey).Result()
	if err == nil {
		metrics.RecordWorkerQueueDepth(workerId, queueLength)
	}
	for _, request := range requests {
		if !request.Timestamp.IsZero() {
			metrics.RecordWorkerQueueReceiveLatency(workerId, time.Since(request.Timestamp), request)
		}
	}
	return requests, nil
}

func (r *WorkerRedisRepository) RequeueContainerRequests(workerId string, requests []*types.ContainerRequest) error {
	values := make([]interface{}, 0, len(requests))
	for i := len(requests) - 1; i >= 0; i-- {
		if requests[i] == nil {
			return errors.New("cannot requeue a nil container request")
		}
		encoded, err := json.Marshal(requests[i])
		if err != nil {
			return err
		}
		values = append(values, encoded)
	}
	if len(values) == 0 {
		return nil
	}
	return r.rdb.LPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(workerId), values...).Err()
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
	err := r.lock.Acquire(context.TODO(), lockKey, common.RedisLockOptions{
		TtlS:          30,
		Retries:       600,
		RetryInterval: 50 * time.Millisecond,
	})
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

func (r *WorkerRedisRepository) GetContainerIpAssignments(networkPrefix string) ([]types.ContainerIpAssignment, error) {
	ctx := context.TODO()
	assignments := []types.ContainerIpAssignment{}

	ips, err := r.rdb.SMembers(ctx, common.RedisKeys.WorkerNetworkIpIndex(networkPrefix)).Result()
	if err != nil {
		return nil, err
	}

	if len(ips) > 0 {
		ownerKeys := make([]string, 0, len(ips))
		for _, ip := range ips {
			ownerKeys = append(ownerKeys, common.RedisKeys.WorkerNetworkIpOwner(networkPrefix, ip))
		}

		owners, err := r.rdb.MGet(ctx, ownerKeys...).Result()
		if err != nil {
			return nil, err
		}

		pipe := r.rdb.TxPipeline()
		cleanupStaleIndexes := false
		for i, value := range owners {
			containerId, ok := value.(string)
			if !ok || containerId == "" {
				pipe.SRem(ctx, common.RedisKeys.WorkerNetworkIpIndex(networkPrefix), ips[i])
				pipe.HDel(ctx, common.RedisKeys.WorkerNetworkIpRefCounts(networkPrefix), ips[i])
				cleanupStaleIndexes = true
				continue
			}

			assignments = append(assignments, types.ContainerIpAssignment{
				ContainerID: containerId,
				IPAddress:   ips[i],
			})
		}
		if cleanupStaleIndexes {
			if _, err := pipe.Exec(ctx); err != nil {
				return nil, err
			}
		}
	}

	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].ContainerID < assignments[j].ContainerID
	})
	return assignments, nil
}

func (r *WorkerRedisRepository) GetContainerIp(networkPrefix string, containerId string) (string, error) {
	containerIp, err := r.rdb.Get(context.TODO(), common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, containerId)).Result()
	if err != nil {
		return "", err
	}

	return containerIp, nil
}

func (r *WorkerRedisRepository) SetContainerIp(networkPrefix string, containerId, containerIp string) error {
	return setWorkerNetworkContainerIPScript.Run(
		context.TODO(),
		r.rdb,
		[]string{
			common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, containerId),
			common.RedisKeys.WorkerNetworkIpIndex(networkPrefix),
			common.RedisKeys.WorkerNetworkIpRefCounts(networkPrefix),
			common.RedisKeys.WorkerNetworkIpOwner(networkPrefix, containerIp),
		},
		containerIp,
		containerId,
		common.RedisKeys.WorkerNetworkIpOwnerPrefix(networkPrefix),
	).Err()
}

func (r *WorkerRedisRepository) MoveContainerIp(networkPrefix, fromContainerId, toContainerId, containerIp string) error {
	return moveWorkerNetworkContainerIPScript.Run(
		context.TODO(),
		r.rdb,
		[]string{
			common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, fromContainerId),
			common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, toContainerId),
			common.RedisKeys.WorkerNetworkIpIndex(networkPrefix),
			common.RedisKeys.WorkerNetworkIpRefCounts(networkPrefix),
			common.RedisKeys.WorkerNetworkIpOwner(networkPrefix, containerIp),
		},
		containerIp,
		fromContainerId,
		toContainerId,
	).Err()
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
	return removeWorkerNetworkContainerIPScript.Run(
		context.TODO(),
		r.rdb,
		[]string{
			common.RedisKeys.WorkerNetworkContainerIp(networkPrefix, containerId),
			common.RedisKeys.WorkerNetworkIpIndex(networkPrefix),
			common.RedisKeys.WorkerNetworkIpRefCounts(networkPrefix),
		},
		containerId,
		common.RedisKeys.WorkerNetworkIpOwnerPrefix(networkPrefix),
	).Err()
}
