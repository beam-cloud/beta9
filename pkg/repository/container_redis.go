package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	redis "github.com/redis/go-redis/v9"
)

const (
	concurrencyUsageInitialized              = "1"
	concurrencyUsageInitializationTimeout    = 15 * time.Second
	concurrencyUsageInitializationPollPeriod = 100 * time.Millisecond
)

var reserveConcurrencyLimitScript = redis.NewScript(`
local used_gpu = tonumber(redis.call("HGET", KEYS[1], "gpu_count") or "0")
local used_cpu = tonumber(redis.call("HGET", KEYS[1], "cpu") or "0")
local gpu_limit = tonumber(ARGV[1])
local cpu_limit = tonumber(ARGV[2])
local request_gpu = tonumber(ARGV[3])
local request_cpu = tonumber(ARGV[4])

if redis.call("EXISTS", KEYS[2]) == 1 then
	return "ok"
end

if used_gpu + request_gpu > gpu_limit then
	return "gpu"
end

if used_cpu + request_cpu > cpu_limit then
	return "cpu"
end

redis.call("HINCRBY", KEYS[1], "gpu_count", request_gpu)
redis.call("HINCRBY", KEYS[1], "cpu", request_cpu)
redis.call("HSET", KEYS[1], "initialized", "1", "updated_at", ARGV[7])
redis.call("HSET", KEYS[2],
	"workspace_id", ARGV[5],
	"container_id", ARGV[6],
	"gpu_count", request_gpu,
	"cpu", request_cpu,
	"created_at", ARGV[7])
return "ok"
`)

var releaseConcurrencyLimitScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	return 0
end

local reserved_gpu = tonumber(redis.call("HGET", KEYS[2], "gpu_count") or "0")
local reserved_cpu = tonumber(redis.call("HGET", KEYS[2], "cpu") or "0")

local used_gpu = redis.call("HINCRBY", KEYS[1], "gpu_count", -reserved_gpu)
local used_cpu = redis.call("HINCRBY", KEYS[1], "cpu", -reserved_cpu)

if used_gpu < 0 then
	redis.call("HSET", KEYS[1], "gpu_count", 0)
end
if used_cpu < 0 then
	redis.call("HSET", KEYS[1], "cpu", 0)
end

redis.call("HSET", KEYS[1], "updated_at", ARGV[1])
redis.call("DEL", KEYS[2])
return 1
`)

type ContainerRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewContainerRedisRepository(r *common.RedisClient) ContainerRepository {
	lock := common.NewRedisLock(r)
	return &ContainerRedisRepository{rdb: r, lock: lock}
}

func (cr *ContainerRedisRepository) GetContainerState(containerId string) (*types.ContainerState, error) {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 2})
	if err != nil {
		return nil, err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)

	res, err := cr.rdb.HGetAll(context.TODO(), stateKey).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get container state: %w", err)
	}

	if len(res) == 0 {
		return nil, &types.ErrContainerStateNotFound{ContainerId: containerId}
	}

	state := &types.ContainerState{}
	if err = common.ToStruct(res, state); err != nil {
		return nil, fmt.Errorf("failed to deserialize container state <%v>: %v", stateKey, err)
	}

	return state, nil
}

func (cr *ContainerRedisRepository) SetContainerState(containerId string, state *types.ContainerState) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	err = cr.rdb.HSet(
		context.TODO(), stateKey,
		"container_id", containerId,
		"status", string(state.Status),
		"scheduled_at", state.ScheduledAt,
		"stub_id", state.StubId,
		"workspace_id", state.WorkspaceId,
		"gpu", state.Gpu,
		"gpu_count", state.GpuCount,
		"cpu", state.Cpu,
		"memory", state.Memory,
	).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state <%v>: %w", stateKey, err)
	}

	err = cr.rdb.Expire(context.TODO(), stateKey, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state ttl <%v>: %w", stateKey, err)
	}

	// Add container state key to index (by stub id)
	indexKey := common.RedisKeys.SchedulerContainerIndex(state.StubId)
	err = cr.rdb.SAdd(context.TODO(), indexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add container state key to index <%v>: %w", indexKey, err)
	}

	// Add container state key to index (by workspace id)
	indexKey = common.RedisKeys.SchedulerContainerWorkspaceIndex(state.WorkspaceId)
	err = cr.rdb.SAdd(context.TODO(), indexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add container state key to workspace index <%v>: %w", indexKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) SetContainerExitCode(containerId string, exitCode int) error {
	exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)
	err := cr.rdb.SetEx(context.TODO(), exitCodeKey, exitCode, time.Duration(types.ContainerExitCodeTtlS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set exit code <%v> for container <%v>: %w", exitCodeKey, containerId, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) GetContainerExitCode(containerId string) (int, error) {
	exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)
	exitCode, err := cr.rdb.Get(context.TODO(), exitCodeKey).Int()
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}

func (cr *ContainerRedisRepository) UpdateContainerStatus(containerId string, status types.ContainerStatus, expirySeconds int64) error {
	expiry := time.Duration(expirySeconds) * time.Second

	switch status {
	case types.ContainerStatusPending, types.ContainerStatusRunning, types.ContainerStatusStopping:
		// continue
	default:
		return fmt.Errorf("invalid status: %s", status)
	}

	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	// Get current state
	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	res, err := cr.rdb.HGetAll(context.TODO(), stateKey).Result()
	if err != nil {
		return err
	}

	// Convert response to struct
	state := &types.ContainerState{}
	err = common.ToStruct(res, state)
	if err != nil {
		return fmt.Errorf("failed to deserialize container state: %v", err)
	}

	// Update StartedAt if this is the first time we set container status to RUNNING
	if status == types.ContainerStatusRunning && state.Status != types.ContainerStatusRunning {
		state.StartedAt = time.Now().Unix()
	}

	// Update status
	state.Status = status

	// Save state to database
	err = cr.rdb.HSet(context.TODO(), stateKey, common.ToSlice(state)).Err()
	if err != nil {
		return fmt.Errorf("failed to update container state status <%v>: %w", stateKey, err)
	}

	// Set ttl on state
	err = cr.rdb.Expire(context.TODO(), stateKey, expiry).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state ttl <%v>: %w", stateKey, err)
	}

	if status == types.ContainerStatusStopping {
		// The release script is idempotent. Run it on every STOPPING update so
		// callers can retry if a previous release failed after status persisted.
		if err := cr.releaseContainerConcurrencyReservation(context.TODO(), state.WorkspaceId, containerId); err != nil {
			return err
		}
	}

	return nil
}

func (cr *ContainerRedisRepository) UpdateAssignedContainerGPU(containerId string, gpuType string) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	// Get current state
	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	res, err := cr.rdb.HGetAll(context.TODO(), stateKey).Result()
	if err != nil {
		return err
	}

	// Convert response to struct
	state := &types.ContainerState{}
	err = common.ToStruct(res, state)
	if err != nil {
		return fmt.Errorf("failed to deserialize container state: %v", err)
	}

	// Update GPU
	state.Gpu = gpuType

	// Save state to database
	err = cr.rdb.HSet(context.TODO(), stateKey, common.ToSlice(state)).Err()
	if err != nil {
		return fmt.Errorf("failed to update container state gpu <%v>: %w", stateKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) DeleteContainerState(containerId string) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	workspaceId, err := cr.rdb.HGet(context.TODO(), stateKey, "workspace_id").Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get container workspace <%v>: %w", stateKey, err)
	}

	if workspaceId != "" {
		if err := cr.releaseContainerConcurrencyReservation(context.TODO(), workspaceId, containerId); err != nil {
			return err
		}
	}

	err = cr.rdb.Del(context.TODO(), stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container state <%v>: %w", stateKey, err)
	}

	addrKey := common.RedisKeys.SchedulerContainerAddress(containerId)
	err = cr.rdb.Del(context.TODO(), addrKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container addr <%v>: %w", addrKey, err)
	}

	addrMapKey := common.RedisKeys.SchedulerContainerAddressMap(containerId)
	err = cr.rdb.Del(context.TODO(), addrMapKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container addrMap <%v>: %w", addrMapKey, err)
	}

	workerAddrKey := common.RedisKeys.SchedulerWorkerAddress(containerId)
	err = cr.rdb.Del(context.TODO(), workerAddrKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete worker addr <%v>: %w", workerAddrKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) SetContainerAddress(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerAddress(containerId), addr, 0).Err()
}

func (cr *ContainerRedisRepository) GetContainerAddress(containerId string) (string, error) {
	return cr.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerAddress(containerId)).Result()
}

func (cr *ContainerRedisRepository) SetContainerAddressMap(containerId string, addressMap map[int32]string) error {
	data, err := json.Marshal(addressMap)
	if err != nil {
		return fmt.Errorf("failed to marshal addressMap for container %s: %w", containerId, err)
	}

	err = cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerAddressMap(containerId), data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set container addressMap for container %s: %w", containerId, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) GetContainerAddressMap(containerId string) (map[int32]string, error) {
	data, err := cr.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerAddressMap(containerId)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get container addressMap for container %s: %w", containerId, err)
	}

	addressMap := make(map[int32]string)
	if err := json.Unmarshal(data, &addressMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal addressMap for container %s: %w", containerId, err)
	}

	return addressMap, nil
}

func (cr *ContainerRedisRepository) SetWorkerAddress(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerWorkerAddress(containerId), addr, 0).Err()
}

func (cr *ContainerRedisRepository) GetWorkerAddress(ctx context.Context, containerId string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	var hostname string = ""
	var err error

	ticker := time.NewTicker(1 * time.Second) // Retry every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return "", fmt.Errorf("failed to schedule container, container id: %s", containerId)
			}
			return "", fmt.Errorf("context cancelled while trying to get worker addr, container id: %s", containerId)
		case <-ticker.C:
			hostname, err = cr.rdb.Get(ctx, common.RedisKeys.SchedulerWorkerAddress(containerId)).Result()
			if err == nil {
				return hostname, nil
			}

			if requestStatus, err := cr.GetContainerRequestStatus(containerId); err == nil && requestStatus == types.ContainerRequestStatusFailed {
				return "", fmt.Errorf("failed to schedule container, container id: %s", containerId)
			}
		}
	}
}

func (cr *ContainerRedisRepository) listContainerStateByIndex(indexKey string, keys []string) ([]types.ContainerState, error) {
	containerStates := make([]types.ContainerState, 0)

	for _, key := range keys {
		exists, err := cr.rdb.Exists(context.TODO(), key).Result()
		if err != nil {
			continue
		}
		if exists == 0 {
			containerId := strings.Split(key, ":")[len(strings.Split(key, ":"))-1]
			exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)

			exitCodeKeyExists, err := cr.rdb.Exists(context.TODO(), exitCodeKey).Result()
			if err != nil {
				continue
			}

			if exitCodeKeyExists > 0 {
				continue
			}

			// We don't have an exit code, or a state key, remove key from set
			cr.rdb.SRem(context.TODO(), indexKey, key)
			continue
		}

		res, err := cr.rdb.HGetAll(context.TODO(), key).Result()
		if err != nil {
			continue
		}

		var state types.ContainerState
		if err = common.ToStruct(res, &state); err != nil {
			continue
		}

		if state.ContainerId == "" {
			continue
		}

		containerStates = append(containerStates, state)
	}

	return containerStates, nil
}

func (cr *ContainerRedisRepository) GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error) {
	indexKey := common.RedisKeys.SchedulerContainerIndex(stubId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	return cr.listContainerStateByIndex(indexKey, keys)
}

func (cr *ContainerRedisRepository) GetActiveContainersByWorkspaceId(workspaceId string) ([]types.ContainerState, error) {
	indexKey := common.RedisKeys.SchedulerContainerWorkspaceIndex(workspaceId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	return cr.listContainerStateByIndex(indexKey, keys)
}

func (cr *ContainerRedisRepository) GetActiveContainersByWorkerId(workerId string) ([]types.ContainerState, error) {
	indexKey := common.RedisKeys.SchedulerContainerWorkerIndex(workerId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	return cr.listContainerStateByIndex(indexKey, keys)
}

func (cr *ContainerRedisRepository) GetFailedContainersByStubId(stubId string) ([]string, error) {
	indexKey := common.RedisKeys.SchedulerContainerIndex(stubId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	// Retrieve the value (exit code) for each key
	failedContainerIds := make([]string, 0)
	for _, key := range keys {
		containerId := strings.Split(key, ":")[len(strings.Split(key, ":"))-1]
		exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)

		exitCode, err := cr.rdb.Get(context.Background(), exitCodeKey).Int()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to get value for key <%v>: %w", key, err)
		} else if err == redis.Nil {
			continue
		}

		// Check if the exit code is non-zero
		if types.ContainerExitCode(exitCode).IsFailed() {
			failedContainerIds = append(failedContainerIds, containerId)
		}
	}

	return failedContainerIds, nil
}

func (c *ContainerRedisRepository) SetContainerStateWithConcurrencyLimit(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error {
	reservedConcurrency := quota != nil
	if reservedConcurrency {
		if err := c.ensureWorkspaceConcurrencyUsageInitialized(request.WorkspaceId); err != nil {
			return err
		}

		if err := c.reserveContainerConcurrency(quota, request); err != nil {
			return err
		}
	}

	err := c.SetContainerState(request.ContainerId, &types.ContainerState{
		ContainerId: request.ContainerId,
		StubId:      request.StubId,
		Status:      types.ContainerStatusPending,
		WorkspaceId: request.WorkspaceId,
		ScheduledAt: time.Now().Unix(),
		StartedAt:   0,
		Gpu:         request.Gpu,
		GpuCount:    request.GpuCount,
		Cpu:         request.Cpu,
		Memory:      request.Memory,
	})
	if err != nil {
		if reservedConcurrency {
			if releaseErr := c.releaseContainerConcurrencyReservation(context.TODO(), request.WorkspaceId, request.ContainerId); releaseErr != nil {
				return errors.Join(err, fmt.Errorf("failed to release concurrency reservation after container state error: %w", releaseErr))
			}
		}
		return err
	}

	return nil
}

func (c *ContainerRedisRepository) ensureWorkspaceConcurrencyUsageInitialized(workspaceId string) error {
	ctx := context.TODO()
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceId)

	initialized, err := c.rdb.HGet(ctx, usageKey, "initialized").Result()
	if err == nil && initialized == concurrencyUsageInitialized {
		return nil
	}
	if err != nil && err != redis.Nil {
		return err
	}

	lock, err := redislock.Obtain(ctx, c.rdb, common.RedisKeys.WorkspaceConcurrencyLimitLock(workspaceId), concurrencyUsageInitializationTimeout, nil)
	if err != nil && err != redislock.ErrNotObtained {
		return err
	}
	if err == redislock.ErrNotObtained {
		return c.waitForWorkspaceConcurrencyUsageInitialized(ctx, usageKey, workspaceId)
	}
	defer lock.Release(ctx)

	initialized, err = c.rdb.HGet(ctx, usageKey, "initialized").Result()
	if err == nil && initialized == concurrencyUsageInitialized {
		return nil
	}
	if err != nil && err != redis.Nil {
		return err
	}

	return c.rebuildWorkspaceConcurrencyUsage(ctx, workspaceId)
}

func (c *ContainerRedisRepository) waitForWorkspaceConcurrencyUsageInitialized(ctx context.Context, usageKey, workspaceId string) error {
	deadline := time.After(concurrencyUsageInitializationTimeout)
	ticker := time.NewTicker(concurrencyUsageInitializationPollPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("concurrency limit usage initialization timed out for workspace %s", workspaceId)
		case <-ticker.C:
			initialized, err := c.rdb.HGet(ctx, usageKey, "initialized").Result()
			if err == nil && initialized == concurrencyUsageInitialized {
				return nil
			}
			if err != nil && err != redis.Nil {
				return err
			}
		}
	}
}

func (c *ContainerRedisRepository) rebuildWorkspaceConcurrencyUsage(ctx context.Context, workspaceId string) error {
	containers, err := c.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return err
	}

	totalGpuCount := int64(0)
	totalCpu := int64(0)
	now := time.Now().Unix()

	pipe := c.rdb.TxPipeline()
	for _, container := range containers {
		if container.Status == types.ContainerStatusStopping {
			continue
		}

		totalGpuCount += int64(container.GpuCount)
		totalCpu += container.Cpu
		pipe.HSet(ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceId, container.ContainerId),
			"workspace_id", workspaceId,
			"container_id", container.ContainerId,
			"gpu_count", container.GpuCount,
			"cpu", container.Cpu,
			"created_at", now,
		)
	}

	pipe.HSet(ctx, common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceId),
		"gpu_count", totalGpuCount,
		"cpu", totalCpu,
		"initialized", concurrencyUsageInitialized,
		"updated_at", now,
	)

	_, err = pipe.Exec(ctx)
	return err
}

func (c *ContainerRedisRepository) reserveContainerConcurrency(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error {
	ctx := context.TODO()
	result, err := reserveConcurrencyLimitScript.Run(ctx, c.rdb, []string{
		common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId),
		common.RedisKeys.WorkspaceConcurrencyLimitReservation(request.WorkspaceId, request.ContainerId),
	},
		int64(quota.GPULimit),
		int64(quota.CPUMillicoreLimit),
		int64(request.GpuCount),
		request.Cpu,
		request.WorkspaceId,
		request.ContainerId,
		time.Now().Unix(),
	).Text()
	if err != nil {
		return err
	}

	switch result {
	case "ok":
		return nil
	case "gpu":
		metrics.RecordConcurrencyLimitThrottle("gpu", request)
		return &types.ThrottledByConcurrencyLimitError{Reason: "gpu quota exceeded"}
	case "cpu":
		metrics.RecordConcurrencyLimitThrottle("cpu", request)
		return &types.ThrottledByConcurrencyLimitError{Reason: "cpu quota exceeded"}
	default:
		return fmt.Errorf("unexpected concurrency reservation result: %s", result)
	}
}

func (c *ContainerRedisRepository) releaseContainerConcurrencyReservation(ctx context.Context, workspaceId, containerId string) error {
	if workspaceId == "" || containerId == "" {
		return nil
	}

	_, err := releaseConcurrencyLimitScript.Run(ctx, c.rdb, []string{
		common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceId),
		common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceId, containerId),
	}, time.Now().Unix()).Result()
	return err
}

func (cr *ContainerRedisRepository) GetStubState(stubId string) (string, error) {
	stateKey := common.RedisKeys.SchedulerStubState(stubId)
	state, err := cr.rdb.Get(context.TODO(), stateKey).Result()
	if err != nil {
		if err == redis.Nil {
			return types.StubStateHealthy, nil
		}
		return "", err
	}

	return state, nil
}

var unhealthyStateTTL = 10 * time.Minute

func (cr *ContainerRedisRepository) SetStubState(stubId, state string) error {
	stateKey := common.RedisKeys.SchedulerStubState(stubId)
	return cr.rdb.SetEx(context.TODO(), stateKey, state, unhealthyStateTTL).Err()
}

func (cr *ContainerRedisRepository) DeleteStubState(stubId string) error {
	stateKey := common.RedisKeys.SchedulerStubState(stubId)
	return cr.rdb.Del(context.TODO(), stateKey).Err()
}

func (cr *ContainerRedisRepository) SetContainerRequestStatus(containerId string, status types.ContainerRequestStatus) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerRequestStatus(containerId), status, types.ContainerRequestStatusTTL).Err()
}

func (cr *ContainerRedisRepository) GetContainerRequestStatus(containerId string) (types.ContainerRequestStatus, error) {
	status, err := cr.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerRequestStatus(containerId)).Result()
	if err != nil {
		return "", err
	}

	return types.ContainerRequestStatus(status), nil
}

func (cr *ContainerRedisRepository) SetBuildContainerTTL(containerId string, ttl time.Duration) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.ImageBuildContainerTTL(containerId), "1", ttl).Err()
}

func (cr *ContainerRedisRepository) HasBuildContainerTTL(containerId string) bool {
	return cr.rdb.Exists(context.TODO(), common.RedisKeys.ImageBuildContainerTTL(containerId)).Val() != 0
}
