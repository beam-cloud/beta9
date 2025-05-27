package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	redis "github.com/redis/go-redis/v9"
)

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
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
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

	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
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

	return nil
}

func (cr *ContainerRedisRepository) UpdateAssignedContainerGPU(containerId string, gpuType string) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
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
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
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
	// Acquire the concurrency limit lock for the workspace to prevent
	// simultaneous requests from exceeding the quota
	context := context.TODO()
	retryStrategy := redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), 10)
	lock, err := redislock.Obtain(context, c.rdb, common.RedisKeys.WorkspaceConcurrencyLimitLock(request.WorkspaceId), time.Duration(10)*time.Second, &redislock.Options{
		RetryStrategy: retryStrategy,
	})
	if err != nil && err != redislock.ErrNotObtained {
		return err
	}

	defer lock.Release(context)

	if quota != nil { // If a quota is set, check if the request exceeds it
		containers, err := c.GetActiveContainersByWorkspaceId(request.WorkspaceId)
		if err != nil {
			return err
		}

		totalGpuCount := 0
		totalCpu := 0
		totalMemory := int64(0)
		for _, container := range containers {
			if container.Status == types.ContainerStatusStopping {
				continue
			}

			totalGpuCount += int(container.GpuCount)
			totalCpu += int(container.Cpu)
			totalMemory += container.Memory
		}

		if totalGpuCount+int(request.GpuCount) > int(quota.GPULimit) {
			return &types.ThrottledByConcurrencyLimitError{
				Reason: "GPU quota exceeded. Please upgrade your plan for higher limit\n\nUpgrade here: https://platform.beam.cloud/settings/plans",
			}
		}

		if totalCpu+int(request.Cpu) > int(quota.CPUMillicoreLimit) {
			return &types.ThrottledByConcurrencyLimitError{
				Reason: "CPU quota exceeded. Please upgrade your plan for higher limit\n\nUpgrade here: https://platform.beam.cloud/settings/plans",
			}
		}
	}

	err = c.SetContainerState(request.ContainerId, &types.ContainerState{
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
		return err
	}

	return nil
}

func (cr *ContainerRedisRepository) UpdateCheckpointState(workspaceName, checkpointId string, checkpointState *types.CheckpointState) error {
	stateKey := common.RedisKeys.SchedulerCheckpointState(workspaceName, checkpointId)
	err := cr.rdb.HSet(
		context.TODO(), stateKey,
		"stub_id", checkpointState.StubId,
		"container_id", checkpointState.ContainerId,
		"status", string(checkpointState.Status),
		"remote_key", checkpointState.RemoteKey,
	).Err()
	if err != nil {
		return fmt.Errorf("failed to set checkpoint state <%v>: %w", stateKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) GetCheckpointState(workspaceName, checkpointId string) (*types.CheckpointState, error) {
	stateKey := common.RedisKeys.SchedulerCheckpointState(workspaceName, checkpointId)

	res, err := cr.rdb.HGetAll(context.TODO(), stateKey).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get container state: %w", err)
	}

	if len(res) == 0 {
		return nil, &types.ErrCheckpointNotFound{CheckpointId: checkpointId}
	}

	state := &types.CheckpointState{}
	if err = common.ToStruct(res, state); err != nil {
		return nil, fmt.Errorf("failed to deserialize checkpoint state <%v>: %v", stateKey, err)
	}

	return state, nil
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
