package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/bsm/redislock"
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

func (cr *ContainerRedisRepository) SetContainerState(containerId string, info *types.ContainerState) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	err = cr.rdb.HSet(
		context.TODO(), stateKey,
		"container_id", containerId,
		"status", string(info.Status),
		"scheduled_at", info.ScheduledAt,
		"stub_id", info.StubId,
		"workspace_id", info.WorkspaceId,
		"gpu", info.Gpu,
		"gpu_count", info.GpuCount,
		"cpu", info.Cpu,
		"memory", info.Memory,
	).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state <%v>: %w", stateKey, err)
	}

	err = cr.rdb.Expire(context.TODO(), stateKey, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state ttl <%v>: %w", stateKey, err)
	}

	// Add container state key to index (by stub id)
	indexKey := common.RedisKeys.SchedulerContainerIndex(info.StubId)
	err = cr.rdb.SAdd(context.TODO(), indexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add container state key to index <%v>: %w", indexKey, err)
	}

	// Add container state key to index (by workspace id)
	indexKey = common.RedisKeys.SchedulerContainerWorkspaceIndex(info.WorkspaceId)
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

func (cr *ContainerRedisRepository) UpdateContainerStatus(containerId string, status types.ContainerStatus, expiry time.Duration) error {
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

func (cr *ContainerRedisRepository) DeleteContainerState(request *types.ContainerRequest) error {
	containerId := request.ContainerId

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

	return nil
}

func (cr *ContainerRedisRepository) SetContainerAddress(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerAddress(containerId), addr, 0).Err()
}

func (cr *ContainerRedisRepository) GetContainerAddress(containerId string) (string, error) {
	return cr.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerAddress(containerId)).Result()
}

func (cr *ContainerRedisRepository) SetWorkerAddress(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerWorkerAddress(containerId), addr, 0).Err()
}

func (cr *ContainerRedisRepository) GetWorkerAddress(containerId string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var hostname string = ""
	var err error

	ticker := time.NewTicker(1 * time.Second) // Retry every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", errors.New("timeout reached while trying to get worker addr")
		case <-ticker.C:
			hostname, err = cr.rdb.Get(ctx, common.RedisKeys.SchedulerWorkerAddress(containerId)).Result()
			if err == nil {
				return hostname, nil
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

func (cr *ContainerRedisRepository) GetFailedContainerCountByStubId(stubId string) (int, error) {
	indexKey := common.RedisKeys.SchedulerContainerIndex(stubId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()
	if err != nil {
		return -1, err
	}

	// Retrieve the value (exit code) for each key
	failedCount := 0
	for _, key := range keys {
		containerId := strings.Split(key, ":")[len(strings.Split(key, ":"))-1]
		exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)

		exitCode, err := cr.rdb.Get(context.Background(), exitCodeKey).Int()
		if err != nil && err != redis.Nil {
			return -1, fmt.Errorf("failed to get value for key <%v>: %w", key, err)
		} else if err == redis.Nil {
			continue
		}

		// Check if the exit code is non-zero
		if exitCode != 0 {
			failedCount++
		}
	}

	return failedCount, nil
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
			totalGpuCount += int(container.GpuCount)
			totalCpu += int(container.Cpu)
			totalMemory += container.Memory
		}

		if totalGpuCount+int(request.GpuCount) > int(quota.GPULimit) {
			return &types.ThrottledByConcurrencyLimitError{
				Reason: "gpu quota exceeded",
			}
		}

		if totalCpu+int(request.Cpu) > int(quota.CPUMillicoreLimit) {
			return &types.ThrottledByConcurrencyLimitError{
				Reason: "cpu quota exceeded",
			}
		}
	}

	err = c.SetContainerState(request.ContainerId, &types.ContainerState{
		ContainerId: request.ContainerId,
		StubId:      request.StubId,
		Status:      types.ContainerStatusPending,
		WorkspaceId: request.WorkspaceId,
		ScheduledAt: time.Now().Unix(),
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
