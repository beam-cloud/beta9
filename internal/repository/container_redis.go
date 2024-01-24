package repository

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
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
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
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
	err = cr.rdb.HSet(context.TODO(), stateKey, "container_id", containerId, "status", string(info.Status), "scheduled_at", info.ScheduledAt).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state <%v>: %w", stateKey, err)
	}

	err = cr.rdb.Expire(context.TODO(), stateKey, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state ttl <%v>: %w", stateKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) SetContainerExitCode(containerId string, exitCode int) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)
	err = cr.rdb.SetEx(context.TODO(), exitCodeKey, exitCode, time.Duration(types.ContainerExitCodeTtlS)*time.Second).Err()
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

	hostKey := common.RedisKeys.SchedulerContainerHost(containerId)
	err = cr.rdb.Del(context.TODO(), hostKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container host <%v>: %w", hostKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) SetContainerAddress(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerHost(containerId), addr, 0).Err()
}

func (cr *ContainerRedisRepository) SetContainerWorkerHostname(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerWorkerContainerHost(containerId), addr, 0).Err()
}

func canConnectToHost(host string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (cr *ContainerRedisRepository) GetContainerWorkerHostname(containerId string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var hostname string = ""
	var err error

	ticker := time.NewTicker(1 * time.Second) // Retry every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", errors.New("timeout reached while trying to get worker hostname")
		case <-ticker.C:
			hostname, err = cr.rdb.Get(ctx, common.RedisKeys.SchedulerWorkerContainerHost(containerId)).Result()
			if err == nil && canConnectToHost(hostname, 1*time.Second) {
				return hostname, nil
			}
		}
	}
}

func (cr *ContainerRedisRepository) GetActiveContainersByPrefix(patternPrefix string) ([]types.ContainerState, error) {
	pattern := common.RedisKeys.SchedulerContainerState(patternPrefix)

	keys, err := cr.rdb.Scan(context.Background(), pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys for pattern <%v>: %v", pattern, err)
	}

	containerStates := make([]types.ContainerState, 0, len(keys))
	for _, key := range keys {
		res, err := cr.rdb.HGetAll(context.Background(), key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get container state for key %s: %w", key, err)
		}

		state := &types.ContainerState{}
		if err = common.ToStruct(res, state); err != nil {
			return nil, fmt.Errorf("failed to deserialize container state <%v>: %v", key, err)
		}

		containerStates = append(containerStates, *state)
	}

	return containerStates, nil
}

func (cr *ContainerRedisRepository) GetFailedContainerCountByPrefix(patternPrefix string) (int, error) {
	pattern := common.RedisKeys.SchedulerContainerExitCode(patternPrefix)

	// Retrieve keys with the specified pattern
	keys, err := cr.rdb.Scan(context.Background(), pattern)
	if err != nil {
		return -1, fmt.Errorf("failed to get keys with pattern <%v>: %w", pattern, err)
	}

	failedCount := 0
	for _, key := range keys {
		// Retrieve the value (exit code) for each key
		exitCode, err := cr.rdb.Get(context.Background(), key).Int()
		if err != nil {
			return -1, fmt.Errorf("failed to get value for key <%v>: %w", key, err)
		}

		// Check if the exit code is non-zero
		if exitCode != 0 {
			failedCount++
		}
	}

	return failedCount, nil
}
