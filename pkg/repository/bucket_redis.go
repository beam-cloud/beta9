package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/types"
	redis "github.com/redis/go-redis/v9"
)

type RequestBucketRedisRepository struct {
	name       string
	identityId string
	rdb        *common.RedisClient
	lock       *common.RedisLock
	bucketType types.RequestBucketType
}

func NewRequestBucketRedisRepository(name string, identityId string, rdb *common.RedisClient, bucketType types.RequestBucketType) RequestBucketRepository {
	lock := common.NewRedisLock(rdb)
	return &RequestBucketRedisRepository{name: name, identityId: identityId, rdb: rdb, lock: lock, bucketType: bucketType}
}

// GetRequestBucketState retrieves the state of the RequestBucket, including the counts of running, pending, stopping, and failed containers.
// It internally calls GetCurrentActiveContainers to fetch the current container information associated with the RequestBucket.
// Returns a pointer to a RequestBucketState struct that contains the container state counts.
func (rb *RequestBucketRedisRepository) GetRequestBucketState() (*types.RequestBucketState, error) {
	containers, err := rb.GetCurrentActiveContainers()
	if err != nil {
		return nil, err
	}

	failedContainers, err := rb.GetFailedContainers()
	if err != nil {
		return nil, err
	}

	state := types.RequestBucketState{}

	for _, container := range containers {
		switch container.Status {
		case types.ContainerStatusRunning:
			state.RunningContainers++
		case types.ContainerStatusPending:
			state.PendingContainers++
		case types.ContainerStatusStopping:
			state.StoppingContainers++
		}
	}

	state.FailedContainers = failedContainers

	return &state, nil
}

func (rb *RequestBucketRedisRepository) GetCurrentActiveContainers() ([]types.ContainerState, error) {
	patternSuffix := ""

	switch rb.bucketType {
	case types.RequestBucketTypeDeployment:
		patternSuffix = fmt.Sprintf("%s%s-*", types.DeploymentContainerPrefix, rb.name)
	case types.RequestBucketTypeServe:
		patternSuffix = fmt.Sprintf("%s%s", types.ServeContainerPrefix, rb.name)
	default:
		return nil, errors.New("invalid request bucket type")
	}

	pattern := common.RedisKeys.SchedulerContainerState(patternSuffix)

	keys, err := rb.rdb.Scan(context.TODO(), pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys for pattern <%v>: %v", pattern, err)
	}

	containerStates := make([]types.ContainerState, 0, len(keys))
	for _, key := range keys {
		res, err := rb.rdb.HGetAll(context.TODO(), key).Result()
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

func (rb *RequestBucketRedisRepository) GetStoppableContainers(deploymentStatus string) ([]string, error) {
	containers, err := rb.GetCurrentActiveContainers()
	if err != nil {
		return nil, err
	}

	// Create a slice to hold the keys
	keys := make([]string, 0, len(containers))
	for _, container := range containers {
		if container.Status == types.ContainerStatusStopping || container.Status == types.ContainerStatusPending {
			continue
		}

		// Skip containers with keep warm locks
		keepWarmVal, err := rb.rdb.Get(context.TODO(), common.RedisKeys.QueueKeepWarmLock(rb.identityId, rb.name, container.ContainerId)).Int()
		if err != nil && err != redis.Nil {
			log.Printf("<%s> Error getting keep warm lock for container: %v\n", rb.name, err)
			continue
		}

		keepWarm := keepWarmVal > 0
		if keepWarm && deploymentStatus != types.DeploymentStatusStopped {
			continue
		}

		// Check if a queue processing lock exists for the container and skip if it does
		// This indicates the container is currently processing an item in the queue
		_, err = rb.rdb.Get(context.TODO(), common.RedisKeys.QueueProcessingLock(rb.identityId, rb.name, container.ContainerId)).Result()
		if err == nil || err != redis.Nil {
			continue
		}

		// If any tasks are currently running, skip this container
		tasksRunning, err := rb.rdb.Keys(context.TODO(), common.RedisKeys.QueueTaskRunningLock(rb.identityId, rb.name, container.ContainerId, "*"))
		if err != nil && err != redis.Nil {
			log.Printf("<%s> Error getting task running locks for container: %v\n", rb.name, err)
			continue
		}

		if len(tasksRunning) > 0 {
			continue
		}

		keys = append(keys, container.ContainerId)
	}

	return keys, nil
}

func (rb *RequestBucketRedisRepository) GetFailedContainers() (int, error) {
	patternSuffix := fmt.Sprintf("%s%s-*", types.DeploymentContainerPrefix, rb.name)
	pattern := common.RedisKeys.SchedulerContainerExitCode(patternSuffix)

	keys, err := rb.rdb.Scan(context.TODO(), pattern)
	if err != nil {
		return -1, fmt.Errorf("failed to get keys with pattern <%v>: %w", pattern, err)
	}

	return len(keys), nil
}

func (rb *RequestBucketRedisRepository) WaitForTaskCompletion(taskId string) (string, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), types.RequestTimeoutDurationS)
	defer cancel()

	messages, errs := rb.rdb.Subscribe(ctx, common.RedisKeys.QueueTaskCompleteEvent(rb.identityId, rb.name, taskId))

	// Wait for task completion event, and then retrieve the response
	for {
		select {
		case m := <-messages:
			containerHostname := m.Payload
			if containerHostname != "" {
				return containerHostname, nil
			}

		case <-ctx.Done():
			return "", fmt.Errorf("task timed out: %v", ctx.Err())

		case err := <-errs:
			return "", fmt.Errorf("error waiting for task completion subscription: %v", err)
		}
	}
}

func canConnectToHost(host string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (rb *RequestBucketRedisRepository) SetKeepWarm(containerId string, scaleDownDelay float32) error {
	err := rb.rdb.SetEx(context.TODO(), common.RedisKeys.QueueKeepWarmLock(rb.identityId, rb.name, containerId), 1, time.Duration(scaleDownDelay)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}

func (rb *RequestBucketRedisRepository) findAvailableHosts() ([]*types.AvailableHost, error) {
	containers, err := rb.GetCurrentActiveContainers()
	if err != nil {
		return nil, err
	}

	hosts := make([]*types.AvailableHost, 0, len(containers))
	for _, container := range containers {
		if container.Status == types.ContainerStatusStopping || container.Status == types.ContainerStatusPending {
			continue
		}

		hostname, err := rb.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerHost(container.ContainerId)).Result()
		if err != nil {
			continue
		}

		if canConnectToHost(hostname, 1*time.Second) {
			hosts = append(hosts, &types.AvailableHost{
				Hostname:    hostname,
				ContainerId: container.ContainerId,
			})
		}
	}

	return hosts, nil
}

func (rb *RequestBucketRedisRepository) GetAvailableContainerHost() (*types.AvailableHost, error) {
	hosts, err := rb.findAvailableHosts()
	if err != nil {
		return nil, err
	}

	if len(hosts) > 0 {
		randIndex := rand.Intn(len(hosts))
		return hosts[randIndex], nil
	}

	// Poll for a host until the timeout
	timeout := time.After(types.RequestTimeoutDurationS)
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-timeout:
			return nil, errors.New("timeout reached while waiting for an available container")
		case <-ticker.C:
			hostnames, err := rb.findAvailableHosts()
			if err != nil {
				return nil, err
			}

			if len(hostnames) > 0 {
				randIndex := rand.Intn(len(hostnames))
				return hostnames[randIndex], nil
			}
		}
	}
}
