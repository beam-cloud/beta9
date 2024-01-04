package taskqueue

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
)

type taskQueueState struct {
	RunningContainers  int
	PendingContainers  int
	StoppingContainers int
	FailedContainers   int
}

type taskQueueInstance struct {
	name               string
	stub               *types.Stub
	stubConfig         *types.StubConfigV1
	ctx                context.Context
	lock               *common.RedisLock
	scheduler          *scheduler.Scheduler
	containerEventChan chan types.ContainerEvent
	containers         map[string]bool
	scaleEventChan     chan int
	rdb                *common.RedisClient
}

func (i *taskQueueInstance) monitor() error {
	// go i.autoScaler.Start() // Start the autoscaler

	for {
		select {

		case <-i.ctx.Done():
			return nil

		case containerEvent := <-i.containerEventChan:
			initialContainerCount := len(i.containers)

			_, exists := i.containers[containerEvent.ContainerId]
			switch {
			case !exists && containerEvent.Change == 1: // Container created and doesn't exist in map
				i.containers[containerEvent.ContainerId] = true
			case exists && containerEvent.Change == -1: // Container removed and exists in map
				delete(i.containers, containerEvent.ContainerId)
			}

			if initialContainerCount != len(i.containers) {
				log.Printf("<%s> scaled from %d->%d", i.name, initialContainerCount, len(i.containers))
			}

		case desiredContainers := <-i.scaleEventChan:
			err := i.handleScalingEvent(i, desiredContainers)
			if err == nil {
				continue
			}
		}
	}
}

func (i *taskQueueInstance) state() (*taskQueueState, error) {
	containers, err := i.activeContainers()
	if err != nil {
		return nil, err
	}

	failedContainers, err := i.failedContainers()
	if err != nil {
		return nil, err
	}

	state := taskQueueState{}

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

func (i *taskQueueInstance) activeContainers() ([]types.ContainerState, error) {
	patternSuffix := fmt.Sprintf("%s%s-*", taskQueueContainerPrefix, i.stub.ExternalId)
	pattern := common.RedisKeys.SchedulerContainerState(patternSuffix)

	keys, err := i.rdb.Scan(i.ctx, pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys for pattern <%v>: %v", pattern, err)
	}

	containerStates := make([]types.ContainerState, 0, len(keys))
	for _, key := range keys {
		res, err := i.rdb.HGetAll(i.ctx, key).Result()
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

func (i *taskQueueInstance) failedContainers() (int, error) {
	patternSuffix := fmt.Sprintf("%s%s-*", taskQueueContainerPrefix, i.stub.ExternalId)
	pattern := common.RedisKeys.SchedulerContainerExitCode(patternSuffix)

	keys, err := i.rdb.Scan(i.ctx, pattern)
	if err != nil {
		return -1, fmt.Errorf("failed to get keys with pattern <%v>: %w", pattern, err)
	}

	return len(keys), nil
}

func (i *taskQueueInstance) handleScalingEvent(queue *taskQueueInstance, desiredContainers int) error {
	err := queue.lock.Acquire(queue.ctx, Keys.taskQueueInstanceLock(queue.name), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer i.lock.Release(Keys.taskQueueInstanceLock(queue.name))

	state, err := i.state()
	if err != nil {
		return err
	}

	if state.FailedContainers >= types.FailedContainerThreshold {
		log.Printf("<%s> Reached failed container threshold, scaling to zero.", queue.name)
		desiredContainers = 0
	}

	// if rb.Status == types.DeploymentStatusStopped {
	// 	desiredContainers = 0
	// }

	noContainersRunning := (state.PendingContainers == 0) && (state.RunningContainers == 0) && (state.StoppingContainers == 0)
	if desiredContainers == 0 && noContainersRunning {
		return nil //types.ErrBucketNotInUse
	}

	containerDelta := desiredContainers - (state.RunningContainers + state.PendingContainers)
	if containerDelta > 0 {
		err = i.startContainers(containerDelta)
	} else if containerDelta < 0 {
		err = i.stopContainers(-containerDelta)
	}

	return err
}

func (i *taskQueueInstance) startContainers(containersToRun int) error {
	for c := 0; c < containersToRun; c++ {
		runRequest := &types.ContainerRequest{}

		err := i.scheduler.Run(runRequest)
		if err != nil {
			return err
		}

		log.Printf("<%s> unable to run container: %v", i.name, err)
		continue
	}

	return nil
}

func (i *taskQueueInstance) stopContainers(containersToStop int) error {
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)

	containerIds := []string{}

	for c := 0; c < containersToStop && len(containerIds) > 0; c++ {
		idx := rnd.Intn(len(containerIds))
		containerId := containerIds[idx]

		err := i.scheduler.Stop(containerId)
		if err != nil {
			log.Printf("unable to stop container: %v", err)
			return err
		}

		// Remove the containerId from the containerIds slice to avoid
		// sending multiple stop requests to the same container
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)
	}

	return nil
}
