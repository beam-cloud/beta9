package taskqueue

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"path"
	"time"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type taskQueueState struct {
	RunningContainers  int
	PendingContainers  int
	StoppingContainers int
	FailedContainers   int
}

type taskQueueInstance struct {
	ctx                context.Context
	cancelFunc         context.CancelFunc
	name               string
	workspace          *types.Workspace
	stub               *types.Stub
	stubConfig         *types.StubConfigV1
	object             *types.Object
	token              *types.Token
	lock               *common.RedisLock
	scheduler          *scheduler.Scheduler
	containerEventChan chan types.ContainerEvent
	containers         map[string]bool
	scaleEventChan     chan int
	rdb                *common.RedisClient
	containerRepo      repository.ContainerRepository
	autoscaler         *autoscaler
	client             *taskQueueClient
}

func (i *taskQueueInstance) monitor() error {
	go i.autoscaler.start(i.ctx) // Start the autoscaler

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
				log.Printf("<taskqueue %s> scaled from %d->%d", i.name, initialContainerCount, len(i.containers))
			}

		case desiredContainers := <-i.scaleEventChan:
			if err := i.handleScalingEvent(desiredContainers); err != nil {
				continue
			}
		}
	}
}

func (i *taskQueueInstance) state() (*taskQueueState, error) {
	patternPrefix := fmt.Sprintf("%s%s-*", taskQueueContainerPrefix, i.stub.ExternalId)
	containers, err := i.containerRepo.GetActiveContainersByPrefix(patternPrefix)
	if err != nil {
		return nil, err
	}

	_, err = i.containerRepo.GetFailedContainerCountByPrefix(patternPrefix)
	if err != nil {
		return nil, err
	}
	failedContainers := 0

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

func (i *taskQueueInstance) handleScalingEvent(desiredContainers int) error {
	err := i.lock.Acquire(i.ctx, Keys.taskQueueInstanceLock(i.workspace.Name, i.stub.ExternalId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer i.lock.Release(Keys.taskQueueInstanceLock(i.workspace.Name, i.stub.ExternalId))

	state, err := i.state()
	if err != nil {
		return err
	}

	if state.FailedContainers >= types.FailedContainerThreshold {
		log.Printf("<taskqueue %s> reached failed container threshold, scaling to zero.", i.name)
		desiredContainers = 0
	}

	// TODO: put this back...?
	// Stubs currently have no concept of status so this is meaningless
	// if rb.Status == types.DeploymentStatusStopped {
	// 	desiredContainers = 0
	// }

	noContainersRunning := (state.PendingContainers == 0) && (state.RunningContainers == 0) && (state.StoppingContainers == 0)
	if desiredContainers == 0 && noContainersRunning {
		i.cancelFunc()
		return nil
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
		runRequest := &types.ContainerRequest{
			ContainerId: i.genContainerId(),
			Env: []string{
				fmt.Sprintf("HANDLER=%s", i.stubConfig.Handler),
				fmt.Sprintf("BEAM_TOKEN=%s", i.token.Key),
				fmt.Sprintf("STUB_ID=%s", i.stub.ExternalId),
				fmt.Sprintf("CONCURRENCY=%d", i.stubConfig.Concurrency),
				fmt.Sprintf("SCALE_DOWN_DELAY=%d", i.stubConfig.KeepWarmSeconds),
			},
			Cpu:        i.stubConfig.Runtime.Cpu,
			Memory:     i.stubConfig.Runtime.Memory,
			Gpu:        string(i.stubConfig.Runtime.Gpu),
			ImageId:    i.stubConfig.Runtime.ImageId,
			EntryPoint: []string{i.stubConfig.PythonVersion, "-m", "beam.runner.taskqueue"},
			Mounts: []types.Mount{
				{
					LocalPath: path.Join(types.DefaultExtractedObjectPath, i.workspace.Name, i.object.ExternalId),
					MountPath: types.WorkerUserCodeVolume, ReadOnly: true,
				},
			},
		}

		err := i.scheduler.Run(runRequest)
		if err != nil {
			log.Printf("<taskqueue %s> unable to run  container: %v", i.name, err)
			return err
		}

		continue
	}

	return nil
}

func (i *taskQueueInstance) stopContainers(containersToStop int) error {
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)

	containerIds, err := i.stoppableContainers()
	if err != nil {
		return err
	}

	for c := 0; c < containersToStop && len(containerIds) > 0; c++ {
		idx := rnd.Intn(len(containerIds))
		containerId := containerIds[idx]

		err := i.scheduler.Stop(containerId)
		if err != nil {
			log.Printf("<taskqueue %s> unable to stop container: %v", i.name, err)
			return err
		}

		// Remove the containerId from the containerIds slice to avoid
		// sending multiple stop requests to the same container
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)
	}

	return nil
}

func (i *taskQueueInstance) stoppableContainers() ([]string, error) {
	patternPrefix := fmt.Sprintf("%s%s-*", taskQueueContainerPrefix, i.stub.ExternalId)
	containers, err := i.containerRepo.GetActiveContainersByPrefix(patternPrefix)
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
		keepWarmVal, err := i.rdb.Get(context.TODO(), Keys.taskQueueKeepWarmLock(i.workspace.Name, i.stub.ExternalId, container.ContainerId)).Int()
		if err != nil && err != redis.Nil {
			log.Printf("<taskqueue %s> error getting keep warm lock for container: %v\n", i.name, err)
			continue
		}

		keepWarm := keepWarmVal > 0
		if keepWarm {
			continue
		}

		// Check if a queue processing lock exists for the container and skip if it does
		// This indicates the container is currently processing an item in the queue
		_, err = i.rdb.Get(context.TODO(), Keys.taskQueueProcessingLock(i.workspace.Name, i.stub.ExternalId, container.ContainerId)).Result()
		if err == nil || err != redis.Nil {
			continue
		}

		// If any tasks are currently running, skip this container
		tasksRunning, err := i.rdb.Keys(context.TODO(), Keys.taskQueueTaskRunningLock(i.workspace.Name, i.stub.ExternalId, container.ContainerId, "*"))
		if err != nil && err != redis.Nil {
			log.Printf("<taskqueue %s> error getting task running locks for container: %v\n", i.name, err)
			continue
		}

		if len(tasksRunning) > 0 {
			continue
		}

		keys = append(keys, container.ContainerId)
	}

	return keys, nil
}

func (i *taskQueueInstance) genContainerId() string {
	return fmt.Sprintf("%s%s-%s", taskQueueContainerPrefix, i.stub.ExternalId, uuid.New().String()[:8])
}
