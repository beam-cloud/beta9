package endpoint

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"path"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type endpointState struct {
	RunningContainers  int
	PendingContainers  int
	StoppingContainers int
	FailedContainers   int
}

type ContainerDetails struct {
	id    string
	mutex *sync.Mutex
	ready bool

	Address            string
	ActiveRequestCount int
}

type endpointInstance struct {
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
	containers         map[string]*ContainerDetails
	scaleEventChan     chan int
	rdb                *common.RedisClient
	containerRepo      repository.ContainerRepository
	autoscaler         *autoscaler
	buffer             *RequestBuffer
}

func (i *endpointInstance) monitor() error {
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
				i.containers[containerEvent.ContainerId] = &ContainerDetails{
					id:    containerEvent.ContainerId,
					mutex: &sync.Mutex{},
					ready: false,
				}
			case exists && containerEvent.Change == -1: // Container removed and exists in map
				delete(i.containers, containerEvent.ContainerId)
			}

			if initialContainerCount != len(i.containers) {
				log.Printf("<endpoint %s> scaled from %d->%d", i.name, initialContainerCount, len(i.containers))
			}

		case desiredContainers := <-i.scaleEventChan:
			if err := i.handleScalingEvent(desiredContainers); err != nil {
				continue
			}
		}
	}
}

func (i *endpointInstance) state() (*endpointState, error) {
	patternPrefix := fmt.Sprintf("%s-%s-*", endpointContainerPrefix, i.stub.ExternalId)
	containers, err := i.containerRepo.GetActiveContainersByPrefix(patternPrefix)
	if err != nil {
		return nil, err
	}

	failedContainers, err := i.containerRepo.GetFailedContainerCountByPrefix(patternPrefix)
	if err != nil {
		return nil, err
	}

	state := endpointState{}
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

func (i *endpointInstance) handleScalingEvent(desiredContainers int) error {
	err := i.lock.Acquire(i.ctx, Keys.endpointInstanceLock(i.workspace.Name, i.stub.ExternalId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer i.lock.Release(Keys.endpointInstanceLock(i.workspace.Name, i.stub.ExternalId))

	state, err := i.state()
	if err != nil {
		return err
	}

	if state.FailedContainers >= types.FailedContainerThreshold {
		log.Printf("<endpoint %s> reached failed container threshold, scaling to zero.", i.name)
		desiredContainers = 0
	}

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

func (i *endpointInstance) startContainers(containersToRun int) error {
	for c := 0; c < containersToRun; c++ {
		runRequest := &types.ContainerRequest{
			ContainerId: i.genContainerId(),
			Env: []string{
				fmt.Sprintf("BETA9_TOKEN=%s", i.token.Key),
				fmt.Sprintf("HANDLER=%s", i.stubConfig.Handler),
				fmt.Sprintf("STUB_ID=%s", i.stub.ExternalId),
				fmt.Sprintf("CONCURRENCY=%d", i.stubConfig.Concurrency),
				fmt.Sprintf("KEEP_WARM_SECONDS=%d", i.stubConfig.KeepWarmSeconds),
			},
			Cpu:        i.stubConfig.Runtime.Cpu,
			Memory:     i.stubConfig.Runtime.Memory,
			Gpu:        string(i.stubConfig.Runtime.Gpu),
			ImageId:    i.stubConfig.Runtime.ImageId,
			EntryPoint: []string{i.stubConfig.PythonVersion, "-m", "beta9.runner.endpoint"}, // TODO: Configurable
			Mounts: []types.Mount{
				{
					LocalPath: path.Join(types.DefaultExtractedObjectPath, i.workspace.Name, i.object.ExternalId),
					MountPath: types.WorkerUserCodeVolume, ReadOnly: true,
				},
			},
		}

		err := i.scheduler.Run(runRequest)
		if err != nil {
			log.Printf("<endpoint %s> unable to run  container: %v", i.name, err)
			return err
		}

		continue
	}

	return nil
}

func (i *endpointInstance) stopContainers(containersToStop int) error {
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
			log.Printf("<endpoint %s> unable to stop container: %v", i.name, err)
			return err
		}

		// Remove the containerId from the containerIds slice to avoid
		// sending multiple stop requests to the same container
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)
	}

	return nil
}

func (i *endpointInstance) stoppableContainers() ([]string, error) {
	patternPrefix := fmt.Sprintf("%s-%s-*", endpointContainerPrefix, "*")
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
		keepWarmVal, err := i.rdb.Get(context.TODO(), Keys.endpointKeepWarmLock(i.workspace.Name, i.stub.ExternalId, container.ContainerId)).Int()
		if err != nil && err != redis.Nil {
			log.Printf("<endpoint %s> error getting keep warm lock for container: %v\n", i.name, err)
			continue
		}

		keepWarm := keepWarmVal > 0
		if keepWarm {
			continue
		}

		keys = append(keys, container.ContainerId)
	}

	return keys, nil
}

func (i *endpointInstance) genContainerId() string {
	return fmt.Sprintf("%s-%s-%s", endpointContainerPrefix, i.stub.ExternalId, uuid.New().String()[:8])
}
