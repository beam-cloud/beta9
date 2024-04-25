package taskqueue

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"path"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func withAutoscaler(constructor func(i *taskQueueInstance) *abstractions.Autoscaler[*taskQueueInstance, *taskQueueAutoscalerSample]) func(*taskQueueInstance) {
	return func(i *taskQueueInstance) {
		i.Autoscaler = constructor(i)
	}
}

func withEntryPoint(entryPoint func(instance *taskQueueInstance) []string) func(*taskQueueInstance) {
	return func(i *taskQueueInstance) {
		i.EntryPoint = entryPoint(i)
	}
}

type taskQueueInstance struct {
	*abstractions.AutoscaledInstance
	client *taskQueueClient
}

func (i *taskQueueInstance) startContainers(containersToRun int) error {
	for c := 0; c < containersToRun; c++ {
		runRequest := &types.ContainerRequest{
			ContainerId: i.genContainerId(),
			Env: []string{
				fmt.Sprintf("BETA9_TOKEN=%s", i.Token.Key),
				fmt.Sprintf("HANDLER=%s", i.StubConfig.Handler),
				fmt.Sprintf("ON_START=%s", i.StubConfig.OnStart),
				fmt.Sprintf("STUB_ID=%s", i.Stub.ExternalId),
				fmt.Sprintf("STUB_TYPE=%s", i.Stub.Type),
				fmt.Sprintf("CONCURRENCY=%d", i.StubConfig.Concurrency),
				fmt.Sprintf("KEEP_WARM_SECONDS=%d", i.StubConfig.KeepWarmSeconds),
				fmt.Sprintf("PYTHON_VERSION=%s", i.StubConfig.PythonVersion),
			},
			Cpu:         i.StubConfig.Runtime.Cpu,
			Memory:      i.StubConfig.Runtime.Memory,
			Gpu:         string(i.StubConfig.Runtime.Gpu),
			ImageId:     i.StubConfig.Runtime.ImageId,
			StubId:      i.Stub.ExternalId,
			WorkspaceId: i.Workspace.ExternalId,
			EntryPoint:  i.EntryPoint,
			Mounts: []types.Mount{
				{
					LocalPath: path.Join(types.DefaultExtractedObjectPath, i.Workspace.Name, i.Object.ExternalId),
					MountPath: types.WorkerUserCodeVolume, ReadOnly: true,
				},
			},
		}

		err := i.Scheduler.Run(runRequest)
		if err != nil {
			log.Printf("<%s> unable to run container: %v", i.Name, err)
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

		err := i.Scheduler.Stop(containerId)
		if err != nil {
			log.Printf("<%s> unable to stop container: %v", i.Name, err)
			return err
		}

		// Remove the containerId from the containerIds slice to avoid
		// sending multiple stop requests to the same container
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)
	}

	return nil
}

func (i *taskQueueInstance) stoppableContainers() ([]string, error) {
	containers, err := i.ContainerRepo.GetActiveContainersByStubId(i.Stub.ExternalId)
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
		keepWarmVal, err := i.Rdb.Get(context.TODO(), Keys.taskQueueKeepWarmLock(i.Workspace.Name, i.Stub.ExternalId, container.ContainerId)).Int()
		if err != nil && err != redis.Nil {
			log.Printf("<%s> error getting keep warm lock for container: %v\n", i.Name, err)
			continue
		}

		keepWarm := keepWarmVal > 0
		if keepWarm {
			continue
		}

		// Check if a queue processing lock exists for the container and skip if it does
		// This indicates the container is currently processing an item in the queue
		_, err = i.Rdb.Get(context.TODO(), Keys.taskQueueProcessingLock(i.Workspace.Name, i.Stub.ExternalId, container.ContainerId)).Result()
		if err == nil || err != redis.Nil {
			continue
		}

		// If any tasks are currently running, skip this container
		tasksRunning, err := i.Rdb.Keys(context.TODO(), Keys.taskQueueTaskRunningLock(i.Workspace.Name, i.Stub.ExternalId, container.ContainerId, "*"))
		if err != nil && err != redis.Nil {
			log.Printf("<%s> error getting task running locks for container: %v\n", i.Name, err)
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
	return fmt.Sprintf("%s-%s-%s", taskQueueContainerPrefix, i.Stub.ExternalId, uuid.New().String()[:8])
}
