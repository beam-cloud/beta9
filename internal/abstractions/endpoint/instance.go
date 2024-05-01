package endpoint

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func withAutoscaler(constructor func(i *endpointInstance) *abstractions.Autoscaler[*endpointInstance, *endpointAutoscalerSample]) func(*endpointInstance) {
	return func(i *endpointInstance) {
		i.Autoscaler = constructor(i)
	}
}

func withEntryPoint(entryPoint func(instance *endpointInstance) []string) func(*endpointInstance) {
	return func(i *endpointInstance) {
		i.EntryPoint = entryPoint(i)
	}
}

type endpointInstance struct {
	*abstractions.AutoscaledInstance
	buffer *RequestBuffer
}

func (i *endpointInstance) startContainers(containersToRun int) error {
	for c := 0; c < containersToRun; c++ {
		containerId := i.genContainerId()
		runRequest := &types.ContainerRequest{
			ContainerId: containerId,
			Env: []string{
				fmt.Sprintf("BETA9_TOKEN=%s", i.Token.Key),
				fmt.Sprintf("HANDLER=%s", i.StubConfig.Handler),
				fmt.Sprintf("ON_START=%s", i.StubConfig.OnStart),
				fmt.Sprintf("STUB_ID=%s", i.Stub.ExternalId),
				fmt.Sprintf("STUB_TYPE=%s", i.Stub.Type),
				fmt.Sprintf("CONCURRENCY=%d", i.StubConfig.Concurrency),
				fmt.Sprintf("KEEP_WARM_SECONDS=%d", i.StubConfig.KeepWarmSeconds),
				fmt.Sprintf("PYTHON_VERSION=%s", i.StubConfig.PythonVersion),
				fmt.Sprintf("TIMEOUT=%d", i.StubConfig.TaskPolicy.Timeout),
			},
			Cpu:         i.StubConfig.Runtime.Cpu,
			Memory:      i.StubConfig.Runtime.Memory,
			Gpu:         string(i.StubConfig.Runtime.Gpu),
			ImageId:     i.StubConfig.Runtime.ImageId,
			StubId:      i.Stub.ExternalId,
			WorkspaceId: i.Workspace.ExternalId,
			EntryPoint:  i.EntryPoint,
			Mounts: abstractions.ConfigureContainerRequestMounts(
				i.Stub.Object.ExternalId,
				i.Workspace.Name,
				*i.buffer.stubConfig,
			),
		}

		// Set initial keepwarm to prevent rapid spin-up/spin-down of containers
		i.Rdb.SetEx(
			context.Background(),
			Keys.endpointKeepWarmLock(i.Workspace.Name, i.Stub.ExternalId, containerId),
			1,
			time.Duration(i.StubConfig.KeepWarmSeconds)*time.Second,
		)

		err := i.Scheduler.Run(runRequest)
		if err != nil {
			log.Printf("<%s> unable to run  container: %v", i.Name, err)
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

func (i *endpointInstance) stoppableContainers() ([]string, error) {
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
		keepWarmVal, err := i.Rdb.Get(context.TODO(), Keys.endpointKeepWarmLock(i.Workspace.Name, i.Stub.ExternalId, container.ContainerId)).Int()
		if err != nil && err != redis.Nil {
			log.Printf("<%s> error getting keep warm lock for container: %v\n", i.Name, err)
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
	return fmt.Sprintf("%s-%s-%s", endpointContainerPrefix, i.Stub.ExternalId, uuid.New().String()[:8])
}
