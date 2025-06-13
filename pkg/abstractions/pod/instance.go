package pod

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type podInstance struct {
	*abstractions.AutoscaledInstance
	buffer *PodProxyBuffer
}

func (i *podInstance) startContainers(containersToRun int) error {
	secrets, err := abstractions.ConfigureContainerRequestSecrets(i.Workspace, *i.StubConfig)
	if err != nil {
		return err
	}

	env := []string{}
	env = append(i.StubConfig.Env, env...)
	env = append(secrets, env...)
	env = append(env, []string{
		fmt.Sprintf("BETA9_TOKEN=%s", i.Token.Key),
		fmt.Sprintf("STUB_ID=%s", i.Stub.ExternalId),
		fmt.Sprintf("STUB_TYPE=%s", i.Stub.Type),
		fmt.Sprintf("KEEP_WARM_SECONDS=%d", i.StubConfig.KeepWarmSeconds),
	}...)

	gpuRequest := types.GpuTypesToStrings(i.StubConfig.Runtime.Gpus)
	if i.StubConfig.Runtime.Gpu != "" {
		gpuRequest = append(gpuRequest, i.StubConfig.Runtime.Gpu.String())
	}

	gpuCount := i.StubConfig.Runtime.GpuCount
	if i.StubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	checkpointEnabled := i.StubConfig.CheckpointEnabled
	if gpuCount > 1 {
		checkpointEnabled = false
	}

	ports := []uint32{}
	if len(i.StubConfig.Ports) > 0 {
		ports = i.StubConfig.Ports
	}

	for c := 0; c < containersToRun; c++ {
		containerId := i.genContainerId()
		mounts, err := abstractions.ConfigureContainerRequestMounts(
			containerId,
			i.Stub.Object.ExternalId,
			i.Workspace,
			*i.StubConfig,
			i.Stub.ExternalId,
		)
		if err != nil {
			return err
		}

		runRequest := &types.ContainerRequest{
			ContainerId:       containerId,
			Env:               env,
			Cpu:               i.StubConfig.Runtime.Cpu,
			Memory:            i.StubConfig.Runtime.Memory,
			GpuRequest:        gpuRequest,
			GpuCount:          uint32(gpuCount),
			ImageId:           i.StubConfig.Runtime.ImageId,
			StubId:            i.Stub.ExternalId,
			AppId:             i.Stub.App.ExternalId,
			WorkspaceId:       i.Workspace.ExternalId,
			Workspace:         *i.Workspace,
			EntryPoint:        i.EntryPoint,
			Mounts:            mounts,
			Stub:              *i.Stub,
			CheckpointEnabled: checkpointEnabled,
			Ports:             ports,
		}

		ttl := time.Duration(i.StubConfig.KeepWarmSeconds) * time.Second
		key := Keys.podKeepWarmLock(i.Workspace.Name, i.Stub.ExternalId, runRequest.ContainerId)
		if ttl <= 0 {
			i.Rdb.Set(context.Background(), key, 1, 0)
		} else {
			i.Rdb.SetEx(context.Background(), key, 1, ttl)
		}

		err = i.Scheduler.Run(runRequest)
		if err != nil {
			log.Error().Str("instance_name", i.Name).Err(err).Msg("unable to run container")
			return err
		}

		continue
	}

	return nil
}

func (i *podInstance) stopContainers(containersToStop int) error {
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)

	containerIds, err := i.stoppableContainers()
	if err != nil {
		return err
	}

	for c := 0; c < containersToStop && len(containerIds) > 0; c++ {
		idx := rnd.Intn(len(containerIds))
		containerId := containerIds[idx]

		err := i.Scheduler.Stop(&types.StopContainerArgs{ContainerId: containerId, Force: true, Reason: types.StopContainerReasonScheduler})
		if err != nil {
			log.Error().Str("instance_name", i.Name).Err(err).Msg("unable to stop container")
			return err
		}

		// Remove the containerId from the containerIds slice to avoid
		// sending multiple stop requests to the same container
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)
	}

	return nil
}

func (i *podInstance) stoppableContainers() ([]string, error) {
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

		// When deployment is stopped, all containers should be stopped even if they have keep warm
		if !i.IsActive {
			keys = append(keys, container.ContainerId)
			continue
		}

		// Skip containers with keep warm locks
		keepWarmVal, err := i.Rdb.Get(context.TODO(), Keys.podKeepWarmLock(i.Workspace.Name, i.Stub.ExternalId, container.ContainerId)).Int()
		if err != nil && err != redis.Nil {
			log.Error().Str("instance_name", i.Name).Err(err).Msg("error getting keep warm lock for container")
			continue
		}

		keepWarm := keepWarmVal > 0
		if keepWarm {
			continue
		}

		connectionsVal, err := i.Rdb.Get(context.TODO(), Keys.podContainerConnections(i.Workspace.Name, i.Stub.ExternalId, container.ContainerId)).Int()
		if err != nil && err != redis.Nil {
			log.Error().Str("instance_name", i.Name).Err(err).Msg("error getting connections for container")
			continue
		}

		connections := connectionsVal > 0
		if connections {
			continue
		}

		keys = append(keys, container.ContainerId)
	}

	return keys, nil
}

func (i *podInstance) genContainerId() string {
	return fmt.Sprintf("%s-%s-%s", podContainerPrefix, i.Stub.ExternalId, uuid.New().String()[:8])
}
