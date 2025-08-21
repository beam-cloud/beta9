package endpoint

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
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
	isASGI bool
}

func (i *endpointInstance) startContainers(containersToRun int) error {
	secrets, err := abstractions.ConfigureContainerRequestSecrets(i.Workspace, *i.buffer.stubConfig)
	if err != nil {
		return err
	}

	env := []string{}
	env = append(i.StubConfig.Env, env...)
	env = append(secrets, env...)
	env = append(env, []string{
		fmt.Sprintf("BETA9_TOKEN=%s", i.Token.Key),
		fmt.Sprintf("HANDLER=%s", i.StubConfig.Handler),
		fmt.Sprintf("ON_START=%s", i.StubConfig.OnStart),
		fmt.Sprintf("STUB_ID=%s", i.Stub.ExternalId),
		fmt.Sprintf("STUB_TYPE=%s", i.Stub.Type),
		fmt.Sprintf("WORKERS=%d", i.StubConfig.Workers),
		fmt.Sprintf("KEEP_WARM_SECONDS=%d", i.StubConfig.KeepWarmSeconds),
		fmt.Sprintf("PYTHON_VERSION=%s", i.StubConfig.PythonVersion),
		fmt.Sprintf("CALLBACK_URL=%s", i.StubConfig.CallbackUrl),
		fmt.Sprintf("TIMEOUT=%d", i.StubConfig.TaskPolicy.Timeout),
		fmt.Sprintf("BETA9_INPUTS=%s", i.StubConfig.Inputs.ToString()),
		fmt.Sprintf("BETA9_OUTPUTS=%s", i.StubConfig.Outputs.ToString()),
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

	for c := 0; c < containersToRun; c++ {
		containerId := i.genContainerId()

		mounts, err := abstractions.ConfigureContainerRequestMounts(
			containerId,
			i.Stub.Object.ExternalId,
			i.Workspace,
			*i.buffer.stubConfig,
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
			AppId:             i.Stub.App.ExternalId,
			ImageId:           i.StubConfig.Runtime.ImageId,
			StubId:            i.Stub.ExternalId,
			WorkspaceId:       i.Workspace.ExternalId,
			Workspace:         *i.Workspace,
			EntryPoint:        i.EntryPoint,
			Mounts:            mounts,
			Stub:              *i.Stub,
			CheckpointEnabled: checkpointEnabled,
			Preemptable:       true,
		}

		// Set initial keepwarm to prevent rapid spin-up/spin-down of containers
		i.Rdb.SetEx(
			context.Background(),
			Keys.endpointKeepWarmLock(i.Workspace.Name, i.Stub.ExternalId, containerId),
			1,
			time.Duration(i.StubConfig.KeepWarmSeconds)*time.Second,
		)

		err = i.Scheduler.Run(runRequest)
		if err != nil {
			log.Error().Str("instance_name", i.Name).Err(err).Msg("unable to run container")
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

		err := i.Scheduler.Stop(&types.StopContainerArgs{ContainerId: containerId, Reason: types.StopContainerReasonScheduler})
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

		// When deployment is stopped, all containers should be stopped even if they have keep warm
		if !i.IsActive {
			keys = append(keys, container.ContainerId)
			continue
		}

		if i.Stub.Type.IsDeployment() {

			// Skip containers with keep warm locks
			keepWarmVal, err := i.Rdb.Get(context.TODO(), Keys.endpointKeepWarmLock(i.Workspace.Name, i.Stub.ExternalId, container.ContainerId)).Int()
			if err != nil && err != redis.Nil {
				log.Error().Str("instance_name", i.Name).Err(err).Msg("error getting keep warm lock for container")
				continue
			}

			keepWarm := keepWarmVal > 0
			if keepWarm {
				continue
			}
		}

		keys = append(keys, container.ContainerId)
	}

	return keys, nil
}

func (i *endpointInstance) genContainerId() string {
	return fmt.Sprintf("%s-%s-%s", endpointContainerPrefix, i.Stub.ExternalId, uuid.New().String()[:8])
}
