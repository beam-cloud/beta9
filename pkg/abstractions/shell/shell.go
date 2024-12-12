package shell

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type ShellService struct {
	ctx            context.Context
	config         types.AppConfig
	rdb            *common.RedisClient
	backendRepo    repository.BackendRepository
	containerRepo  repository.ContainerRepository
	scheduler      *scheduler.Scheduler
	shellInstances *common.SafeMap[*shellInstance]
	taskRepo       repository.TaskRepository
	tailscale      *network.Tailscale
}

var Keys = &keys{}

type keys struct{}

var (
	shellInstanceLock string = "shell:%s:%s:shell_lock"
)

var (
	shellContainerPrefix      string = "endpoint"
	shellMinRequestBufferSize int    = 10
)

func (ss *ShellService) StartShell(in *pb.StartShellRequest, stream pb.ShellService_StartShellServer) error {
	stubID := in.StubId

	// Get or create shell instance
	instance, err := ss.getOrCreateShellInstance(stream.Context(), stubID)

	if err != nil {
		return fmt.Errorf("failed to get or create shell instance: %w", err)
	}
}

func (ss *ShellService) getOrCreateShellInstance(ctx context.Context, stubId string) (*shellInstance, error) {
	instance, exists := ss.shellInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	stub, err := ss.backendRepo.GetStubByExternalId(ss.ctx, stubId)
	if err != nil {
		return nil, errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return nil, err
	}

	token, err := ss.backendRepo.RetrieveActiveToken(ss.ctx, stub.Workspace.Id)

	if err != nil {
		return nil, err
	}

	requestBufferSize := int(stubConfig.MaxPendingTasks) + 1
	if requestBufferSize < shellMinRequestBufferSize {
		requestBufferSize = shellMinRequestBufferSize
	}

	// Create shell instance to hold shell specific methods/fields
	instance = &shellInstance{}

	autoscaledInstance, err := abstractions.NewAutoscaledInstance(ss.ctx, &abstractions.AutoscaledInstanceConfig{
		Name:                fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		AppConfig:           ss.config,
		Rdb:                 ss.rdb,
		Stub:                stub,
		StubConfig:          stubConfig,
		Object:              &stub.Object,
		Workspace:           &stub.Workspace,
		Token:               token,
		Scheduler:           ss.scheduler,
		ContainerRepo:       ss.containerRepo,
		BackendRepo:         ss.backendRepo,
		TaskRepo:            ss.taskRepo,
		InstanceLockKey:     Keys.shellInstanceLock(stub.Workspace.Name, stubId),
		StartContainersFunc: instance.startContainers,
		StopContainersFunc:  instance.stopContainers,
	})

	if err != nil {
		return nil, err
	}

	if stub.Type.Kind() == types.StubTypeASGI {
		instance.isASGI = true
	}

	instance.buffer = NewRequestBuffer(autoscaledInstance.Ctx, ss.rdb, &stub.Workspace, stubId, requestBufferSize, ss.containerRepo, stubConfig, ss.tailscale, ss.config.Tailscale, instance.isASGI)

	// Embed autoscaled instance struct
	instance.AutoscaledInstance = autoscaledInstance

	// if instance.Autoscaler == nil {
	// 	instance.Autoscaler = abstractions.NewAutoscaler(instance, shellSampleFunc, shellServeScaleFunc)
	// }

	ss.shellInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Monitor()
	go func(i *shellInstance) {
		<-i.Ctx.Done()
		ss.shellInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

func (k *keys) shellInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(shellInstanceLock, workspaceName, stubId)
}
