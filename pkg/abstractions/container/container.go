package container

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	pb "github.com/beam-cloud/beta9/proto"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	containerContainerPrefix          string        = "container-"
	defaultContainerCpu               int64         = 100
	defaultContainerMemory            int64         = 128
	containerCommandExpirationTimeout time.Duration = 600 * time.Second
	functionResultExpirationTimeout   time.Duration = 600 * time.Second
)

type ContainerService interface {
	pb.ContainerServiceServer
	ExecuteCommand(in *pb.CommandExecutionRequest, stream pb.ContainerService_ExecuteCommandServer) error
}

type CmdContainerService struct {
	pb.ContainerServiceServer
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	tailscale       *network.Tailscale
	config          types.AppConfig
	eventRepo       repository.EventRepository
}

type ContainerServiceOpts struct {
	Config        types.AppConfig
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Tailscale     *network.Tailscale
	Scheduler     *scheduler.Scheduler
	RedisClient   *common.RedisClient
	EventRepo     repository.EventRepository
}

func NewContainerService(
	ctx context.Context,
	opts ContainerServiceOpts,
) (ContainerService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	cs := &CmdContainerService{
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		tailscale:       opts.Tailscale,
		config:          opts.Config,
		eventRepo:       opts.EventRepo,
	}

	return cs, nil
}

func (cs *CmdContainerService) ExecuteCommand(in *pb.CommandExecutionRequest, stream pb.ContainerService_ExecuteCommandServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)
	keyEventChan := make(chan common.KeyEvent)

	stub, err := cs.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil {
		return err
	}

	go cs.eventRepo.PushRunStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)

	task, err := cs.backendRepo.CreateTask(ctx, &types.TaskParams{
		WorkspaceId: authInfo.Workspace.Id,
		StubId:      stub.Stub.Id,
	})
	if err != nil {
		return err
	}

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return err
	}

	taskId := task.ExternalId
	containerId := fmt.Sprintf("%s%s", containerContainerPrefix, taskId)
	task.ContainerId = containerId

	go cs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)

	_, err = cs.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	if err != nil {
		return err
	}

	// Don't allow negative and 0-valued compute requests
	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultContainerCpu
	}

	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultContainerMemory
	}

	mounts, err := abstractions.ConfigureContainerRequestMounts(
		containerId,
		stub.Object.ExternalId,
		authInfo.Workspace,
		stubConfig,
		stub.ExternalId,
	)
	if err != nil {
		return err
	}

	secrets, err := abstractions.ConfigureContainerRequestSecrets(
		authInfo.Workspace,
		stubConfig,
	)
	if err != nil {
		return err
	}

	env := []string{
		fmt.Sprintf("TASK_ID=%s", taskId),
		fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
		fmt.Sprintf("BETA9_TOKEN=%s", authInfo.Token.Key),
		fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		fmt.Sprintf("CALLBACK_URL=%s", stubConfig.CallbackUrl),
	}

	env = append(secrets, env...)

	gpuRequest := types.GpuTypesToStrings(stubConfig.Runtime.Gpus)
	if stubConfig.Runtime.Gpu != "" {
		gpuRequest = append(gpuRequest, stubConfig.Runtime.Gpu.String())
	}

	gpuCount := stubConfig.Runtime.GpuCount
	if stubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	err = cs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env:         env,
		Cpu:         stubConfig.Runtime.Cpu,
		Memory:      stubConfig.Runtime.Memory,
		GpuRequest:  gpuRequest,
		GpuCount:    uint32(gpuCount),
		ImageId:     stubConfig.Runtime.ImageId,
		StubId:      stub.ExternalId,
		WorkspaceId: authInfo.Workspace.ExternalId,
		Workspace:   *authInfo.Workspace,
		EntryPoint:  []string{stubConfig.PythonVersion, "-m", "beta9.runner.container", base64.StdEncoding.EncodeToString(in.Command)},
		Mounts:      mounts,
		Stub:        *stub,
	})
	if err != nil {
		return err
	}

	hostname, err := cs.containerRepo.GetWorkerAddress(ctx, task.ContainerId)
	if err != nil {
		return err
	}

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, cs.tailscale, cs.config.Tailscale)
	if err != nil {
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key, conn)
	if err != nil {
		return err
	}

	go client.StreamLogs(ctx, task.ContainerId, outputChan)
	return cs.handleStreams(ctx, stream, task.ExternalId, task.ContainerId, outputChan, keyEventChan)
}

func (cs *CmdContainerService) handleStreams(ctx context.Context,
	stream pb.ContainerService_ExecuteCommandServer,
	taskId, containerId string,
	outputChan chan common.OutputMsg, keyEventChan chan common.KeyEvent) error {

	var lastMessage common.OutputMsg

_stream:
	for {
		select {
		case o := <-outputChan:
			if err := stream.Send(&pb.CommandExecutionResponse{TaskId: taskId, Output: o.Msg, Done: o.Done}); err != nil {
				lastMessage = o
				break
			}

			if o.Done {
				lastMessage = o
				break _stream
			}
		case <-keyEventChan:
			exitCode, err := cs.containerRepo.GetContainerExitCode(containerId)
			if err != nil {
				exitCode = -1
			}

			if err := stream.Send(&pb.CommandExecutionResponse{TaskId: taskId, Done: true, ExitCode: int32(exitCode)}); err != nil {
				break
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if !lastMessage.Success {
		return errors.New("function failed")
	}

	return nil
}
