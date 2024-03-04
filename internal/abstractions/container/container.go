package container

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/internal/abstractions"
	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
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
}

type ContainerServiceOpts struct {
	Config        types.AppConfig
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Tailscale     *network.Tailscale
	Scheduler     *scheduler.Scheduler
	RedisClient   *common.RedisClient
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

	task, err := cs.backendRepo.CreateTask(ctx, "", authInfo.Workspace.Id, stub.Stub.Id)
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

	mounts := abstractions.ConfigureContainerRequestMounts(
		stub.Object.ExternalId,
		authInfo.Workspace.Name,
		stubConfig,
	)

	err = cs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env: []string{
			fmt.Sprintf("TASK_ID=%s", taskId),
			fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
			fmt.Sprintf("BETA9_TOKEN=%s", authInfo.Token.Key),
			fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		},
		Cpu:        stubConfig.Runtime.Cpu,
		Memory:     stubConfig.Runtime.Memory,
		Gpu:        string(stubConfig.Runtime.Gpu),
		ImageId:    stubConfig.Runtime.ImageId,
		StubId:     stub.ExternalId,
		EntryPoint: []string{stubConfig.PythonVersion, "-m", "beta9.runner.container", base64.StdEncoding.EncodeToString(in.Command)},
		Mounts:     mounts,
	})
	if err != nil {
		return err
	}

	hostname, err := cs.containerRepo.GetContainerWorkerHostname(task.ContainerId)
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
	return cs.handleStreams(ctx, stream, authInfo.Workspace.Name, task.ExternalId, task.ContainerId, outputChan, keyEventChan)
}

func (cs *CmdContainerService) handleStreams(ctx context.Context,
	stream pb.ContainerService_ExecuteCommandServer,
	workspaceName, taskId, containerId string,
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
