package container

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	defaultContainerCpu               int64         = 100
	defaultContainerMemory            int64         = 128
	containerCommandExpirationTimeout time.Duration = 600 * time.Second
	functionResultExpirationTimeout   time.Duration = 600 * time.Second
)

type ContainerServicer interface {
	pb.ContainerServiceServer
	ExecuteCommand(in *pb.CommandExecutionRequest, stream pb.ContainerService_ExecuteCommandServer) error
	StopContainerRun(ctx context.Context, in *pb.StopContainerRunRequest) (*pb.StopContainerRunResponse, error)
}

type ContainerService struct {
	pb.ContainerServiceServer
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
}

func NewContainerService(ctx context.Context,
	rdb *common.RedisClient,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	scheduler *scheduler.Scheduler,
) (ContainerServicer, error) {
	keyEventManager, err := common.NewKeyEventManager(rdb)
	if err != nil {
		return nil, err
	}

	cs := &ContainerService{
		backendRepo:     backendRepo,
		containerRepo:   containerRepo,
		scheduler:       scheduler,
		rdb:             rdb,
		keyEventManager: keyEventManager,
	}

	return cs, nil
}

func (cs *ContainerService) ExecuteCommand(in *pb.CommandExecutionRequest, stream pb.ContainerService_ExecuteCommandServer) error {
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
	containerId := fmt.Sprintf("%s%s", "container-", taskId)
	task.ContainerId = containerId

	// what does this do?
	go cs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)

	_, err = cs.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	if err != nil {
		return err
	}

	// Don't allow negative compute requests
	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultContainerCpu
	}

	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultContainerMemory
	}

	mounts := []types.Mount{
		{
			LocalPath: path.Join(types.DefaultExtractedObjectPath, authInfo.Workspace.Name, stub.Object.ExternalId),
			MountPath: types.WorkerUserCodeVolume,
			ReadOnly:  true,
		},
	}

	for _, v := range stubConfig.Volumes {
		mounts = append(mounts, types.Mount{
			LocalPath: path.Join(types.DefaultVolumesPath, authInfo.Workspace.Name, v.Id),
			LinkPath:  path.Join(types.DefaultExtractedObjectPath, authInfo.Workspace.Name, stub.Object.ExternalId, v.MountPath),
			MountPath: path.Join(types.ContainerVolumePath, v.MountPath),
			ReadOnly:  false,
		})
	}

	err = cs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env: []string{
			fmt.Sprintf("TASK_ID=%s", taskId),
			fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
			fmt.Sprintf("BEAM_TOKEN=%s", authInfo.Token.Key),
			fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		},
		Cpu:        stubConfig.Runtime.Cpu,
		Memory:     stubConfig.Runtime.Memory,
		Gpu:        string(stubConfig.Runtime.Gpu),
		ImageId:    stubConfig.Runtime.ImageId,
		EntryPoint: []string{"bash", "-c", string(in.Command)},
		Mounts:     mounts,
	})
	if err != nil {
		return err
	}

	hostname, err := cs.containerRepo.GetContainerWorkerHostname(task.ContainerId)
	if err != nil {
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key)
	if err != nil {
		return err
	}

	go client.StreamLogs(ctx, task.ContainerId, outputChan)
	return cs.handleStreams(ctx, stream, authInfo.Workspace.Name, task.ExternalId, task.ContainerId, outputChan, keyEventChan)
}

func (cs *ContainerService) StopContainerRun(ctx context.Context, in *pb.StopContainerRunRequest) (*pb.StopContainerRunResponse, error) {
	msg := "successfully stopped container"
	err := cs.scheduler.Stop(in.ContainerId)
	if err != nil {
		msg = err.Error()
	}
	return &pb.StopContainerRunResponse{
		Success: err == nil,
		Message: msg,
	}, err
}

func (cs *ContainerService) handleStreams(ctx context.Context,
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
