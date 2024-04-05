package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	abCommon "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type FunctionTask struct {
	msg         *types.TaskMessage
	fs          *RunCFunctionService
	containerId string
}

func (ft *FunctionTask) Execute(ctx context.Context) error {
	stub, err := ft.fs.backendRepo.GetStubByExternalId(ctx, ft.msg.StubId)
	if err != nil {
		return err
	}

	taskId := ft.msg.TaskId
	containerId := ft.fs.genContainerId(taskId)

	ft.containerId = containerId

	_, err = ft.fs.backendRepo.CreateTask(ctx, &types.TaskParams{
		WorkspaceId: stub.WorkspaceId,
		StubId:      stub.Id,
		TaskId:      taskId,
		ContainerId: containerId,
	})
	if err != nil {
		return err
	}

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return err
	}

	args, err := json.Marshal(types.TaskPayload{
		Args:   ft.msg.Args,
		Kwargs: ft.msg.Kwargs,
	})
	if err != nil {
		return err
	}

	err = ft.fs.rdb.Set(ctx, Keys.FunctionArgs(stub.Workspace.Name, taskId), args, functionArgsExpirationTimeout).Err()
	if err != nil {
		return errors.New("unable to store function args")
	}

	// Don't allow negative compute requests
	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultFunctionContainerCpu
	}

	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultFunctionContainerMemory
	}

	mounts := abCommon.ConfigureContainerRequestMounts(
		stub.Object.ExternalId,
		stub.Workspace.Name,
		stubConfig,
	)

	token, err := ft.fs.backendRepo.RetrieveActiveToken(ctx, stub.Workspace.Id)
	if err != nil {
		return err
	}

	err = ft.fs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env: []string{
			fmt.Sprintf("TASK_ID=%s", taskId),
			fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
			fmt.Sprintf("BETA9_TOKEN=%s", token.Key),
			fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		},
		Cpu:         stubConfig.Runtime.Cpu,
		Memory:      stubConfig.Runtime.Memory,
		Gpu:         string(stubConfig.Runtime.Gpu),
		GpuCount:    1,
		ImageId:     stubConfig.Runtime.ImageId,
		StubId:      stub.ExternalId,
		WorkspaceId: stub.Workspace.ExternalId,
		EntryPoint:  []string{stubConfig.PythonVersion, "-m", "beta9.runner.function"},
		Mounts:      mounts,
	})
	if err != nil {
		return err
	}

	return nil
}

func (ft *FunctionTask) Retry(ctx context.Context) error {
	return nil
}

func (ft *FunctionTask) Cancel(ctx context.Context) error {
	return nil
}

func (ft *FunctionTask) HeartBeat(ctx context.Context) (bool, error) {
	return true, nil
}

func (ft *FunctionTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		StubId:        ft.msg.StubId,
		WorkspaceName: ft.msg.WorkspaceName,
		TaskId:        ft.msg.TaskId,
		ContainerId:   ft.containerId,
	}
}
