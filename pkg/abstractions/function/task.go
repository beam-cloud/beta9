package function

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type FunctionTask struct {
	msg         *types.TaskMessage
	fs          *RunCFunctionService
	containerId string
}

func (t *FunctionTask) Execute(ctx context.Context, options ...interface{}) error {
	stub, err := t.fs.backendRepo.GetStubByExternalId(ctx, t.msg.StubId)
	if err != nil {
		return err
	}

	taskId := t.msg.TaskId
	containerId := t.fs.genContainerId(taskId)

	t.containerId = containerId

	_, err = t.fs.backendRepo.CreateTask(ctx, &types.TaskParams{
		WorkspaceId: stub.WorkspaceId,
		StubId:      stub.Id,
		TaskId:      taskId,
		ContainerId: containerId,
	})
	if err != nil {
		return err
	}

	return t.run(ctx, stub)
}

func (t *FunctionTask) Retry(ctx context.Context) error {
	stub, err := t.fs.backendRepo.GetStubByExternalId(ctx, t.msg.StubId)
	if err != nil {
		return err
	}

	taskId := t.msg.TaskId

	task, err := t.fs.backendRepo.GetTaskWithRelated(ctx, taskId)
	if err != nil {
		return err
	}

	containerId := t.fs.genContainerId(taskId)
	t.containerId = containerId

	task.Status = types.TaskStatusRetry
	task.ContainerId = containerId
	_, err = t.fs.backendRepo.UpdateTask(ctx, taskId, task.Task)
	if err != nil {
		return err
	}

	return t.run(ctx, stub)
}

var cloudPickleHeader []byte = []byte{0x80, 0x05, 0x95}

func (t *FunctionTask) run(ctx context.Context, stub *types.StubWithRelated) error {
	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err := json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return err
	}

	args, err := json.Marshal(types.TaskPayload{
		Args:   t.msg.Args,
		Kwargs: t.msg.Kwargs,
	})
	if err != nil {

		return err
	}

	// If t.msg.Args has exactly one element and it's a []byte, check for magic bytes
	// This means the payload was cloudpickled
	if len(t.msg.Args) == 1 {
		if arg, ok := t.msg.Args[0].([]byte); ok && bytes.HasPrefix(arg, cloudPickleHeader) {
			args = arg
		}
	}

	err = t.fs.rdb.Set(ctx, Keys.FunctionArgs(stub.Workspace.Name, t.msg.TaskId), args, functionArgsExpirationTimeout).Err()
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

	mounts := abstractions.ConfigureContainerRequestMounts(
		stub.Object.ExternalId,
		stub.Workspace.Name,
		stubConfig,
		stub.ExternalId,
	)

	secrets, err := abstractions.ConfigureContainerRequestSecrets(
		&stub.Workspace,
		stubConfig,
	)
	if err != nil {
		return err
	}

	token, err := t.fs.backendRepo.RetrieveActiveToken(ctx, stub.Workspace.Id)
	if err != nil {
		return err
	}

	gpuCount := 0
	if stubConfig.Runtime.Gpu != "" {
		gpuCount = 1
	}

	env := []string{
		fmt.Sprintf("TASK_ID=%s", t.msg.TaskId),
		fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
		fmt.Sprintf("BETA9_TOKEN=%s", token.Key),
		fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		fmt.Sprintf("CALLBACK_URL=%s", stubConfig.CallbackUrl),
	}

	env = append(secrets, env...)

	err = t.fs.scheduler.Run(&types.ContainerRequest{
		ContainerId: t.containerId,
		Env:         env,
		Cpu:         stubConfig.Runtime.Cpu,
		Memory:      stubConfig.Runtime.Memory,
		Gpu:         string(stubConfig.Runtime.Gpu),
		GpuCount:    uint32(gpuCount),
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

func (t *FunctionTask) Cancel(ctx context.Context, reason types.TaskCancellationReason) error {
	task, err := t.fs.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	switch reason {
	case types.TaskExpired:
		task.Status = types.TaskStatusTimeout
	case types.TaskExceededRetryLimit:
		task.Status = types.TaskStatusError
	default:
		task.Status = types.TaskStatusError
	}

	_, err = t.fs.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *FunctionTask) HeartBeat(ctx context.Context) (bool, error) {
	res, err := t.fs.rdb.Exists(ctx, Keys.FunctionHeartbeat(t.msg.WorkspaceName, t.msg.TaskId)).Result()
	if err != nil {
		return false, err
	}

	return res > 0, nil
}

func (t *FunctionTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
		TaskId:        t.msg.TaskId,
		ContainerId:   t.containerId,
	}
}
