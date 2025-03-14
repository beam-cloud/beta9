package function

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
	containerId := t.fs.genContainerId(taskId, stub.Type.Kind())

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

	containerId := t.fs.genContainerId(taskId, stub.Type.Kind())
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

	mounts, err := abstractions.ConfigureContainerRequestMounts(
		t.containerId,
		stub.Object.ExternalId,
		&stub.Workspace,
		stubConfig,
		stub.ExternalId,
	)
	if err != nil {
		return err
	}

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

	env := []string{}
	env = append(stubConfig.Env, env...)
	env = append(secrets, env...)
	env = append(env, []string{
		fmt.Sprintf("TASK_ID=%s", t.msg.TaskId),
		fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
		fmt.Sprintf("BETA9_TOKEN=%s", token.Key),
		fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		fmt.Sprintf("CALLBACK_URL=%s", stubConfig.CallbackUrl),
	}...)

	gpuRequest := types.GpuTypesToStrings(stubConfig.Runtime.Gpus)
	if stubConfig.Runtime.Gpu != "" {
		gpuRequest = append(gpuRequest, stubConfig.Runtime.Gpu.String())
	}

	gpuCount := stubConfig.Runtime.GpuCount
	if stubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	err = t.fs.scheduler.Run(&types.ContainerRequest{
		ContainerId: t.containerId,
		Env:         env,
		Cpu:         stubConfig.Runtime.Cpu,
		Memory:      stubConfig.Runtime.Memory,
		GpuRequest:  gpuRequest,
		GpuCount:    uint32(gpuCount),
		ImageId:     stubConfig.Runtime.ImageId,
		StubId:      stub.ExternalId,
		WorkspaceId: stub.Workspace.ExternalId,
		Workspace:   stub.Workspace,
		EntryPoint:  []string{stubConfig.PythonVersion, "-m", "beta9.runner.function"},
		Mounts:      mounts,
		Stub:        *stub,
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

	if !task.Status.IsInflight() {
		return nil
	}

	switch reason {
	case types.TaskExpired:
		task.Status = types.TaskStatusExpired
	case types.TaskExceededRetryLimit:
		task.Status = types.TaskStatusError
	case types.TaskRequestCancelled:
		task.Status = types.TaskStatusCancelled
	default:
		task.Status = types.TaskStatusError
	}

	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
	_, err = t.fs.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *FunctionTask) HeartBeat(ctx context.Context) (bool, error) {
	task, err := t.fs.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return false, err
	}

	// Don't check heartbeats if the task has been running for less than 60 seconds
	if task.Status == types.TaskStatusRunning && time.Since(time.Unix(task.StartedAt.Time.Unix(), 0)) < time.Duration(defaultFunctionHeartbeatTimeoutS)*time.Second {
		return true, nil
	}

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

func (t *FunctionTask) Message() *types.TaskMessage {
	return t.msg
}
