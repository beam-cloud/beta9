package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	// How long we keep the containerId -> task mapping around. This outlives any
	// reasonable pod run so the lifecycle watcher can always resolve the task
	// when the container's state key is finally removed.
	podRunTaskMappingTtl time.Duration = 48 * time.Hour

	podRunTaskEventBufferSize int = 16384
)

// podRunTaskPolicy returns the task policy used for pod runs. Runs are not
// retried by default -- they execute arbitrary user entrypoints, so retrying
// automatically is unsafe. The expiry only applies while the task is unclaimed,
// which should never last long since we claim it immediately after scheduling.
func podRunTaskPolicy() types.TaskPolicy {
	return types.TaskPolicy{
		MaxRetries: 0,
		Timeout:    0,
		Expires:    time.Now().Add(time.Duration(types.MaxTaskTTL) * time.Second),
	}
}

// podRunTaskMapping links a pod run container back to its task so the
// container lifecycle watcher can update task state from key events alone.
type podRunTaskMapping struct {
	TaskId        string `json:"task_id"`
	WorkspaceName string `json:"workspace_name"`
	StubId        string `json:"stub_id"`
}

// PodTask tracks a single `pod/run` container as a task. Unlike function or
// taskqueue tasks, pod containers run arbitrary user entrypoints with no beta9
// runner inside to report task start/end -- so task state transitions are
// driven by container lifecycle key events (see watchPodRunTaskContainers).
type PodTask struct {
	ps          *GenericPodService
	msg         *types.TaskMessage
	containerId string
}

func (ps *GenericPodService) podTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &PodTask{
		ps:  ps,
		msg: &msg,
	}, nil
}

func (t *PodTask) Execute(ctx context.Context, options ...interface{}) error {
	authInfo := options[0].(*auth.AuthInfo)
	stub := options[1].(*types.StubWithRelated)
	opts := options[2].(runOptions)

	containerId := t.ps.generateContainerId(stub.ExternalId, stub.Type)
	t.containerId = containerId

	task, err := t.ps.backendRepo.CreateTask(ctx, &types.TaskParams{
		WorkspaceId: stub.WorkspaceId,
		StubId:      stub.Id,
		TaskId:      t.msg.TaskId,
		ContainerId: containerId,
	})
	if err != nil {
		return err
	}

	opts.taskId = t.msg.TaskId
	opts.containerId = containerId

	// Store the container -> task mapping before scheduling so the lifecycle
	// watcher can never observe a container event without a resolvable task.
	if err := t.ps.setPodRunTaskMapping(ctx, containerId, t.msg); err != nil {
		t.failTask(ctx, task, err.Error())
		return err
	}

	if _, err := t.ps.run(ctx, authInfo, stub, opts); err != nil {
		t.failTask(ctx, task, err.Error())
		t.ps.deletePodRunTaskMapping(ctx, containerId)
		t.ps.taskDispatcher.Complete(ctx, t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId)
		return err
	}

	// Claim the task on behalf of the container right away. Pod containers have
	// no runner inside to claim it, and unclaimed tasks are expired by the
	// dispatcher monitor.
	if err := t.ps.taskDispatcher.Claim(ctx, t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId, containerId); err != nil {
		log.Warn().Str("task_id", t.msg.TaskId).Str("container_id", containerId).Err(err).Msg("failed to claim pod run task")
	}

	return nil
}

func (t *PodTask) failTask(ctx context.Context, task *types.Task, reason string) {
	task.Status = types.TaskStatusError
	task.FailureReason = reason
	task.EndedAt = types.NullTime{}.Now()
	if _, err := t.ps.backendRepo.UpdateTask(ctx, task.ExternalId, *task); err != nil {
		log.Error().Str("task_id", task.ExternalId).Err(err).Msg("failed to mark pod run task as failed")
	}
}

func (t *PodTask) Retry(ctx context.Context) error {
	stub, err := t.ps.backendRepo.GetStubByExternalId(ctx, t.msg.StubId)
	if err != nil {
		return err
	}

	task, err := t.ps.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	token, err := t.ps.backendRepo.RetrieveActiveToken(ctx, stub.Workspace.Id)
	if err != nil {
		return err
	}
	authInfo := &auth.AuthInfo{Workspace: &stub.Workspace, Token: token}

	containerId := t.ps.generateContainerId(stub.ExternalId, stub.Type)
	t.containerId = containerId

	task.Status = types.TaskStatusRetry
	task.ContainerId = containerId
	task.FailureReason = ""
	updatedTask, err := t.ps.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	if err := t.ps.setPodRunTaskMapping(ctx, containerId, t.msg); err != nil {
		return err
	}

	if _, err := t.ps.run(ctx, authInfo, stub, runOptions{taskId: t.msg.TaskId, containerId: containerId}); err != nil {
		t.failTask(ctx, updatedTask, err.Error())
		t.ps.deletePodRunTaskMapping(ctx, containerId)
		return err
	}

	if err := t.ps.taskDispatcher.Claim(ctx, t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId, containerId); err != nil {
		log.Warn().Str("task_id", t.msg.TaskId).Str("container_id", containerId).Err(err).Msg("failed to claim pod run task")
	}

	return nil
}

func (t *PodTask) Cancel(ctx context.Context, reason types.TaskCancellationReason) error {
	task, err := t.ps.backendRepo.GetTask(ctx, t.msg.TaskId)
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
		// The container disappeared without the watcher finalizing the task
		// (e.g. missed key event). Try to recover the real exit status.
		task.Status = types.TaskStatusError
		task.FailureReason = "Container exited unexpectedly"
		if task.ContainerId != "" {
			if exitCode, err := t.ps.containerRepo.GetContainerExitCode(task.ContainerId); err == nil {
				task.Status, task.FailureReason = podRunTaskTerminalStatus(types.ContainerExitCode(exitCode))
			}
		}
	case types.TaskRequestCancelled:
		task.Status = types.TaskStatusCancelled
	default:
		task.Status = types.TaskStatusError
	}

	task.EndedAt = types.NullTime{}.Now()
	if _, err := t.ps.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task); err != nil {
		return err
	}

	if task.ContainerId != "" {
		t.ps.deletePodRunTaskMapping(ctx, task.ContainerId)

		err = t.ps.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: task.ContainerId,
			Reason:      types.StopContainerReasonUser,
			Force:       true,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *PodTask) HeartBeat(ctx context.Context) (bool, error) {
	task, err := t.ps.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return false, err
	}

	// Completed tasks have nothing to retry; report healthy so the dispatcher
	// leaves them alone (the watcher clears the in-flight state).
	if task.Status.IsCompleted() {
		return true, nil
	}

	if task.ContainerId == "" {
		return false, nil
	}

	// The container's state key exists for its entire lifetime (pending in the
	// scheduler backlog through running). If it's gone, the run is over.
	if _, err := t.ps.containerRepo.GetContainerState(task.ContainerId); err != nil {
		return false, nil
	}

	return true, nil
}

func (t *PodTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
		TaskId:        t.msg.TaskId,
		ContainerId:   t.containerId,
	}
}

func (t *PodTask) Message() *types.TaskMessage {
	return t.msg
}

// podRunTaskTerminalStatus maps a container exit code to a terminal task state.
func podRunTaskTerminalStatus(code types.ContainerExitCode) (types.TaskStatus, string) {
	switch code {
	case types.ContainerExitCodeSuccess:
		return types.TaskStatusComplete, ""
	case types.ContainerExitCodeUser, types.ContainerExitCodeAdmin, types.ContainerExitCodeScheduler:
		return types.TaskStatusCancelled, types.ExitCodeMessages[code]
	case types.ContainerExitCodeTtl:
		return types.TaskStatusTimeout, types.ExitCodeMessages[code]
	case types.ContainerExitCodeOomKill:
		return types.TaskStatusError, types.WorkerContainerExitCodeOomKillMessage
	default:
		if msg, ok := types.WorkerContainerExitCodes[code]; ok {
			return types.TaskStatusError, msg
		}
		return types.TaskStatusError, fmt.Sprintf("Container exited with code %d", int(code))
	}
}

func (ps *GenericPodService) setPodRunTaskMapping(ctx context.Context, containerId string, msg *types.TaskMessage) error {
	mapping := podRunTaskMapping{
		TaskId:        msg.TaskId,
		WorkspaceName: msg.WorkspaceName,
		StubId:        msg.StubId,
	}

	data, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	return ps.rdb.Set(ctx, Keys.podRunTask(containerId), data, podRunTaskMappingTtl).Err()
}

func (ps *GenericPodService) getPodRunTaskMapping(ctx context.Context, containerId string) (*podRunTaskMapping, error) {
	data, err := ps.rdb.Get(ctx, Keys.podRunTask(containerId)).Bytes()
	if err != nil {
		return nil, err
	}

	mapping := &podRunTaskMapping{}
	if err := json.Unmarshal(data, mapping); err != nil {
		return nil, err
	}

	return mapping, nil
}

func (ps *GenericPodService) deletePodRunTaskMapping(ctx context.Context, containerId string) {
	if err := ps.rdb.Del(ctx, Keys.podRunTask(containerId)).Err(); err != nil {
		log.Warn().Str("container_id", containerId).Err(err).Msg("failed to delete pod run task mapping")
	}
}

// watchPodRunTaskContainers drives pod run task state from container lifecycle
// key events: a running container marks its task RUNNING, and a removed
// container state key finalizes the task from the recorded exit code.
func (ps *GenericPodService) watchPodRunTaskContainers() {
	eventChan := make(chan common.KeyEvent, podRunTaskEventBufferSize)
	err := ps.keyEventManager.ListenForPattern(ps.ctx, common.RedisKeys.SchedulerContainerState(podContainerPrefix), eventChan)
	if err != nil {
		log.Error().Err(err).Msg("failed to listen for pod run container events")
		return
	}

	for {
		select {
		case event := <-eventChan:
			containerId := fmt.Sprintf("%s%s", podContainerPrefix, event.Key)

			mapping, err := ps.getPodRunTaskMapping(ps.ctx, containerId)
			if err != nil {
				// No mapping -> not a task-tracked pod run container (e.g. pod deployments)
				continue
			}

			switch event.Operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				ps.handlePodRunContainerUpdate(ps.ctx, containerId, mapping)
			case common.KeyOperationDel, common.KeyOperationExpired:
				ps.handlePodRunContainerExit(ps.ctx, containerId, mapping)
			}
		case <-ps.ctx.Done():
			return
		}
	}
}

func (ps *GenericPodService) handlePodRunContainerUpdate(ctx context.Context, containerId string, mapping *podRunTaskMapping) {
	state, err := ps.containerRepo.GetContainerState(containerId)
	if err != nil {
		return
	}

	if state.Status != types.ContainerStatusRunning {
		return
	}

	task, err := ps.backendRepo.GetTask(ctx, mapping.TaskId)
	if err != nil {
		return
	}

	if task.Status != types.TaskStatusPending && task.Status != types.TaskStatusRetry {
		return
	}

	task.Status = types.TaskStatusRunning
	task.StartedAt = types.NullTime{}.Now()
	if _, err := ps.backendRepo.UpdateTask(ctx, task.ExternalId, *task); err != nil {
		log.Error().Str("task_id", task.ExternalId).Err(err).Msg("failed to mark pod run task as running")
	}
}

func (ps *GenericPodService) handlePodRunContainerExit(ctx context.Context, containerId string, mapping *podRunTaskMapping) {
	task, err := ps.backendRepo.GetTask(ctx, mapping.TaskId)
	if err != nil {
		return
	}

	// The task moved on to a different container (e.g. a retry); only clean up
	// the stale mapping for this container.
	if task.ContainerId != containerId {
		ps.deletePodRunTaskMapping(ctx, containerId)
		return
	}

	if !task.Status.IsCompleted() {
		status := types.TaskStatusError
		failureReason := "Container exited unexpectedly"
		if exitCode, err := ps.containerRepo.GetContainerExitCode(containerId); err == nil {
			status, failureReason = podRunTaskTerminalStatus(types.ContainerExitCode(exitCode))
		}

		task.Status = status
		task.FailureReason = failureReason
		task.EndedAt = types.NullTime{}.Now()
		if _, err := ps.backendRepo.UpdateTask(ctx, task.ExternalId, *task); err != nil {
			log.Error().Str("task_id", task.ExternalId).Err(err).Msg("failed to finalize pod run task")
			return
		}
	}

	ps.deletePodRunTaskMapping(ctx, containerId)
	if err := ps.taskDispatcher.Complete(ctx, mapping.WorkspaceName, mapping.StubId, mapping.TaskId); err != nil {
		log.Warn().Str("task_id", mapping.TaskId).Err(err).Msg("failed to complete pod run task state")
	}
}
