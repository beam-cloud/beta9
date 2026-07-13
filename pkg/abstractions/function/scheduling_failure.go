package function

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const schedulingFailureHandlerTimeout = 10 * time.Second

func (fs *ContainerFunctionService) listenForSchedulingFailures() {
	events := common.NewEventBus(fs.rdb, common.EventBusSubscriber{
		Type:     common.EventTypeContainerSchedulingFailed,
		Callback: fs.handleContainerSchedulingFailure,
	})
	events.ReceiveEvents(fs.ctx)
}

func (fs *ContainerFunctionService) handleContainerSchedulingFailure(event *common.Event) bool {
	failure, ok := common.ParseContainerSchedulingFailure(event)
	if !ok {
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), schedulingFailureHandlerTimeout)
	defer cancel()

	task, err := fs.backendRepo.GetTaskWithRelated(ctx, failure.TaskID)
	if err != nil {
		log.Error().Err(err).Str("task_id", failure.TaskID).Msg("failed to load task after container scheduling failure")
		return false
	}
	if task == nil || task.ContainerId != failure.ContainerID {
		return true
	}

	if task.Status.IsInflight() {
		task.Status = types.TaskStatusError
		task.FailureReason = schedulingFailureMessage(failure)
		task.EndedAt = types.NullTime{}.Now()
		if _, err := fs.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task); err != nil {
			log.Error().Err(err).Str("task_id", failure.TaskID).Msg("failed to mark task after container scheduling failure")
			return false
		}
	}

	if err := fs.taskDispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId); err != nil {
		log.Error().Err(err).Str("task_id", failure.TaskID).Msg("failed to remove unscheduled task from dispatcher")
		return false
	}
	return true
}

func schedulingFailureMessage(failure common.ContainerSchedulingFailure) string {
	pool := ""
	if failure.PoolSelector != "" {
		pool = fmt.Sprintf(" in pool %q", failure.PoolSelector)
	}

	switch failure.Reason {
	case "worker_capacity_timeout", "retry_limit":
		return fmt.Sprintf("No compatible worker%s became available before scheduling timed out. Check that a machine is online and has enough CPU, memory, and GPU capacity.", pool)
	case "no_controller":
		return fmt.Sprintf("The selected compute pool %q is unavailable or has no active controller.", failure.PoolSelector)
	case "managed_fallback_concurrency_limit":
		return "Private compute had no capacity, and managed fallback exceeded the workspace concurrency limit."
	case "managed_fallback_no_capacity":
		return "Private compute and managed fallback had no compatible capacity."
	default:
		return fmt.Sprintf("The task could not be scheduled (%s).", failure.Reason)
	}
}
