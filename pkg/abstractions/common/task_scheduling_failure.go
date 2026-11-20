package abstractions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const taskSchedulingFailureTimeout = 10 * time.Second

type taskSchedulingFailureHandler struct {
	backendRepo repository.BackendRepository
	dispatcher  *task.Dispatcher
}

func ListenForTaskSchedulingFailures(
	ctx context.Context,
	rdb *common.RedisClient,
	backendRepo repository.BackendRepository,
	dispatcher *task.Dispatcher,
) {
	handler := &taskSchedulingFailureHandler{
		backendRepo: backendRepo,
		dispatcher:  dispatcher,
	}
	events := common.NewEventBus(rdb, common.EventBusSubscriber{
		Type:     common.EventTypeContainerSchedulingFailed,
		Callback: handler.handle,
	})
	events.ReceiveEvents(ctx)
}

func (h *taskSchedulingFailureHandler) handle(event *common.Event) bool {
	failure, ok := common.ParseContainerSchedulingFailure(event)
	if !ok {
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), taskSchedulingFailureTimeout)
	defer cancel()

	task, err := h.backendRepo.GetTaskWithRelated(ctx, failure.TaskID)
	if err != nil {
		log.Error().Err(err).Str("task_id", failure.TaskID).Msg("failed to load task after container scheduling failure")
		return false
	}
	if task == nil || task.ContainerId != failure.ContainerID {
		return true
	}

	if task.Status.IsInflight() {
		task.Status = types.TaskStatusError
		task.FailureReason = SchedulingFailureMessage(failure.ContainerRequestFailure)
		task.EndedAt = types.NullTime{}.Now()
		if _, err := h.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task); err != nil {
			log.Error().Err(err).Str("task_id", failure.TaskID).Msg("failed to mark task after container scheduling failure")
			return false
		}
	}

	if err := h.dispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId); err != nil {
		log.Error().Err(err).Str("task_id", failure.TaskID).Msg("failed to remove unscheduled task from dispatcher")
		return false
	}
	return true
}

// SchedulingFailureMessage says why the scheduler gave up on a request, to
// the person who made it.
func SchedulingFailureMessage(failure types.ContainerRequestFailure) string {
	pool := ""
	if failure.PoolSelector != "" {
		pool = fmt.Sprintf(" in pool %q", failure.PoolSelector)
	}

	switch failure.Reason {
	case types.ContainerSchedulingFailureProvisioningLimit:
		// A request that named no pool ran on Beam's capacity, which is not
		// its caller's to check.
		if failure.PoolSelector == "" {
			return fmt.Sprintf("No %s is available right now: a machine could not be started for this workload after several attempts.", requestedCapacity(failure))
		}
		return fmt.Sprintf("Beta9 could not provision a compatible worker%s after several attempts. Check that the pool has enough CPU, memory, and GPU capacity.", pool)
	case types.ContainerSchedulingFailureWorkerCapacityTimeout, types.ContainerSchedulingFailureRetryLimit:
		if failure.PoolSelector == "" {
			return fmt.Sprintf("No %s became available %s.", requestedCapacity(failure), schedulingWait(failure))
		}
		return fmt.Sprintf("No compatible worker%s became available before scheduling timed out. Check that a machine is online and has enough CPU, memory, and GPU capacity.", pool)
	case types.ContainerSchedulingFailureNoController:
		return fmt.Sprintf("The selected compute pool %q is unavailable or has no active controller.", failure.PoolSelector)
	case types.ContainerSchedulingFailureManagedFallbackConcurrencyLimit:
		return "Private compute had no capacity, and managed fallback exceeded the workspace concurrency limit."
	case types.ContainerSchedulingFailureManagedFallbackNoCapacity:
		return "Private compute and managed fallback had no compatible capacity."
	case types.ContainerSchedulingFailureNoOpportunisticCapacity:
		return "No spare capacity became available for this Spot workload before scheduling timed out. Try again later, or use the shared or performance CPU tier."
	default:
		return fmt.Sprintf("The task could not be scheduled (%s).", failure.Reason)
	}
}

// requestedCapacity names the capacity a request asked for: "A10G capacity",
// "A10G or T4 capacity", or "capacity" for a request for no GPU.
func requestedCapacity(failure types.ContainerRequestFailure) string {
	switch len(failure.Gpus) {
	case 0:
		return "capacity"
	case 1:
		return failure.Gpus[0] + " capacity"
	default:
		last := len(failure.Gpus) - 1
		return strings.Join(failure.Gpus[:last], ", ") + " or " + failure.Gpus[last] + " capacity"
	}
}

// schedulingWait says how long the scheduler tried: "in 20 minutes".
func schedulingWait(failure types.ContainerRequestFailure) string {
	switch waited := time.Duration(failure.WaitedSeconds) * time.Second; {
	case waited <= 0:
		return "before scheduling timed out"
	case waited < 2*time.Minute:
		return fmt.Sprintf("in %d seconds", int(waited/time.Second))
	default:
		return fmt.Sprintf("in %d minutes", int(waited.Round(time.Minute)/time.Minute))
	}
}

// ContainerNotScheduledError is how the wait for a container ends when the
// scheduler gave up on it. A gRPC handler that returns it answers with its
// message, and with ResourceExhausted when capacity was what was missing.
type ContainerNotScheduledError struct {
	Failure types.ContainerRequestFailure
}

func (e *ContainerNotScheduledError) Error() string {
	return SchedulingFailureMessage(e.Failure)
}

func (e *ContainerNotScheduledError) GRPCStatus() *status.Status {
	code := codes.Unknown
	if e.Failure.NoCapacity() {
		code = codes.ResourceExhausted
	}
	return status.New(code, e.Error())
}

// SchedulingFailureTrailer is the trailer of a stream that ends with a
// ContainerNotScheduledError. It names why the scheduler gave up
// (types.ContainerSchedulingFailureReason), which is how a client tells this
// end from another with the same status code.
const SchedulingFailureTrailer = "x-beta9-scheduling-failure"

// NotScheduled reports whether err is that of a container the scheduler gave
// up on. If it is, the stream names the reason in its trailer.
func NotScheduled(stream grpc.ServerStream, err error) bool {
	var notScheduled *ContainerNotScheduledError
	if !errors.As(err, &notScheduled) {
		return false
	}
	stream.SetTrailer(metadata.Pairs(SchedulingFailureTrailer, string(notScheduled.Failure.Reason)))
	return true
}
