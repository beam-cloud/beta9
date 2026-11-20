package abstractions

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	taskdispatcher "github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type schedulingFailureBackend struct {
	repository.BackendRepository
	task *types.TaskWithRelated
}

func (r *schedulingFailureBackend) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return r.task, nil
}

func (r *schedulingFailureBackend) UpdateTask(_ context.Context, _ string, task types.Task) (*types.Task, error) {
	r.task.Task = task
	return &task, nil
}

func TestTaskSchedulingFailureEndsTaskAndClearsDispatcherState(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	taskRepo := repository.NewTaskRedisRepository(rdb)
	dispatcher, err := taskdispatcher.NewDispatcher(ctx, taskRepo)
	require.NoError(t, err)

	message := &types.TaskMessage{TaskId: "task-id", StubId: "stub-id", WorkspaceName: "workspace"}
	encoded, err := message.Encode()
	require.NoError(t, err)
	require.NoError(t, taskRepo.SetTaskState(ctx, "workspace", "stub-id", "task-id", encoded))

	backend := &schedulingFailureBackend{task: &types.TaskWithRelated{
		Task: types.Task{
			ExternalId:  "task-id",
			ContainerId: "container-id",
			Status:      types.TaskStatusPending,
		},
		Workspace: types.Workspace{Name: "workspace"},
		Stub:      types.Stub{ExternalId: "stub-id"},
	}}
	handler := &taskSchedulingFailureHandler{backendRepo: backend, dispatcher: dispatcher}

	handled := handler.handle(common.NewContainerSchedulingFailedEvent(common.ContainerSchedulingFailure{
		TaskID:      "task-id",
		ContainerID: "container-id",
		ContainerRequestFailure: types.ContainerRequestFailure{
			PoolSelector: "my-pool",
			Reason:       types.ContainerSchedulingFailureWorkerCapacityTimeout,
		},
	}))

	require.True(t, handled)
	require.Equal(t, types.TaskStatusError, backend.task.Status)
	require.True(t, backend.task.EndedAt.Valid)
	require.Equal(t, "No compatible worker in pool \"my-pool\" became available before scheduling timed out. Check that a machine is online and has enough CPU, memory, and GPU capacity.", backend.task.FailureReason)
	_, err = taskRepo.GetTaskState(ctx, "workspace", "stub-id", "task-id")
	require.Error(t, err)
}

func TestSchedulingFailureMessageExplainsWorkerProvisioningLimit(t *testing.T) {
	message := SchedulingFailureMessage(types.ContainerRequestFailure{
		Reason:       types.ContainerSchedulingFailureProvisioningLimit,
		PoolSelector: "gpu-pool",
	})

	require.Equal(t, "Beta9 could not provision a compatible worker in pool \"gpu-pool\" after several attempts. Check that the pool has enough CPU, memory, and GPU capacity.", message)
}

func TestSchedulingFailureMessageExplainsSpotCapacityTimeout(t *testing.T) {
	message := SchedulingFailureMessage(types.ContainerRequestFailure{Reason: types.ContainerSchedulingFailureNoOpportunisticCapacity})

	require.Equal(t, "No spare capacity became available for this Spot workload before scheduling timed out. Try again later, or use the shared or performance CPU tier.", message)
}

func TestTaskSchedulingFailureIgnoresStaleContainer(t *testing.T) {
	backend := &schedulingFailureBackend{task: &types.TaskWithRelated{
		Task: types.Task{ExternalId: "task-id", ContainerId: "new-container", Status: types.TaskStatusPending},
	}}
	handler := &taskSchedulingFailureHandler{backendRepo: backend}

	handled := handler.handle(common.NewContainerSchedulingFailedEvent(common.ContainerSchedulingFailure{
		TaskID:                  "task-id",
		ContainerID:             "old-container",
		ContainerRequestFailure: types.ContainerRequestFailure{Reason: types.ContainerSchedulingFailureWorkerCapacityTimeout},
	}))

	require.True(t, handled)
	require.Equal(t, types.TaskStatusPending, backend.task.Status)
}

// A request that named no pool ran on capacity that is not its caller's to
// check: it is told what there was none of, and for how long.
func TestSchedulingFailureMessageNamesTheCapacityAServerlessRequestAskedFor(t *testing.T) {
	for _, test := range []struct {
		failure types.ContainerRequestFailure
		message string
	}{
		{
			failure: types.ContainerRequestFailure{
				Reason: types.ContainerSchedulingFailureWorkerCapacityTimeout, Gpus: []string{"A10G"}, WaitedSeconds: 1200,
			},
			message: "No A10G capacity became available in 20 minutes.",
		},
		{
			failure: types.ContainerRequestFailure{
				Reason: types.ContainerSchedulingFailureRetryLimit, Gpus: []string{"A10G", "L4", "T4"}, WaitedSeconds: 45,
			},
			message: "No A10G, L4 or T4 capacity became available in 45 seconds.",
		},
		{
			failure: types.ContainerRequestFailure{Reason: types.ContainerSchedulingFailureWorkerCapacityTimeout},
			message: "No capacity became available before scheduling timed out.",
		},
		{
			failure: types.ContainerRequestFailure{
				Reason: types.ContainerSchedulingFailureProvisioningLimit, Gpus: []string{"H100", "A100-80"}, WaitedSeconds: 600,
			},
			message: "No H100 or A100-80 capacity is available right now: a machine could not be started for this workload after several attempts.",
		},
	} {
		require.Equal(t, test.message, SchedulingFailureMessage(test.failure))
		require.True(t, test.failure.NoCapacity())
	}
}

// What the scheduler says of the request reaches the task and its caller
// whole.
func TestContainerSchedulingFailedEventCarriesWhatWasAskedFor(t *testing.T) {
	sent := common.ContainerSchedulingFailure{
		TaskID:      "task-id",
		ContainerID: "container-id",
		ContainerRequestFailure: types.ContainerRequestFailure{
			Reason: types.ContainerSchedulingFailureProvisioningLimit, Gpus: []string{"A10G", "T4"}, WaitedSeconds: 75,
		},
	}

	received, ok := common.ParseContainerSchedulingFailure(common.NewContainerSchedulingFailedEvent(sent))
	require.True(t, ok)
	require.Equal(t, sent, received)

	// An event from a gateway that sent neither is still a failure.
	event := common.NewContainerSchedulingFailedEvent(sent)
	delete(event.Args, types.EventAttrGpus)
	delete(event.Args, types.EventAttrWaitedSeconds)
	received, ok = common.ParseContainerSchedulingFailure(event)
	require.True(t, ok)
	require.Equal(t, types.ContainerSchedulingFailureProvisioningLimit, received.Reason)
	require.Empty(t, received.Gpus)
	require.Zero(t, received.WaitedSeconds)
}

// The status a gRPC handler answers with: capacity that was missing is
// ResourceExhausted, and anything else stays as it was.
func TestContainerNotScheduledErrorStatus(t *testing.T) {
	noCapacity := &ContainerNotScheduledError{Failure: types.ContainerRequestFailure{
		Reason: types.ContainerSchedulingFailureWorkerCapacityTimeout, Gpus: []string{"A10G"}, WaitedSeconds: 1200,
	}}
	st, ok := status.FromError(fmt.Errorf("stream ended: %w", noCapacity))
	require.True(t, ok)
	require.Equal(t, codes.ResourceExhausted, st.Code())
	require.Contains(t, st.Message(), "No A10G capacity became available in 20 minutes.")

	noController := &ContainerNotScheduledError{Failure: types.ContainerRequestFailure{
		Reason: types.ContainerSchedulingFailureNoController, PoolSelector: "gone",
	}}
	require.Equal(t, codes.Unknown, status.Code(noController))
	require.Equal(t, `The selected compute pool "gone" is unavailable or has no active controller.`, status.Convert(noController).Message())
}
