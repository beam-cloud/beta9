package function

import (
	"context"
	"sync"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// invocationStream is the caller's end of an invocation: what it is sent,
// and the trailer its stream ends with.
type invocationStream struct {
	pb.FunctionService_FunctionInvokeServer
	ctx context.Context

	mu        sync.Mutex
	responses []*pb.FunctionInvokeResponse
	trailer   metadata.MD
}

func (s *invocationStream) Context() context.Context { return s.ctx }

func (s *invocationStream) Send(response *pb.FunctionInvokeResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, response)
	return nil
}

func (s *invocationStream) SetTrailer(md metadata.MD) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trailer = metadata.Join(s.trailer, md)
}

// phases are the pending phases the caller was told, in order.
func (s *invocationStream) phases() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var phases []string
	for _, response := range s.responses {
		if response.Pending != nil {
			phases = append(phases, response.Pending.Phase+"/"+response.Pending.Reason)
		}
	}
	return phases
}

// pendingInvocation is an invocation whose container the scheduler has not
// placed, on the repository the gateway uses.
type pendingInvocation struct {
	service    *ContainerFunctionService
	containers repository.ContainerRepository
	task       *types.TaskWithRelated
	stream     *invocationStream
	authInfo   *auth.AuthInfo
}

const pendingInvocationContainer = "function-stub-1"

func newPendingInvocation(t *testing.T) *pendingInvocation {
	t.Helper()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	containers := repository.NewContainerRedisRepositoryForTest(rdb)
	require.NoError(t, containers.SetContainerState(pendingInvocationContainer, &types.ContainerState{
		ContainerId: pendingInvocationContainer,
		StubId:      "stub",
		Status:      types.ContainerStatusPending,
		ScheduledAt: time.Now().Unix(),
	}))

	task := &types.TaskWithRelated{
		Task:      types.Task{ExternalId: "task", Status: types.TaskStatusPending, ContainerId: pendingInvocationContainer},
		Workspace: types.Workspace{ExternalId: "workspace"},
		Stub:      types.Stub{ExternalId: "stub"},
	}
	authInfo := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "workspace", Name: "workspace"}, Token: &types.Token{Key: "token"}}
	ctx, cancel := context.WithTimeout(auth.ContextWithAuthInfo(context.Background(), authInfo), 10*time.Second)
	t.Cleanup(cancel)

	return &pendingInvocation{
		service: &ContainerFunctionService{
			ctx:             context.Background(),
			rdb:             rdb,
			keyEventManager: common.NewKeyEventManager(rdb),
			backendRepo:     completedInvocationBackend{task: task},
			containerRepo:   containers,
		},
		containers: containers,
		task:       task,
		stream:     &invocationStream{ctx: ctx},
		authInfo:   authInfo,
	}
}

// follow streams the invocation's container to its caller, as FunctionInvoke
// does once the task is accepted.
func (i *pendingInvocation) follow(containerId string) <-chan error {
	done := make(chan error, 1)
	go func() {
		task := &FunctionTask{containerId: containerId, msg: &types.TaskMessage{TaskId: "task"}}
		done <- i.service.stream(i.stream.ctx, i.stream, i.authInfo, task)
	}()
	return done
}

// giveUp is the scheduler giving up on the container.
func (i *pendingInvocation) giveUp(t *testing.T, containerId string, failure types.ContainerRequestFailure) {
	t.Helper()
	require.NoError(t, i.containers.DeleteContainerState(containerId))
	require.NoError(t, i.containers.SetContainerRequestFailure(containerId, failure))
}

var noA10G = types.ContainerRequestFailure{
	Reason:        types.ContainerSchedulingFailureWorkerCapacityTimeout,
	Gpus:          []string{"A10G"},
	WaitedSeconds: 1200,
}

func requireNoA10G(t *testing.T, err error, stream *invocationStream) {
	t.Helper()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "No A10G capacity became available in 20 minutes.", status.Convert(err).Message())
	require.Equal(t, []string{string(types.ContainerSchedulingFailureWorkerCapacityTimeout)}, stream.trailer.Get(abstractions.SchedulingFailureTrailer))
}

// A caller that waits for capacity which never comes is told so. The task is
// still pending when the stream ends: what marks it failed runs on its own.
func TestInvocationWhoseContainerIsNotScheduledIsToldWhy(t *testing.T) {
	invocation := newPendingInvocation(t)
	_, err := invocation.containers.SetContainerPending(
		pendingInvocationContainer, types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity, time.Now(),
	)
	require.NoError(t, err)
	done := invocation.follow(pendingInvocationContainer)

	require.Eventually(t, func() bool {
		return len(invocation.stream.phases()) > 0
	}, 5*time.Second, 10*time.Millisecond, "the caller was not told what its invocation waits for")
	require.Equal(t, []string{"waiting_for_capacity/no_capacity"}, invocation.stream.phases())

	invocation.giveUp(t, pendingInvocationContainer, noA10G)

	requireNoA10G(t, <-done, invocation.stream)
}

// The scheduler giving up on a container the task has moved on from is not
// the end of the task.
func TestInvocationOfAnAttemptTheTaskMovedOnFromWaitsForRecovery(t *testing.T) {
	invocation := newPendingInvocation(t)
	invocation.task.ContainerId = "function-stub-2"
	done := invocation.follow(pendingInvocationContainer)

	invocation.giveUp(t, pendingInvocationContainer, noA10G)

	err := <-done
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Empty(t, invocation.stream.trailer)
}

// A caller that comes back for an invocation the scheduler gave up on is told
// the same, for as long as the reason is kept.
func TestResumedInvocationIsToldWhyItsContainerWasNotScheduled(t *testing.T) {
	invocation := newPendingInvocation(t)
	invocation.giveUp(t, pendingInvocationContainer, noA10G)

	// Before the task is marked failed, and after.
	requireNoA10G(t, invocation.service.resumeInvocation("stub", "task", invocation.stream), invocation.stream)

	invocation.task.Status = types.TaskStatusError
	invocation.stream.trailer = nil
	requireNoA10G(t, invocation.service.resumeInvocation("stub", "task", invocation.stream), invocation.stream)
}

// A task that failed for another reason has its result, as before.
func TestResumedInvocationThatFailedOnItsOwnReturnsItsResult(t *testing.T) {
	invocation := newPendingInvocation(t)
	require.NoError(t, invocation.containers.DeleteContainerState(pendingInvocationContainer))
	invocation.task.Status = types.TaskStatusError

	require.NoError(t, invocation.service.resumeInvocation("stub", "task", invocation.stream))
	require.Empty(t, invocation.stream.trailer)
	last := invocation.stream.responses[len(invocation.stream.responses)-1]
	require.True(t, last.Done)
	require.EqualValues(t, 1, last.ExitCode)
}
