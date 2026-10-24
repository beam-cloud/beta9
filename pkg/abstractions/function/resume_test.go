package function

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type completedInvocationBackend struct {
	repository.BackendRepository
	task *types.TaskWithRelated
}

func (r completedInvocationBackend) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return r.task, nil
}

func (r completedInvocationBackend) GetTask(context.Context, string) (*types.Task, error) {
	return &r.task.Task, nil
}

type resumeInvocationStream struct {
	pb.FunctionService_FunctionInvokeServer
	ctx      context.Context
	response *pb.FunctionInvokeResponse
}

func (s *resumeInvocationStream) Context() context.Context { return s.ctx }
func (s *resumeInvocationStream) Send(response *pb.FunctionInvokeResponse) error {
	s.response = response
	return nil
}

func TestCompletedInvocationResumesLogsBeforeResult(t *testing.T) {
	redis := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	task := &types.TaskWithRelated{Task: types.Task{Status: types.TaskStatusComplete, ContainerId: "function-1"}, Workspace: types.Workspace{ExternalId: "workspace"}, Stub: types.Stub{ExternalId: "stub"}}
	service := &ContainerFunctionService{rdb: rdb, backendRepo: completedInvocationBackend{task: task}, containerRepo: testCompletedInvocationContainers{}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "workspace", Name: "workspace"}})
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(common.LogOffsetHeader, "invalid"))
	redis.Set(Keys.FunctionResult("workspace", "task"), "result")
	redis.Set(common.RedisKeys.SchedulerWorkerAddress("function-1"), "worker.internal")
	stream := &resumeInvocationStream{ctx: ctx}
	require.Equal(t, codes.InvalidArgument, status.Code(service.resumeInvocation("stub", "task", stream)))
	require.Nil(t, stream.response, "completed result must not skip cursor-aware log replay")
	redis.Del(common.RedisKeys.SchedulerWorkerAddress("function-1"))
	require.NoError(t, service.resumeInvocation("stub", "task", stream))
	require.True(t, stream.response.Done)
	require.Equal(t, []byte("result"), stream.response.Result)
	require.Equal(t, codes.PermissionDenied, status.Code(service.resumeInvocation("other-stub", "task", stream)))
}

func TestCompletedInvocationHandlesOldWorkerWithoutLogs(t *testing.T) {
	for _, code := range []codes.Code{codes.Unknown, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			redis := miniredis.RunT(t)
			rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
			require.NoError(t, err)
			t.Cleanup(func() { rdb.Close() })
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			worker := grpc.NewServer()
			pb.RegisterContainerServiceServer(worker, legacyCompletedWorker{code: code})
			go worker.Serve(listener)
			t.Cleanup(worker.Stop)
			task := &types.TaskWithRelated{Task: types.Task{Status: types.TaskStatusComplete, ContainerId: "function-1"}, Workspace: types.Workspace{ExternalId: "workspace"}, Stub: types.Stub{ExternalId: "stub"}}
			service := &ContainerFunctionService{ctx: context.Background(), rdb: rdb, keyEventManager: common.NewKeyEventManager(rdb), backendRepo: completedInvocationBackend{task: task}, containerRepo: testCompletedInvocationContainers{address: listener.Addr().String()}}
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "workspace", Name: "workspace"}, Token: &types.Token{Key: "test-token"}})
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			redis.Set(Keys.FunctionResult("workspace", "task"), "result")
			redis.Set(common.RedisKeys.SchedulerWorkerAddress("function-1"), listener.Addr().String())
			stream := &resumeInvocationStream{ctx: ctx}
			err = service.resumeInvocation("stub", "task", stream)
			if code == codes.Unknown {
				require.NoError(t, err)
				require.Equal(t, []byte("result"), stream.response.Result)
			} else {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, stream.response)
			}
		})
	}
}

func TestInvocationWaitsForRetryWhenAttemptLogsAreGone(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	worker := grpc.NewServer()
	pb.RegisterContainerServiceServer(worker, legacyCompletedWorker{code: codes.Unknown})
	go worker.Serve(listener)
	t.Cleanup(worker.Stop)
	current := &types.TaskWithRelated{Task: types.Task{ExternalId: "task", Status: types.TaskStatusRunning, ContainerId: "old-attempt"}}
	service := &ContainerFunctionService{ctx: context.Background(), rdb: rdb, keyEventManager: common.NewKeyEventManager(rdb), backendRepo: completedInvocationBackend{task: current}, containerRepo: testCompletedInvocationContainers{address: listener.Addr().String()}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	authInfo := &auth.AuthInfo{Workspace: &types.Workspace{Name: "workspace"}, Token: &types.Token{Key: "token"}}
	stream := &resumeInvocationStream{ctx: ctx}
	task := &FunctionTask{containerId: "old-attempt", msg: &types.TaskMessage{TaskId: "task"}}
	require.Equal(t, codes.Unavailable, status.Code(service.stream(ctx, stream, authInfo, task)))
	require.Nil(t, stream.response, "an intermediate attempt's exit cannot finish the invocation")
}

func TestFunctionResultReplayRequiresCurrentExecutionOwner(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	current := &types.TaskWithRelated{Task: types.Task{ExternalId: "task", Status: types.TaskStatusRunning, ContainerId: "container"}, Workspace: types.Workspace{ExternalId: "workspace"}, Stub: types.Stub{ExternalId: "stub"}}
	service := &ContainerFunctionService{rdb: rdb, backendRepo: completedInvocationBackend{task: current}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{Name: "workspace", ExternalId: "workspace"}})
	claim := common.RedisKeys.TaskClaim("workspace", "stub", "task")
	server.Set(claim, "container")
	request := &pb.FunctionSetResultRequest{TaskId: "task", ContainerId: "container", Result: []byte("result")}
	for range 2 {
		response, err := service.FunctionSetResult(ctx, request)
		require.NoError(t, err)
		require.True(t, response.Ok)
	}
	server.Set(claim, "replacement")
	request.Result = []byte("stale result")
	_, err = service.FunctionSetResult(ctx, request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, "result", rdb.Get(ctx, Keys.FunctionResult("workspace", "task")).Val())
}

type legacyCompletedWorker struct {
	pb.UnimplementedContainerServiceServer
	code codes.Code
}

func (w legacyCompletedWorker) ContainerStreamLogs(*pb.ContainerStreamLogsRequest, pb.ContainerService_ContainerStreamLogsServer) error {
	return status.Error(w.code, "container not found")
}

type testCompletedInvocationContainers struct {
	repository.ContainerRepository
	address string
}

func (r testCompletedInvocationContainers) GetWorkerAddress(context.Context, string) (string, error) {
	return r.address, nil
}

func (testCompletedInvocationContainers) GetContainerExitCode(string) (int, error) { return 0, nil }
