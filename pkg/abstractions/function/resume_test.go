package function

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
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

type completedInvocationBackend struct {
	repository.BackendRepository
	task *types.TaskWithRelated
}

func (r completedInvocationBackend) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return r.task, nil
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

type testCompletedInvocationContainers struct{ repository.ContainerRepository }

func (testCompletedInvocationContainers) GetContainerExitCode(string) (int, error) { return 0, nil }
