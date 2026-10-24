package task

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStoreTaskResultHonorsFinalizationContext(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	id := uint(1)
	region, bucket, key, endpoint := "us-east-1", "bucket", "test-key", server.URL
	workspace := &types.Workspace{Name: "workspace", Storage: &types.WorkspaceStorage{
		Id: &id, Region: &region, BucketName: &bucket, AccessKey: &key, SecretKey: &key, EndpointUrl: &endpoint,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- (&Dispatcher{}).StoreTaskResult(ctx, workspace, "task", []byte("result")) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("result upload did not start")
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("result upload ignored the finalization context")
	}
}

type monitorTaskRepository struct {
	repository.TaskRepository
	deleted bool
}

func (r *monitorTaskRepository) GetTasksInFlight(context.Context) ([]*types.TaskMessage, error) {
	return []*types.TaskMessage{{Executor: "not-registered-yet"}}, nil
}

func (r *monitorTaskRepository) DeleteTaskState(context.Context, string, string, string) error {
	r.deleted = true
	return nil
}

func TestMonitorKeepsTasksUntilExecutorRegisters(t *testing.T) {
	repo := &monitorTaskRepository{}
	if err := (&Dispatcher{
		taskRepo:  repo,
		executors: common.NewSafeMap[func(context.Context, types.TaskMessage) (types.TaskInterface, error)](),
	}).monitorTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.deleted {
		t.Fatal("monitor deleted a task before its executor registered")
	}
}

type retryTestTask struct {
	types.TaskInterface
	msg   *types.TaskMessage
	retry func(context.Context) error
}

func (t *retryTestTask) Message() *types.TaskMessage { return t.msg }
func (t *retryTestTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{WorkspaceName: t.msg.WorkspaceName, StubId: t.msg.StubId, TaskId: t.msg.TaskId}
}
func (t *retryTestTask) Retry(ctx context.Context) error { return t.retry(ctx) }

func TestRetryIntentSurvivesInterruptedDispatchWithoutConsumingAnotherRetry(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	repo := repository.NewTaskRedisRepository(rdb)
	msg := types.TaskMessage{Executor: string(types.ExecutorFunction), WorkspaceName: "workspace", StubId: "stub", TaskId: "task", Policy: types.TaskPolicy{MaxRetries: 2, Expires: time.Now().Add(time.Hour)}}
	encoded, err := msg.Encode()
	require.NoError(t, err)
	require.NoError(t, repo.SetTaskState(ctx, "workspace", "stub", "task", encoded))
	require.NoError(t, repo.ClaimTask(ctx, "workspace", "stub", "task", "old-container"))
	dispatcher := &Dispatcher{taskRepo: repo, executors: common.NewSafeMap[func(context.Context, types.TaskMessage) (types.TaskInterface, error)]()}
	interrupted := errors.New("gateway lost before dispatch")
	first := &retryTestTask{msg: &msg, retry: func(context.Context) error { return interrupted }}
	require.ErrorIs(t, dispatcher.RetryTask(ctx, first), interrupted)
	reserved, err := repo.GetTaskState(ctx, "workspace", "stub", "task")
	require.NoError(t, err)
	require.True(t, reserved.RetryPending)
	require.EqualValues(t, 1, reserved.Retries)
	require.NotEmpty(t, reserved.RetryID)
	require.Equal(t, "old-container", rdb.Get(ctx, common.RedisKeys.TaskClaim("workspace", "stub", "task")).Val())
	calls := 0
	dispatcher.executors.Set(string(types.ExecutorFunction), func(_ context.Context, restored types.TaskMessage) (types.TaskInterface, error) {
		return &retryTestTask{msg: &restored, retry: func(ctx context.Context) error {
			calls++
			require.Equal(t, reserved.RetryID, restored.RetryID)
			require.EqualValues(t, 1, restored.Retries)
			return repo.ClaimTask(ctx, "workspace", "stub", "task", "new-container")
		}}, nil
	})
	// A new gateway resumes the durable intent even while the old claim exists.
	require.NoError(t, dispatcher.monitorTasks(ctx))
	require.Equal(t, 1, calls)
	committed, err := repo.GetTaskState(ctx, "workspace", "stub", "task")
	require.NoError(t, err)
	require.False(t, committed.RetryPending)
	require.EqualValues(t, 1, committed.Retries)
	// A stale snapshot from the previous gateway cannot dispatch it again.
	require.NoError(t, dispatcher.RetryTask(ctx, first))
	require.Equal(t, 1, calls)
}
