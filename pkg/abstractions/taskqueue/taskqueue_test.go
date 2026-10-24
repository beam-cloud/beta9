package taskqueue

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestTaskQueueCompletionKeepsDispatcherStateUntilPersisted(t *testing.T) {
	for _, failUpdate := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "database_failure"}[failUpdate], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: &types.Workspace{Name: "workspace"}})
			rdb, err := repository.NewRedisClientForTest()
			require.NoError(t, err)
			t.Cleanup(func() { _ = rdb.Close() })
			tasks := repository.NewTaskRedisRepository(rdb)
			require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
			require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
			dispatcher, err := task.NewDispatcher(ctx, tasks)
			require.NoError(t, err)
			backend := completionBackend{update: func() error {
				claimed, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
				require.NoError(t, err)
				require.True(t, claimed, "the monitor must not report completion before the database update")
				_, err = tasks.GetTaskState(ctx, "workspace", "stub", "task")
				require.NoError(t, err, "a failed database update must remain recoverable by the dispatcher")
				if failUpdate {
					return errors.New("database unavailable")
				}
				return nil
			}}
			queue := &RedisTaskQueue{rdb: rdb, backendRepo: backend, taskDispatcher: dispatcher}
			response, err := queue.TaskQueueComplete(ctx, &pb.TaskQueueCompleteRequest{
				TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete),
			})
			require.NoError(t, err)
			require.Equal(t, !failUpdate, response.Ok)
			claimed, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
			require.NoError(t, err)
			require.Equal(t, failUpdate, claimed)
			inFlight, err := tasks.TasksInFlight(ctx, "workspace", "stub")
			require.NoError(t, err)
			require.Equal(t, map[bool]int{false: 0, true: 1}[failUpdate], inFlight)
		})
	}
}

func TestTaskQueueCompletionKeepsClaimDuringSlowResultUpload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploadStarted := make(chan struct{})
	releaseUpload := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpload) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(uploadStarted)
		<-releaseUpload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer release()
	id := uint(1)
	region, bucket, accessKey, secretKey, endpoint := "us-east-1", "bucket", "test-key", "test-secret", server.URL
	workspace := &types.Workspace{Name: "workspace", Storage: &types.WorkspaceStorage{
		Id: &id, Region: &region, BucketName: &bucket, AccessKey: &accessKey, SecretKey: &secretKey, EndpointUrl: &endpoint,
	}}
	ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: workspace})
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	tasks := repository.NewTaskRedisRepository(rdb)
	require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
	dispatcher, err := task.NewDispatcher(ctx, tasks)
	require.NoError(t, err)
	queue := &RedisTaskQueue{rdb: rdb, taskDispatcher: dispatcher, backendRepo: completionBackend{update: func() error { return nil }}}
	type outcome struct {
		response *pb.TaskQueueCompleteResponse
		err      error
	}
	finished := make(chan outcome, 1)
	go func() {
		response, err := queue.TaskQueueComplete(ctx, &pb.TaskQueueCompleteRequest{
			TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete), Result: []byte("result"),
		})
		finished <- outcome{response, err}
	}()
	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("result upload did not start")
	}
	claimed, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
	require.NoError(t, err)
	// Losing this claim makes the SDK stop its monitor, so its five-second
	// container lease expires while the gateway is still uploading the result.
	require.True(t, claimed, "slow storage must not let the monitor release an unfinished task")
	release()
	select {
	case result := <-finished:
		require.NoError(t, result.err)
		require.True(t, result.response.Ok)
	case <-time.After(5 * time.Second):
		t.Fatal("completion did not finish after the upload")
	}
}
