package task

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
