package taskqueue

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type taskQueuePutFixture struct {
	queue    *RedisTaskQueue
	ctx      context.Context
	authInfo *auth.AuthInfo
	stub     *types.StubWithRelated
	mock     sqlmock.Sqlmock
}

func newTaskQueuePutFixture(t *testing.T) *taskQueuePutFixture {
	t.Helper()
	redisServer := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{redisServer.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rdb.Close()) })

	taskRepo := repository.NewTaskRedisRepository(rdb)
	backend, mock := repository.NewBackendPostgresRepositoryForTest()
	t.Cleanup(func() { require.NoError(t, mock.ExpectationsWereMet()) })

	// The request path uses the real dispatcher without a background expiry monitor.
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	stopMonitor()
	dispatcher, err := task.NewDispatcher(monitorCtx, taskRepo)
	require.NoError(t, err)

	workspace := &types.Workspace{Id: 1, Name: "queue-tests", ExternalId: "workspace-tests"}
	stub := &types.StubWithRelated{
		Stub: types.Stub{
			Id:          2,
			ExternalId:  "e20b7c79-266d-49f0-a9b7-b2e83be572e7",
			WorkspaceId: workspace.Id,
			Type:        types.StubType(types.StubTypeTaskQueue),
		},
		Workspace: *workspace,
	}
	token := &types.Token{Id: 1, Active: true, TokenType: types.TokenTypeWorkspace}
	authInfo := &auth.AuthInfo{Workspace: workspace, Token: token}
	ctx := auth.ContextWithAuthInfo(context.Background(), authInfo)
	queue := &RedisTaskQueue{
		ctx:            ctx,
		rdb:            rdb,
		taskRepo:       taskRepo,
		backendRepo:    backend,
		taskDispatcher: dispatcher,
		queueInstances: common.NewSafeMap[*taskQueueInstance](),
		queueClient:    newRedisTaskQueueClient(rdb, taskRepo),
	}
	// Use the same cached-instance path as an already active queue deployment.
	queue.queueInstances.Set(stub.ExternalId, &taskQueueInstance{
		AutoscaledInstance: &abstractions.AutoscaledInstance{
			Ctx:         ctx,
			Rdb:         rdb,
			Workspace:   workspace,
			Stub:        stub,
			StubConfig:  &types.StubConfigV1{MaxPendingTasks: 1},
			Token:       token,
			BackendRepo: backend,
			TaskRepo:    taskRepo,
		},
		client: queue.queueClient,
	})
	dispatcher.Register(string(types.ExecutorTaskQueue), queue.taskQueueTaskFactory)

	return &taskQueuePutFixture{queue: queue, ctx: ctx, authInfo: authInfo, stub: stub, mock: mock}
}

type taskQueueTaskIDArgument struct {
	id      string
	onMatch func(string)
}

func (a *taskQueueTaskIDArgument) Match(value driver.Value) bool {
	id, ok := value.(string)
	if !ok || id == "" {
		return false
	}
	if a.id != "" {
		return a.id == id
	}
	a.id = id
	if a.onMatch != nil {
		a.onMatch(id)
	}
	return true
}

func TestTaskQueuePutReleasesQuotaWhenTaskCreationFails(t *testing.T) {
	f := newTaskQueuePutFixture(t)
	request := &pb.TaskQueuePutRequest{
		StubId:  f.stub.ExternalId,
		Payload: []byte(`{"args":["payload"],"kwargs":{}}`),
	}
	failedTask := &taskQueueTaskIDArgument{}
	f.mock.ExpectQuery("INSERT INTO task").
		WithArgs(failedTask, "", f.stub.WorkspaceId, f.stub.Id).
		WillReturnError(errors.New("database temporarily unavailable"))

	response, err := f.queue.TaskQueuePut(f.ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Empty(t, response.TaskId)
	require.NotEmpty(t, failedTask.id)

	count, err := f.queue.taskRepo.TasksInFlight(f.ctx, f.authInfo.Workspace.Name, f.stub.ExternalId)
	require.NoError(t, err)
	require.Zero(t, count, "failed task creation must not consume the pending-task quota")
	exists, err := f.queue.rdb.Exists(f.ctx, common.RedisKeys.TaskEntry(f.authInfo.Workspace.Name, f.stub.ExternalId, failedTask.id)).Result()
	require.NoError(t, err)
	require.Zero(t, exists)
	inGlobalIndex, err := f.queue.rdb.SIsMember(f.ctx, common.RedisKeys.TaskIndex(), common.RedisKeys.TaskEntry(f.authInfo.Workspace.Name, f.stub.ExternalId, failedTask.id)).Result()
	require.NoError(t, err)
	require.False(t, inGlobalIndex)
	length, err := f.queue.queueClient.QueueLength(f.ctx, f.authInfo.Workspace.Name, f.stub.ExternalId)
	require.NoError(t, err)
	require.Zero(t, length)

	// Once the backend recovers, the next request must reach creation and enqueue.
	now := time.Now()
	createdRows := sqlmock.NewRows([]string{
		"id", "external_id", "status", "failure_reason", "container_id", "workspace_id", "stub_id", "started_at", "ended_at", "created_at", "updated_at",
	})
	createdTask := &taskQueueTaskIDArgument{onMatch: func(id string) {
		createdRows.AddRow(1, id, types.TaskStatusPending, "", "", f.stub.WorkspaceId, f.stub.Id, nil, nil, now, now)
	}}
	f.mock.ExpectQuery("INSERT INTO task").
		WithArgs(createdTask, "", f.stub.WorkspaceId, f.stub.Id).
		WillReturnRows(createdRows)

	response, err = f.queue.TaskQueuePut(f.ctx, request)
	require.NoError(t, err)
	require.True(t, response.Ok, "a rejected enqueue must leave room for a subsequent request")
	require.Equal(t, createdTask.id, response.TaskId)
	count, err = f.queue.taskRepo.TasksInFlight(f.ctx, f.authInfo.Workspace.Name, f.stub.ExternalId)
	require.NoError(t, err)
	require.Equal(t, 1, count, "successfully enqueued tasks must retain their pending state")
	queuedMessages, err := f.queue.rdb.LRange(f.ctx, Keys.taskQueueList(f.authInfo.Workspace.Name, f.stub.ExternalId), 0, -1)
	require.NoError(t, err)
	require.Len(t, queuedMessages, 1)
	var queuedTask types.TaskMessage
	require.NoError(t, queuedTask.Decode([]byte(queuedMessages[0])))
	require.Equal(t, response.TaskId, queuedTask.TaskId)
	require.Equal(t, f.stub.ExternalId, queuedTask.StubId)
	require.Equal(t, f.authInfo.Workspace.Name, queuedTask.WorkspaceName)
	require.Equal(t, []interface{}{"payload"}, queuedTask.Args)

	// The normal limit still applies to the successfully queued task.
	_, err = f.queue.put(f.ctx, f.authInfo, f.stub.ExternalId, &types.TaskPayload{})
	var limitErr *types.ErrExceededTaskLimit
	require.ErrorAs(t, err, &limitErr)
	count, err = f.queue.taskRepo.TasksInFlight(f.ctx, f.authInfo.Workspace.Name, f.stub.ExternalId)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

type cancelTaskCreationBackend struct {
	repository.BackendRepository
	cancel context.CancelFunc
	taskID string
}

func (b *cancelTaskCreationBackend) CreateTask(ctx context.Context, params *types.TaskParams) (*types.Task, error) {
	b.taskID = params.TaskId
	b.cancel()
	return nil, ctx.Err()
}

func TestTaskQueuePutReleasesQuotaAfterRequestCancellation(t *testing.T) {
	f := newTaskQueuePutFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	backend := &cancelTaskCreationBackend{BackendRepository: f.queue.backendRepo, cancel: cancel}
	f.queue.backendRepo = backend

	_, err := f.queue.put(ctx, f.authInfo, f.stub.ExternalId, &types.TaskPayload{Args: []interface{}{"payload"}})
	require.ErrorIs(t, err, context.Canceled)
	require.NotEmpty(t, backend.taskID)
	count, err := f.queue.taskRepo.TasksInFlight(f.ctx, f.authInfo.Workspace.Name, f.stub.ExternalId)
	require.NoError(t, err)
	require.Zero(t, count, "request cancellation must not cancel task-state cleanup")
	exists, err := f.queue.rdb.Exists(f.ctx, common.RedisKeys.TaskEntry(f.authInfo.Workspace.Name, f.stub.ExternalId, backend.taskID)).Result()
	require.NoError(t, err)
	require.Zero(t, exists)
}
