package endpoint

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type endpointTaskKey struct{ workspace, stub, task string }
type endpointRequestContextKey struct{}

type endpointTaskDeletion struct {
	key       endpointTaskKey
	ctxErr    error
	value     interface{}
	remaining time.Duration
	bounded   bool
}

// Model the task repository contract, not Redis indices, expiry or atomicity.
// The real dispatcher must call DeleteTaskState to release the stored message.
type endpointMemoryTasks struct {
	repository.TaskRepository
	mu        sync.Mutex
	messages  map[endpointTaskKey][]byte
	deletions []endpointTaskDeletion
	deleteErr error
}

func (r *endpointMemoryTasks) SetTaskState(ctx context.Context, workspace, stub, id string, msg []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages[endpointTaskKey{workspace, stub, id}] = append([]byte(nil), msg...)
	return nil
}

func (r *endpointMemoryTasks) GetTaskState(ctx context.Context, workspace, stub, id string) (*types.TaskMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	encoded, exists := r.messages[endpointTaskKey{workspace, stub, id}]
	encoded = append([]byte(nil), encoded...)
	r.mu.Unlock()
	if !exists {
		return nil, errors.New("task state not found")
	}
	msg := &types.TaskMessage{}
	if err := msg.Decode(encoded); err != nil {
		return nil, err
	}
	return msg, nil
}

func (r *endpointMemoryTasks) TasksInFlight(ctx context.Context, workspace, stub string) (int, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for key := range r.messages {
		if key.workspace == workspace && key.stub == stub {
			count++
		}
	}
	return count, nil
}

func (r *endpointMemoryTasks) DeleteTaskState(ctx context.Context, workspace, stub, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	deadline, bounded := ctx.Deadline()
	key := endpointTaskKey{workspace, stub, id}
	// Capture these while Complete is running, before the caller cancels cleanup.
	r.deletions = append(r.deletions, endpointTaskDeletion{
		key: key, ctxErr: ctx.Err(), value: ctx.Value(endpointRequestContextKey{}),
		bounded: bounded, remaining: time.Until(deadline),
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.deleteErr != nil {
		return r.deleteErr
	}
	delete(r.messages, key)
	return nil
}

type endpointRequestFixture struct {
	service  *HttpEndpointService
	ctx      context.Context
	authInfo *auth.AuthInfo
	instance *endpointInstance
	tasks    *endpointMemoryTasks
	mock     sqlmock.Sqlmock
}

func newEndpointRequestFixture(t *testing.T) *endpointRequestFixture {
	t.Helper()
	tasks := &endpointMemoryTasks{messages: make(map[endpointTaskKey][]byte)}
	backend, mock := repository.NewBackendPostgresRepositoryForTest()
	t.Cleanup(func() { require.NoError(t, mock.ExpectationsWereMet()) })
	// Exercise the real request path without a background expiry monitor.
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	stopMonitor()
	dispatcher, err := task.NewDispatcher(monitorCtx, tasks)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), endpointRequestContextKey{}, "request-value")
	workspace := &types.Workspace{Id: 1, Name: "endpoint-tests", ExternalId: "workspace-tests"}
	stub := &types.StubWithRelated{
		Stub:      types.Stub{Id: 2, ExternalId: "endpoint-stub", WorkspaceId: workspace.Id, Type: types.StubType(types.StubTypeASGI)},
		Workspace: *workspace,
	}
	authInfo := &auth.AuthInfo{Workspace: workspace, Token: &types.Token{Id: 1}}
	service := &HttpEndpointService{
		ctx: ctx, taskRepo: tasks, backendRepo: backend, taskDispatcher: dispatcher,
		endpointInstances: common.NewSafeMap[*endpointInstance](),
		requestStats:      newRequestStats(ctx, nil),
	}
	instance := &endpointInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Ctx: ctx, Workspace: workspace, Stub: stub, Token: authInfo.Token,
		StubConfig:  &types.StubConfigV1{MaxPendingTasks: 1, TaskPolicy: types.TaskPolicy{TTL: 600, Timeout: 5}},
		BackendRepo: backend, TaskRepo: tasks,
	}}
	// Creation errors must not enter the cached instance's request buffer.
	service.endpointInstances.Set(stub.ExternalId, instance)
	dispatcher.Register(string(types.ExecutorEndpoint), service.endpointTaskFactory)
	return &endpointRequestFixture{service: service, ctx: ctx, authInfo: authInfo, instance: instance, tasks: tasks, mock: mock}
}

func (f *endpointRequestFixture) request(ctx context.Context) (echo.Context, *httptest.ResponseRecorder) {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/invoke", nil).WithContext(ctx)
	return echo.New().NewContext(request, response), response
}

type endpointTaskIDArgument struct {
	id      string
	onMatch func(string)
}

func (a *endpointTaskIDArgument) Match(value driver.Value) bool {
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

func (f *endpointRequestFixture) failCreation(t *testing.T, ctx context.Context, onInsert func()) (string, error) {
	t.Helper()
	creationErr := errors.New("database temporarily unavailable")
	failedTask := &endpointTaskIDArgument{onMatch: func(id string) {
		// The real Send must have persisted the exact message before INSERT.
		state, err := f.tasks.GetTaskState(f.ctx, f.instance.Workspace.Name, f.instance.Stub.ExternalId, id)
		require.NoError(t, err)
		require.Equal(t, id, state.TaskId)
		require.Equal(t, f.instance.Workspace.Name, state.WorkspaceName)
		require.Equal(t, f.instance.Stub.ExternalId, state.StubId)
		require.Equal(t, string(types.ExecutorEndpoint), state.Executor)
		if onInsert != nil {
			onInsert()
		}
	}}
	f.mock.ExpectQuery("INSERT INTO task").
		WithArgs(failedTask, "", f.instance.Workspace.Id, f.instance.Stub.Id).
		WillReturnError(creationErr)
	echoCtx, _ := f.request(ctx)
	err := f.service.forwardRequest(echoCtx, f.authInfo, f.instance.Stub.ExternalId)
	require.ErrorIs(t, err, creationErr)
	require.NotEmpty(t, failedTask.id)
	return failedTask.id, err
}

func (f *endpointRequestFixture) assertCleanup(t *testing.T, id string) {
	t.Helper()
	require.Len(t, f.tasks.deletions, 1)
	deletion := f.tasks.deletions[0]
	require.Equal(t, endpointTaskKey{f.instance.Workspace.Name, f.instance.Stub.ExternalId, id}, deletion.key)
	require.NoError(t, deletion.ctxErr)
	require.Equal(t, "request-value", deletion.value)
	require.True(t, deletion.bounded)
	require.Positive(t, deletion.remaining)
	require.LessOrEqual(t, deletion.remaining, 5*time.Second)
}

func (f *endpointRequestFixture) pending(t *testing.T) int {
	t.Helper()
	count, err := f.tasks.TasksInFlight(f.ctx, f.instance.Workspace.Name, f.instance.Stub.ExternalId)
	require.NoError(t, err)
	return count
}

type endpointRoundTripFunc func(*http.Request) (*http.Response, error)

func (f endpointRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestEndpointTaskCreationFailureReleasesQuota(t *testing.T) {
	f := newEndpointRequestFixture(t)
	failedID, _ := f.failCreation(t, f.ctx, nil)
	require.Zero(t, f.pending(t), "failed creation must release pending-task quota")
	f.assertCleanup(t, failedID)
	_, err := f.tasks.GetTaskState(f.ctx, f.instance.Workspace.Name, f.instance.Stub.ExternalId, failedID)
	require.Error(t, err)

	// Keep buffer/HTTP forwarding real, with an in-process transport that never dials.
	var transportCalls atomic.Int32
	forwardedTask := make(chan string, 1)
	bufferCtx, stopBuffer := context.WithCancel(f.ctx)
	processorDone := make(chan struct{})
	buffer := &RequestBuffer{
		ctx: bufferCtx, workspace: f.instance.Workspace,
		stubId: f.instance.Stub.ExternalId, stubConfig: f.instance.StubConfig,
		buffer: abstractions.NewRingBuffer[*request](1), isASGI: true, maxTokens: 1,
		workReady:           make(chan struct{}, 1),
		availableContainers: []container{{id: "container-1", address: "in-process.invalid"}},
		httpClient: &http.Client{Transport: endpointRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			transportCalls.Add(1)
			forwardedTask <- request.Header.Get("X-TASK-ID")
			return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		})},
	}
	f.instance.buffer = buffer
	t.Cleanup(func() {
		stopBuffer()
		select {
		case <-processorDone:
		case <-time.After(time.Second):
			t.Error("request processor did not stop")
		}
	})
	go func() { defer close(processorDone); buffer.processRequests() }()
	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "external_id", "status", "failure_reason", "container_id", "workspace_id", "stub_id", "started_at", "ended_at", "created_at", "updated_at",
	})
	createdTask := &endpointTaskIDArgument{onMatch: func(id string) {
		rows.AddRow(1, id, types.TaskStatusPending, "", "", f.instance.Workspace.Id, f.instance.Stub.Id, nil, nil, now, now)
	}}
	f.mock.ExpectQuery("INSERT INTO task").
		WithArgs(createdTask, "", f.instance.Workspace.Id, f.instance.Stub.Id).
		WillReturnRows(rows)
	echoCtx, response := f.request(f.ctx)
	require.NoError(t, f.service.forwardRequest(echoCtx, f.authInfo, f.instance.Stub.ExternalId))
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, createdTask.id, response.Header().Get("X-Task-Id"))
	select {
	case id := <-forwardedTask:
		require.Equal(t, createdTask.id, id)
	default:
		t.Fatal("accepted task did not reach the in-process HTTP transport")
	}
	require.Equal(t, 1, f.pending(t), "accepted task must retain its state for the runner")
	state, err := f.tasks.GetTaskState(f.ctx, f.instance.Workspace.Name, f.instance.Stub.ExternalId, createdTask.id)
	require.NoError(t, err)
	require.Equal(t, createdTask.id, state.TaskId)
	f.assertCleanup(t, failedID) // No cleanup was added for the successful task.
	require.EqualValues(t, 1, transportCalls.Load())
	require.NoError(t, f.mock.ExpectationsWereMet())

	echoCtx, response = f.request(f.ctx)
	require.NoError(t, f.service.forwardRequest(echoCtx, f.authInfo, f.instance.Stub.ExternalId))
	require.Equal(t, http.StatusTooManyRequests, response.Code)
	require.Equal(t, 1, f.pending(t))
	require.EqualValues(t, 1, transportCalls.Load(), "quota rejection must not forward another request")
	f.assertCleanup(t, failedID)
	require.NoError(t, f.mock.ExpectationsWereMet())
}

func TestEndpointTaskCreationFailureAfterClientCancellation(t *testing.T) {
	f := newEndpointRequestFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	// CreateTask uses a background context: cancel the HTTP request, then return
	// a definite SQL error rather than the creation context's nil ctx.Err().
	failedID, _ := f.failCreation(t, ctx, cancel)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Zero(t, f.pending(t))
	f.assertCleanup(t, failedID)
}

func TestEndpointTaskCreationFailureKeepsExistingPendingTask(t *testing.T) {
	f := newEndpointRequestFixture(t)
	f.instance.StubConfig.MaxPendingTasks = 2
	existing, err := f.service.taskDispatcher.Send(f.ctx, string(types.ExecutorEndpoint), f.authInfo,
		f.instance.Stub.ExternalId, &types.TaskPayload{}, types.TaskPolicy{Expires: time.Now().Add(time.Minute)})
	require.NoError(t, err)
	existingID := existing.Metadata().TaskId
	require.Equal(t, 1, f.pending(t))
	failedID, _ := f.failCreation(t, f.ctx, nil)
	require.NotEqual(t, existingID, failedID)
	require.Equal(t, 1, f.pending(t), "cleanup must preserve another pending task")
	f.assertCleanup(t, failedID)
	state, err := f.tasks.GetTaskState(f.ctx, f.instance.Workspace.Name, f.instance.Stub.ExternalId, existingID)
	require.NoError(t, err)
	require.Equal(t, existingID, state.TaskId)
}

func TestEndpointTaskCreationFailurePreservesErrorWhenCleanupFails(t *testing.T) {
	f := newEndpointRequestFixture(t)
	f.tasks.deleteErr = errors.New("task repository unavailable")
	failedID, err := f.failCreation(t, f.ctx, nil)
	require.NotErrorIs(t, err, f.tasks.deleteErr, "cleanup failure must not replace the creation error")
	f.assertCleanup(t, failedID)
	require.Equal(t, 1, f.pending(t), "failed compensation cannot promise state removal")
}
