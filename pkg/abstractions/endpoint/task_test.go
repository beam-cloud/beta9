package endpoint

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type creationContextKey struct{}

type creationTaskRepository struct {
	repository.TaskRepository
	messages          map[[3]string][]byte
	deleted           [3]string
	deleteErr, ctxErr error
	contextValue      interface{}
	remaining         time.Duration
	bounded           bool
	calls             int
}

func (r *creationTaskRepository) SetTaskState(ctx context.Context, workspace, stub, id string, msg []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.messages[[3]string{workspace, stub, id}] = append([]byte(nil), msg...)
	return nil
}

func (r *creationTaskRepository) DeleteTaskState(ctx context.Context, workspace, stub, id string) error {
	r.calls++
	r.deleted = [3]string{workspace, stub, id}
	// Capture the context before Execute's deferred cleanup cancellation.
	r.ctxErr, r.contextValue = ctx.Err(), ctx.Value(creationContextKey{})
	deadline, bounded := ctx.Deadline()
	r.bounded, r.remaining = bounded, time.Until(deadline)
	if r.ctxErr != nil {
		return r.ctxErr
	}
	if r.deleteErr != nil {
		return r.deleteErr
	}
	delete(r.messages, r.deleted)
	return nil
}

func (r *creationTaskRepository) WithTaskMonitorLease(ctx context.Context, _ func(context.Context) error) error {
	return ctx.Err() // The monitor is disabled; only synchronous task calls touch state.
}

func TestEndpointTaskCreationFailure(t *testing.T) {
	for _, test := range []struct {
		name        string
		cancel      bool
		cleanupErr  error
		wantPending int
	}{
		{name: "database failure", wantPending: 1},
		{name: "canceled caller", cancel: true, wantPending: 1},
		{name: "cleanup failure", cleanupErr: errors.New("task repository unavailable"), wantPending: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			tasks := &creationTaskRepository{messages: make(map[[3]string][]byte), deleteErr: test.cleanupErr}
			backend, mock := repository.NewBackendPostgresRepositoryForTest()
			t.Cleanup(func() { require.NoError(t, mock.ExpectationsWereMet()) })
			monitorCtx, stopMonitor := context.WithCancel(context.Background())
			stopMonitor()
			dispatcher, err := task.NewDispatcher(monitorCtx, tasks)
			require.NoError(t, err)

			instanceCtx := context.Background()
			ctx, cancel := context.WithCancel(context.WithValue(instanceCtx, creationContextKey{}, "request-value"))
			defer cancel()
			workspace := &types.Workspace{Id: 1, Name: "workspace"}
			stub := &types.StubWithRelated{Stub: types.Stub{Id: 2, ExternalId: "stub", WorkspaceId: workspace.Id}}
			service := &HttpEndpointService{
				backendRepo: backend, taskDispatcher: dispatcher,
				endpointInstances: common.NewSafeMap[*endpointInstance](),
			}
			// The cached instance outlives the caller, including in the cancellation case.
			service.endpointInstances.Set(stub.ExternalId, &endpointInstance{
				AutoscaledInstance: &abstractions.AutoscaledInstance{Ctx: instanceCtx, Stub: stub},
			})
			dispatcher.Register(string(types.ExecutorEndpoint), service.endpointTaskFactory)
			authInfo := &auth.AuthInfo{Workspace: workspace}
			policy := types.TaskPolicy{Expires: time.Now().Add(time.Minute)}
			other, err := dispatcher.Send(ctx, string(types.ExecutorEndpoint), authInfo, stub.ExternalId, &types.TaskPayload{}, policy)
			require.NoError(t, err)
			otherKey := [3]string{workspace.Name, stub.ExternalId, other.Metadata().TaskId}
			otherMessage := append([]byte(nil), tasks.messages[otherKey]...)
			require.NotEmpty(t, otherMessage)
			target, err := dispatcher.Send(ctx, string(types.ExecutorEndpoint), authInfo, stub.ExternalId, &types.TaskPayload{}, policy)
			require.NoError(t, err)
			targetKey := [3]string{workspace.Name, stub.ExternalId, target.Metadata().TaskId}
			require.NotEqual(t, otherKey, targetKey)
			require.NotEmpty(t, tasks.messages[targetKey])
			require.Len(t, tasks.messages, 2)

			creationErr := errors.New("database temporarily unavailable")
			mock.ExpectQuery("INSERT INTO task").
				WithArgs(targetKey[2], "", workspace.Id, stub.Id).
				WillReturnError(creationErr)
			if test.cancel {
				cancel()
			}
			request := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
			echoCtx := echo.New().NewContext(request, httptest.NewRecorder())
			err = target.Execute(ctx, echoCtx)
			require.ErrorIs(t, err, creationErr)
			require.NotErrorIs(t, err, test.cleanupErr)
			if test.cancel {
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			}
			require.Len(t, tasks.messages, test.wantPending)
			require.Equal(t, otherMessage, tasks.messages[otherKey], "another pending task must remain unchanged")
			_, targetPending := tasks.messages[targetKey]
			require.Equal(t, test.cleanupErr != nil, targetPending)
			require.Equal(t, 1, tasks.calls)
			require.Equal(t, targetKey, tasks.deleted)
			require.NoError(t, tasks.ctxErr)
			require.Equal(t, "request-value", tasks.contextValue)
			require.True(t, tasks.bounded)
			require.Positive(t, tasks.remaining)
			require.LessOrEqual(t, tasks.remaining, 5*time.Second)
		})
	}
}
