package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestListTaskWithRelated(t *testing.T) {
	// Remove this skip if you are testing on local data
	t.Skip()
	ctx := context.Background()

	dbName := "control_plane"
	dbUser := "pguser"
	connStr := fmt.Sprintf("postgres://%s@localhost:5433/%s?sslmode=disable", dbUser, dbName)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	client := sqlx.NewDb(db, "postgres")
	require.NotNil(t, client)
	r := PostgresBackendRepository{
		client: client,
	}

	workspaceId := uint(0)
	stubId := uuid.New().String()
	firstPageId := uint(0)
	secondPageId := uint(0)

	res, err := r.ListTasksWithRelatedPaginated(ctx, types.TaskFilter{WorkspaceID: workspaceId, StubIds: []string{stubId}})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 10, len(res.Data))
	require.Equal(t, firstPageId, res.Data[0].Id)
	require.NotNil(t, res.Next)

	res, err = r.ListTasksWithRelatedPaginated(ctx, types.TaskFilter{WorkspaceID: workspaceId, StubIds: []string{stubId}, Cursor: res.Next})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 10, len(res.Data))
	require.Equal(t, secondPageId, res.Data[0].Id)
	require.NotNil(t, res.Next)
}

func TestHandleTaskEventUsesProvidedTaskSnapshot(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	taskSnapshot := types.Task{
		ExternalId:  "task-123",
		Status:      types.TaskStatusComplete,
		ContainerId: "container-123",
		WorkspaceId: 1,
		StubId:      2,
	}

	expectTaskWithRelatedQuery(mock, taskSnapshot, types.TaskStatusRunning)

	var eventTask *types.TaskWithRelated
	postgresRepo.handleTaskEvent(taskSnapshot, func(task *types.TaskWithRelated) {
		eventTask = task
	})

	require.NotNil(t, eventTask)
	require.Equal(t, types.TaskStatusComplete, eventTask.Status)
	require.Equal(t, "stub-123", eventTask.Stub.ExternalId)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTaskEventPublisherPreservesPerTaskOrder(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	publisher := newTaskEventPublisher(postgresRepo)

	runningSnapshot := types.Task{
		ExternalId:  "task-123",
		Status:      types.TaskStatusRunning,
		ContainerId: "container-123",
		WorkspaceId: 1,
		StubId:      2,
	}
	completeSnapshot := runningSnapshot
	completeSnapshot.Status = types.TaskStatusComplete

	expectTaskWithRelatedQuery(mock, runningSnapshot, types.TaskStatusRunning)
	expectTaskWithRelatedQuery(mock, completeSnapshot, types.TaskStatusRunning)

	statuses := make(chan types.TaskStatus, 2)
	publisher.enqueue(taskEventJob{
		task: runningSnapshot,
		callback: func(task *types.TaskWithRelated) {
			statuses <- task.Status
		},
	})
	publisher.enqueue(taskEventJob{
		task: completeSnapshot,
		callback: func(task *types.TaskWithRelated) {
			statuses <- task.Status
		},
	})

	require.Equal(t, types.TaskStatusRunning, receiveTaskEventStatus(t, statuses))
	require.Equal(t, types.TaskStatusComplete, receiveTaskEventStatus(t, statuses))
	require.NoError(t, mock.ExpectationsWereMet())
}

func expectTaskWithRelatedQuery(mock sqlmock.Sqlmock, taskSnapshot types.Task, dbStatus types.TaskStatus) {
	mock.ExpectQuery("SELECT").
		WithArgs(taskSnapshot.ExternalId).
		WillReturnRows(sqlmock.NewRows([]string{
			"external_id",
			"status",
			"container_id",
			"workspace_id",
			"stub_id",
			"workspace.external_id",
			"workspace.name",
			"stub.external_id",
			"stub.name",
			"stub.type",
			"app.id",
			"app.external_id",
			"app.name",
		}).AddRow(
			taskSnapshot.ExternalId,
			dbStatus,
			taskSnapshot.ContainerId,
			taskSnapshot.WorkspaceId,
			taskSnapshot.StubId,
			"workspace-123",
			"workspace",
			"stub-123",
			"stub",
			types.StubTypeASGIDeployment,
			3,
			"app-123",
			"app",
		))
}

func receiveTaskEventStatus(t *testing.T, statuses <-chan types.TaskStatus) types.TaskStatus {
	t.Helper()

	select {
	case status := <-statuses:
		return status
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task event")
		return ""
	}
}
