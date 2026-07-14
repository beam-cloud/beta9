package repository

import (
	"context"
	"database/sql"
	"errors"
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

func TestGetAdminWorkspaceMapsExternalID(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	createdAt := time.Now().UTC()
	mock.ExpectQuery(`SELECT w\.id, w\.external_id`).WillReturnRows(sqlmock.NewRows([]string{
		"id", "external_id", "name", "created_at", "concurrency_limit_id", "volume_cache_enabled", "multi_gpu_enabled",
	}).AddRow(uint(7), "admin-workspace", "Admin", createdAt, nil, true, true))

	workspace, err := repo.GetAdminWorkspace(context.Background())
	require.NoError(t, err)
	require.Equal(t, "admin-workspace", workspace.ExternalId)
	require.Equal(t, uint(7), workspace.Id)
	require.True(t, workspace.VolumeCacheEnabled)
	require.True(t, workspace.MultiGpuEnabled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func waitForAdminWorkspaceLoad(t *testing.T, repo *PostgresBackendRepository) {
	t.Helper()
	require.Eventually(t, func() bool {
		repo.adminWorkspaceMu.Lock()
		defer repo.adminWorkspaceMu.Unlock()
		return repo.adminWorkspaceLoading != nil
	}, time.Second, time.Millisecond, "admin workspace load did not start")
}

func TestGetAdminWorkspaceCanceledCallerDoesNotWaitForLoad(t *testing.T) {
	repository, mock := NewBackendPostgresRepositoryForTest()
	repo := repository.(*PostgresBackendRepository)
	createdAt := time.Now().UTC()
	mock.ExpectQuery(`SELECT w\.id, w\.external_id`).
		WillDelayFor(200 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "name", "created_at", "concurrency_limit_id", "volume_cache_enabled", "multi_gpu_enabled",
		}).AddRow(uint(7), "admin-workspace", "Admin", createdAt, nil, true, true))

	firstResult := make(chan error, 1)
	go func() {
		_, err := repo.GetAdminWorkspace(context.Background())
		firstResult <- err
	}()

	waitForAdminWorkspaceLoad(t, repo)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := repo.GetAdminWorkspace(ctx)
	require.True(t, errors.Is(err, context.Canceled), "error = %v", err)
	require.NoError(t, <-firstResult)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetAdminWorkspaceSharesFailedLoadWithWaiters(t *testing.T) {
	repository, mock := NewBackendPostgresRepositoryForTest()
	repo := repository.(*PostgresBackendRepository)
	loadErr := errors.New("database unavailable")
	mock.ExpectQuery(`SELECT w\.id, w\.external_id`).
		WillDelayFor(200 * time.Millisecond).
		WillReturnError(loadErr)

	const waiterCount = 8
	results := make(chan error, waiterCount+1)
	go func() {
		_, err := repo.GetAdminWorkspace(context.Background())
		results <- err
	}()

	waitForAdminWorkspaceLoad(t, repo)

	for range waiterCount {
		go func() {
			_, err := repo.GetAdminWorkspace(context.Background())
			results <- err
		}()
	}

	for range waiterCount + 1 {
		require.ErrorIs(t, <-results, loadErr)
	}

	createdAt := time.Now().UTC()
	mock.ExpectQuery(`SELECT w\.id, w\.external_id`).WillReturnRows(sqlmock.NewRows([]string{
		"id", "external_id", "name", "created_at", "concurrency_limit_id", "volume_cache_enabled", "multi_gpu_enabled",
	}).AddRow(uint(7), "admin-workspace", "Admin", createdAt, nil, true, true))
	workspace, err := repo.GetAdminWorkspace(context.Background())
	require.NoError(t, err)
	require.Equal(t, "admin-workspace", workspace.ExternalId)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetAdminWorkspaceRetriesCanceledSharedLoad(t *testing.T) {
	repository, mock := NewBackendPostgresRepositoryForTest()
	repo := repository.(*PostgresBackendRepository)
	loading := &adminWorkspaceLoad{done: make(chan struct{})}
	repo.adminWorkspaceLoading = loading

	createdAt := time.Now().UTC()
	mock.ExpectQuery(`SELECT w\.id, w\.external_id`).WillReturnRows(sqlmock.NewRows([]string{
		"id", "external_id", "name", "created_at", "concurrency_limit_id", "volume_cache_enabled", "multi_gpu_enabled",
	}).AddRow(uint(7), "admin-workspace", "Admin", createdAt, nil, true, true))

	go func() {
		time.Sleep(10 * time.Millisecond)
		repo.adminWorkspaceMu.Lock()
		loading.err = context.Canceled
		repo.adminWorkspaceLoading = nil
		close(loading.done)
		repo.adminWorkspaceMu.Unlock()
	}()

	workspace, err := repo.GetAdminWorkspace(context.Background())
	require.NoError(t, err)
	require.Equal(t, "admin-workspace", workspace.ExternalId)
	require.NoError(t, mock.ExpectationsWereMet())
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

func TestTaskEventPublisherPreservesOrder(t *testing.T) {
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

func TestUpdateDeploymentDoesNotMatchDeletedRowsByNumericID(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	updatedAt := time.Now()
	mock.ExpectQuery(`WHERE \(id = \$1 OR external_id = \$2\) AND deleted_at IS NULL`).
		WithArgs(uint(10), "deployment-id", "service-probe", false, uint(1)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id",
			"external_id",
			"name",
			"active",
			"version",
			"workspace_id",
			"stub_id",
			"stub_type",
			"created_at",
			"updated_at",
		}).AddRow(
			uint(10),
			"deployment-id",
			"service-probe",
			false,
			uint(1),
			uint(1),
			uint(20),
			string(types.StubTypePodDeployment),
			updatedAt,
			updatedAt,
		))

	deployment, err := postgresRepo.UpdateDeployment(context.Background(), types.Deployment{
		Id:         10,
		ExternalId: "deployment-id",
		Name:       "service-probe",
		Active:     false,
		Version:    1,
	})

	require.NoError(t, err)
	require.False(t, deployment.Active)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetOrCreateStubTouchesExistingWorkspaceScopedStub(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	createdAt := time.Now().Add(-time.Hour)
	updatedAt := time.Now()
	stubRows := []string{
		"id",
		"external_id",
		"name",
		"type",
		"config",
		"config_version",
		"object_id",
		"workspace_id",
		"created_at",
		"updated_at",
		"app_id",
	}

	mock.ExpectQuery("FROM stub").
		WithArgs("stub-name", string(types.StubTypeFunction), uint(7), sqlmock.AnyArg(), uint(11)).
		WillReturnRows(sqlmock.NewRows(stubRows).AddRow(
			uint(5),
			"stub-external",
			"stub-name",
			types.StubTypeFunction,
			`{"runtime":"python3"}`,
			uint(1),
			uint(7),
			uint(11),
			createdAt,
			createdAt,
			uint(13),
		))
	mock.ExpectQuery("UPDATE stub").
		WithArgs(uint(5)).
		WillReturnRows(sqlmock.NewRows(stubRows).AddRow(
			uint(5),
			"stub-external",
			"stub-name",
			types.StubTypeFunction,
			`{"runtime":"python3"}`,
			uint(1),
			uint(7),
			uint(11),
			createdAt,
			updatedAt,
			uint(13),
		))
	mock.ExpectExec("UPDATE app set updated_at=NOW").
		WithArgs(uint(13)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub, err := postgresRepo.GetOrCreateStub(
		context.Background(),
		"stub-name",
		string(types.StubTypeFunction),
		types.StubConfigV1{},
		7,
		11,
		false,
		13,
	)

	require.NoError(t, err)
	require.Equal(t, uint(5), stub.Id)
	require.Equal(t, uint(11), stub.WorkspaceId)
	require.True(t, stub.UpdatedAt.Time.Equal(updatedAt))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListStaleCheckpointsRequiresStubUpdatedBeforeCutoff(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	cutoff := time.Now().Add(-7 * 24 * time.Hour)

	mock.ExpectQuery(`s\.updated_at < \$2`).
		WithArgs(sqlmock.AnyArg(), cutoff).
		WillReturnRows(sqlmock.NewRows([]string{
			"checkpoint_id",
			"external_id",
			"source_container_id",
			"container_ip",
			"status",
			"remote_key",
			"workspace_id",
			"stub_id",
			"stub_type",
			"app_id",
			"exposed_ports",
			"created_at",
			"last_restored_at",
			"cache_hash",
			"cache_size_bytes",
			"origin_key",
			"locality",
			"accelerator",
		}).AddRow(
			"checkpoint-123",
			"external-123",
			"sandbox-stub-123-container",
			"10.0.0.12",
			string(types.CheckpointStatusAvailable),
			"checkpoint-123",
			uint(1),
			uint(2),
			types.StubTypeSandbox,
			uint(3),
			"{8080}",
			cutoff.Add(-time.Hour),
			cutoff.Add(-30*time.Minute),
			"sha256-cache",
			int64(128),
			"checkpoints/checkpoint-123.tar",
			"default",
			"cpu",
		))

	checkpoints, err := postgresRepo.ListStaleCheckpoints(context.Background(), []string{"workspace|active-stub"}, cutoff)

	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	require.Equal(t, "checkpoint-123", checkpoints[0].CheckpointId)
	require.Equal(t, []uint32{8080}, checkpoints[0].ExposedPorts)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetLatestCheckpointByStubIdOnlyReturnsAvailable(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	createdAt := time.Now().Add(-time.Minute)
	restoredAt := time.Now()

	mock.ExpectQuery(`c\.status = \$2`).
		WithArgs("stub-123", string(types.CheckpointStatusAvailable)).
		WillReturnRows(sqlmock.NewRows([]string{
			"checkpoint_id",
			"external_id",
			"source_container_id",
			"container_ip",
			"status",
			"remote_key",
			"workspace_id",
			"stub_id",
			"stub_type",
			"app_id",
			"exposed_ports",
			"created_at",
			"last_restored_at",
			"cache_hash",
			"cache_size_bytes",
			"origin_key",
			"locality",
			"accelerator",
		}).AddRow(
			"checkpoint-available",
			"external-available",
			"container-available",
			"10.0.0.12",
			string(types.CheckpointStatusAvailable),
			"checkpoint-available",
			uint(1),
			uint(2),
			types.StubTypeASGI,
			uint(3),
			"{8001}",
			createdAt,
			restoredAt,
			"sha256-cache",
			int64(128),
			"checkpoints/checkpoint-available.tar",
			"default",
			"cpu",
		))

	checkpoint, err := postgresRepo.GetLatestCheckpointByStubId(context.Background(), "stub-123")

	require.NoError(t, err)
	require.Equal(t, "checkpoint-available", checkpoint.CheckpointId)
	require.Equal(t, string(types.CheckpointStatusAvailable), checkpoint.Status)
	require.Equal(t, []uint32{8001}, checkpoint.ExposedPorts)
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
