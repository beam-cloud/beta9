package repository

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateConcurrencyLimit(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	workspaceId := uint(1)
	gpuLimit := uint32(4)
	cpuMillicoreLimit := uint32(8000)

	expectedLimit := &types.ConcurrencyLimit{
		Id:                1,
		ExternalId:        "test-external-id",
		WorkspaceId:       workspaceId,
		GPULimit:          gpuLimit,
		CPUMillicoreLimit: cpuMillicoreLimit,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// Mock the INSERT query for creating concurrency limit
	insertQuery := `INSERT INTO concurrency_limit (workspace_id, gpu_limit, cpu_millicore_limit)`
	mock.ExpectQuery(regexp.QuoteMeta(insertQuery)).
		WithArgs(workspaceId, gpuLimit, cpuMillicoreLimit).
		WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "workspace_id", "gpu_limit", "cpu_millicore_limit", "created_at", "updated_at"}).
			AddRow(expectedLimit.Id, expectedLimit.ExternalId, expectedLimit.WorkspaceId, expectedLimit.GPULimit, expectedLimit.CPUMillicoreLimit, expectedLimit.CreatedAt, expectedLimit.UpdatedAt))

	// Mock the UPDATE query for updating workspace
	updateQuery := `UPDATE workspace SET concurrency_limit_id = $1 WHERE id = $2`
	mock.ExpectExec(regexp.QuoteMeta(updateQuery)).
		WithArgs(expectedLimit.Id, workspaceId).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := repo.CreateConcurrencyLimit(ctx, workspaceId, gpuLimit, cpuMillicoreLimit)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedLimit.Id, result.Id)
	assert.Equal(t, expectedLimit.ExternalId, result.ExternalId)
	assert.Equal(t, expectedLimit.WorkspaceId, result.WorkspaceId)
	assert.Equal(t, expectedLimit.GPULimit, result.GPULimit)
	assert.Equal(t, expectedLimit.CPUMillicoreLimit, result.CPUMillicoreLimit)
}

func TestCreateConcurrencyLimit_Error(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	workspaceId := uint(1)
	gpuLimit := uint32(4)
	cpuMillicoreLimit := uint32(8000)

	// Mock the INSERT query to return an error
	insertQuery := `INSERT INTO concurrency_limit (workspace_id, gpu_limit, cpu_millicore_limit)`
	mock.ExpectQuery(regexp.QuoteMeta(insertQuery)).
		WithArgs(workspaceId, gpuLimit, cpuMillicoreLimit).
		WillReturnError(fmt.Errorf("database error"))

	result, err := repo.CreateConcurrencyLimit(ctx, workspaceId, gpuLimit, cpuMillicoreLimit)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database error")
}

func TestUpdateConcurrencyLimit(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	concurrencyLimitId := uint(1)
	gpuLimit := uint32(8)
	cpuMillicoreLimit := uint32(16000)

	expectedLimit := &types.ConcurrencyLimit{
		Id:                concurrencyLimitId,
		GPULimit:          gpuLimit,
		CPUMillicoreLimit: cpuMillicoreLimit,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// Mock the UPDATE query
	updateQuery := `UPDATE concurrency_limit SET gpu_limit = $2, cpu_millicore_limit = $3, updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	mock.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(concurrencyLimitId, gpuLimit, cpuMillicoreLimit).
		WillReturnRows(sqlmock.NewRows([]string{"id", "gpu_limit", "cpu_millicore_limit", "created_at", "updated_at"}).
			AddRow(expectedLimit.Id, expectedLimit.GPULimit, expectedLimit.CPUMillicoreLimit, expectedLimit.CreatedAt, expectedLimit.UpdatedAt))

	result, err := repo.UpdateConcurrencyLimit(ctx, concurrencyLimitId, gpuLimit, cpuMillicoreLimit)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedLimit.Id, result.Id)
	assert.Equal(t, expectedLimit.GPULimit, result.GPULimit)
	assert.Equal(t, expectedLimit.CPUMillicoreLimit, result.CPUMillicoreLimit)
}

func TestGetConcurrencyLimit(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	concurrencyLimitId := uint(1)

	expectedLimit := &types.ConcurrencyLimit{
		Id:                concurrencyLimitId,
		ExternalId:        "test-external-id",
		WorkspaceId:       uint(1),
		GPULimit:          uint32(4),
		CPUMillicoreLimit: uint32(8000),
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// Mock the SELECT query
	selectQuery := `SELECT gpu_limit, cpu_millicore_limit, created_at, updated_at FROM concurrency_limit WHERE id = $1;`
	mock.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(concurrencyLimitId).
		WillReturnRows(sqlmock.NewRows([]string{"gpu_limit", "cpu_millicore_limit", "created_at", "updated_at"}).
			AddRow(expectedLimit.GPULimit, expectedLimit.CPUMillicoreLimit, expectedLimit.CreatedAt, expectedLimit.UpdatedAt))

	result, err := repo.GetConcurrencyLimit(ctx, concurrencyLimitId)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedLimit.GPULimit, result.GPULimit)
	assert.Equal(t, expectedLimit.CPUMillicoreLimit, result.CPUMillicoreLimit)
}

func TestListConcurrencyLimitsByWorkspaceId(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	workspaceId := "test-workspace-id"

	expectedLimits := []types.ConcurrencyLimit{
		{
			Id:                1,
			ExternalId:        "limit-1",
			WorkspaceId:       1,
			GPULimit:          4,
			CPUMillicoreLimit: 8000,
			CreatedAt:         time.Now(),
			UpdatedAt:         time.Now(),
		},
		{
			Id:                2,
			ExternalId:        "limit-2",
			WorkspaceId:       1,
			GPULimit:          8,
			CPUMillicoreLimit: 16000,
			CreatedAt:         time.Now().Add(-time.Hour),
			UpdatedAt:         time.Now().Add(-time.Hour),
		},
	}

	// Mock the SELECT query
	selectQuery := `SELECT cl.id, cl.external_id, cl.workspace_id, cl.gpu_limit, cl.cpu_millicore_limit, cl.created_at, cl.updated_at`
	mock.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(workspaceId).
		WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "workspace_id", "gpu_limit", "cpu_millicore_limit", "created_at", "updated_at"}).
			AddRow(expectedLimits[0].Id, expectedLimits[0].ExternalId, expectedLimits[0].WorkspaceId, expectedLimits[0].GPULimit, expectedLimits[0].CPUMillicoreLimit, expectedLimits[0].CreatedAt, expectedLimits[0].UpdatedAt).
			AddRow(expectedLimits[1].Id, expectedLimits[1].ExternalId, expectedLimits[1].WorkspaceId, expectedLimits[1].GPULimit, expectedLimits[1].CPUMillicoreLimit, expectedLimits[1].CreatedAt, expectedLimits[1].UpdatedAt))

	result, err := repo.ListConcurrencyLimitsByWorkspaceId(ctx, workspaceId)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result, 2)
	assert.Equal(t, expectedLimits[0].Id, result[0].Id)
	assert.Equal(t, expectedLimits[1].Id, result[1].Id)
}

func TestDeleteConcurrencyLimit(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	workspace := types.Workspace{
		Id:                 1,
		ConcurrencyLimitId: func() *uint { id := uint(2); return &id }(),
	}

	// Mock the transaction
	mock.ExpectBegin()

	// Mock the UPDATE workspace query
	updateQuery := `UPDATE workspace SET concurrency_limit_id = NULL WHERE id = $1`
	mock.ExpectExec(regexp.QuoteMeta(updateQuery)).
		WithArgs(workspace.Id).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock the DELETE concurrency_limit query
	deleteQuery := `DELETE FROM concurrency_limit WHERE id = $1`
	mock.ExpectExec(regexp.QuoteMeta(deleteQuery)).
		WithArgs(*workspace.ConcurrencyLimitId).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock the commit
	mock.ExpectCommit()

	err := repo.DeleteConcurrencyLimit(ctx, workspace)

	require.NoError(t, err)
}

func TestRevertConcurrencyLimit(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	workspaceId := "test-workspace-id"
	concurrencyLimitId := "test-limit-id"

	expectedLimit := &types.ConcurrencyLimit{
		Id:                1,
		ExternalId:        concurrencyLimitId,
		WorkspaceId:       1,
		GPULimit:          4,
		CPUMillicoreLimit: 8000,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// Mock the count query to verify ownership
	countQuery := `SELECT COUNT(id)`
	mock.ExpectQuery(regexp.QuoteMeta(countQuery)).
		WithArgs(workspaceId, concurrencyLimitId).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// Mock the update query
	updateQuery := `UPDATE workspace SET concurrency_limit_id = $1 WHERE external_id = $2`
	mock.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(concurrencyLimitId, workspaceId).
		WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "workspace_id", "gpu_limit", "cpu_millicore_limit", "created_at", "updated_at"}).
			AddRow(expectedLimit.Id, expectedLimit.ExternalId, expectedLimit.WorkspaceId, expectedLimit.GPULimit, expectedLimit.CPUMillicoreLimit, expectedLimit.CreatedAt, expectedLimit.UpdatedAt))

	result, err := repo.RevertToConcurrencyLimit(ctx, workspaceId, concurrencyLimitId)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedLimit.Id, result.Id)
	assert.Equal(t, expectedLimit.ExternalId, result.ExternalId)
}

func TestRevertConcurrencyLimit_NotOwned(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	defer mock.ExpectationsWereMet()

	ctx := context.Background()
	workspaceId := "test-workspace-id"
	concurrencyLimitId := "test-limit-id"

	// Mock the count query to return 0 (not owned)
	countQuery := `SELECT COUNT(id)`
	mock.ExpectQuery(regexp.QuoteMeta(countQuery)).
		WithArgs(workspaceId, concurrencyLimitId).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	result, err := repo.RevertToConcurrencyLimit(ctx, workspaceId, concurrencyLimitId)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "does not belong to workspace")
}
