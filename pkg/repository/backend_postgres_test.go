package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

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
