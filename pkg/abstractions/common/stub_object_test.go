package abstractions

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type emptyObjectRepo struct {
	repository.BackendRepository
	t           *testing.T
	object      *types.Object
	createErr   error
	fetchErr    error
	createCount int
}

func (r *emptyObjectRepo) GetObjectByHash(_ context.Context, hash string, workspaceID uint) (*types.Object, error) {
	require.Equal(r.t, EmptyStubObjectHash(), hash)
	require.Equal(r.t, uint(42), workspaceID)
	if r.fetchErr != nil {
		return nil, r.fetchErr
	}
	if r.object == nil {
		return nil, sql.ErrNoRows
	}
	return r.object, nil
}

func (r *emptyObjectRepo) CreateObject(_ context.Context, hash string, size int64, workspaceID uint) (*types.Object, error) {
	r.createCount++
	require.Equal(r.t, EmptyStubObjectHash(), hash)
	require.Equal(r.t, int64(22), size)
	require.Equal(r.t, uint(42), workspaceID)
	r.object = &types.Object{Id: 7, ExternalId: "empty-context", Hash: hash, Size: size, WorkspaceId: workspaceID}
	return r.object, r.createErr
}

func TestEmptySandboxContextDoesNotRequireObjectStorage(t *testing.T) {
	repo := &emptyObjectRepo{t: t}
	workspace := &types.Workspace{Id: 42, Name: "workspace"}
	for i := 0; i < 2; i++ {
		object, err := GetOrCreateEmptyStubObject(context.Background(), repo, workspace)
		require.NoError(t, err)
		require.True(t, IsEmptyStubObject(object))
		require.Equal(t, uint(42), object.WorkspaceId)
	}
	require.Equal(t, 1, repo.createCount)
}

func TestEmptyContextCreationRaceAndStorageErrors(t *testing.T) {
	repo := &emptyObjectRepo{t: t, createErr: errors.New("concurrent insert")}
	object, err := GetOrCreateEmptyStubObject(context.Background(), repo, &types.Workspace{Id: 42})
	require.NoError(t, err)
	require.Equal(t, "empty-context", object.ExternalId)
	repo.fetchErr = errors.New("database unavailable")
	_, err = GetOrCreateEmptyStubObject(context.Background(), repo, &types.Workspace{Id: 42})
	require.ErrorIs(t, err, repo.fetchErr)
}
