package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type imageStubRepository struct {
	BackendRepository
	stubs   map[string]string // stub ID -> workspace ID
	lookups int
}

func (r *imageStubRepository) GetStubByExternalId(_ context.Context, stubID string, filters ...types.QueryFilter) (*types.StubWithRelated, error) {
	r.lookups++
	var stubFilter types.StubFilter
	types.ParseConditionFromQueryFilters(&stubFilter, filters...)
	if workspaceID, ok := r.stubs[stubID]; !ok || workspaceID != stubFilter.WorkspaceID {
		return nil, nil
	}
	return &types.StubWithRelated{Stub: types.Stub{ExternalId: stubID}}, nil
}

func TestAuthorizeImage(t *testing.T) {
	repo := &imageStubRepository{stubs: map[string]string{"stub-a": "workspace-a"}}
	snapshot := types.SnapshotImageID("stub-a", time.Unix(1700000000, 0))

	require.NoError(t, AuthorizeImage(context.Background(), repo, "workspace-a", snapshot))
	require.Error(t, AuthorizeImage(context.Background(), repo, "workspace-b", snapshot))
	require.Error(t, AuthorizeImage(context.Background(), repo, "workspace-a", types.SnapshotImageID("stub-b", time.Unix(1700000000, 0))))

	lookups := repo.lookups
	require.NoError(t, AuthorizeImage(context.Background(), repo, "workspace-b", "0123456789abcdef"))
	require.NoError(t, AuthorizeImage(context.Background(), repo, "workspace-b", ""))
	require.Equal(t, lookups, repo.lookups, "built image IDs need no lookup")
}
