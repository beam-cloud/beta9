package managedendpoint

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type adminWorkspaceBackend struct {
	repository.BackendRepository
	admin        *types.Workspace
	full         *types.Workspace
	fullErr      error
	key          string
	adminCalls   int
	workspaceIDs []uint
	keyIDs       []string
}

func (b *adminWorkspaceBackend) GetAdminWorkspace(context.Context) (*types.Workspace, error) {
	b.adminCalls++
	return b.admin, nil
}

func (b *adminWorkspaceBackend) GetWorkspace(_ context.Context, id uint) (*types.Workspace, error) {
	b.workspaceIDs = append(b.workspaceIDs, id)
	return b.full, b.fullErr
}

func (b *adminWorkspaceBackend) GetWorkspaceByExternalIdWithSigningKey(_ context.Context, id string) (types.Workspace, error) {
	b.keyIDs = append(b.keyIDs, id)
	// This lookup intentionally has no external ID or storage metadata.
	return types.Workspace{SigningKey: &b.key}, nil
}

func storedAdminWorkspace() *types.Workspace {
	id := uint(9)
	bucket, endpoint, region := "existing-custom-models", "https://storage.example.invalid", "us-east-1"
	access, secret := "decrypted-test-access", "decrypted-test-secret"
	return &types.Workspace{
		Id: 7, ExternalId: "admin-workspace", Name: "admin", VolumeCacheEnabled: true, MultiGpuEnabled: true,
		Storage: &types.WorkspaceStorage{
			Id: &id, BucketName: &bucket, EndpointUrl: &endpoint, Region: &region,
			AccessKey: &access, SecretKey: &secret,
		},
	}
}

func TestAdminWorkspaceRequiresStorageAndRecoversAfterAttachment(t *testing.T) {
	bare := &types.Workspace{Id: 7, ExternalId: "admin-workspace", Name: "admin"}
	backend := &adminWorkspaceBackend{admin: bare, full: bare, key: "test-signing-key"}
	s := &Service{backend: backend}

	workspace, err := s.AdminWorkspace(context.Background())
	require.ErrorContains(t, err, "require workspace storage")
	require.ErrorContains(t, err, "migrate existing objects and volumes")
	require.Nil(t, workspace)
	require.Nil(t, s.adminWorkspace, "a missing storage association must remain retryable")
	require.Empty(t, backend.keyIDs, "reject storage before loading unrelated credentials")

	// The repository's cached admin pointer remains bare after an operator
	// attaches storage. Only the full database lookup observes the migration.
	backend.full = storedAdminWorkspace()
	workspace, err = s.AdminWorkspace(context.Background())
	require.NoError(t, err)
	require.True(t, workspace.StorageAvailable())
	require.Equal(t, "admin-workspace", workspace.ExternalId)
	require.Equal(t, "admin", workspace.Name)
	require.True(t, workspace.VolumeCacheEnabled)
	require.True(t, workspace.MultiGpuEnabled)
	require.Equal(t, backend.full.Storage, workspace.Storage, "keep the full decrypted storage record")
	require.Equal(t, "existing-custom-models", *workspace.Storage.BucketName)
	require.Equal(t, "decrypted-test-secret", *workspace.Storage.SecretKey)
	require.Equal(t, backend.key, *workspace.SigningKey)
	require.True(t, (&types.ContainerRequest{Workspace: *workspace}).StorageAvailable())
	require.False(t, bare.StorageAvailable(), "do not mutate the repository's shared cached pointer")
	require.Nil(t, backend.full.SigningKey, "copy the record before filling in the signing key")
	require.Equal(t, []uint{7, 7}, backend.workspaceIDs)
	require.Equal(t, []string{"admin-workspace"}, backend.keyIDs)

	cached, err := s.AdminWorkspace(context.Background())
	require.NoError(t, err)
	require.Same(t, workspace, cached)
	require.Equal(t, 2, backend.adminCalls, "the steady path must not add backend reads")
	require.Len(t, backend.workspaceIDs, 2)
	require.Len(t, backend.keyIDs, 1)
}

func TestAdminWorkspacePreservesExistingStorage(t *testing.T) {
	for _, hasKey := range []bool{false, true} {
		name := "load signing key"
		if hasKey {
			name = "already has signing key"
		}
		t.Run(name, func(t *testing.T) {
			admin := storedAdminWorkspace()
			backend := &adminWorkspaceBackend{admin: admin, key: "test-signing-key"}
			if hasKey {
				admin.SigningKey = &backend.key
			}
			s := &Service{backend: backend}
			workspace, err := s.AdminWorkspace(context.Background())
			require.NoError(t, err)
			require.Equal(t, admin.Storage, workspace.Storage)
			require.Equal(t, "https://storage.example.invalid", *workspace.Storage.EndpointUrl)
			require.Equal(t, "decrypted-test-access", *workspace.Storage.AccessKey)
			require.Equal(t, "decrypted-test-secret", *workspace.Storage.SecretKey)
			require.Equal(t, "admin-workspace", workspace.ExternalId)
			require.Equal(t, backend.key, *workspace.SigningKey)
			require.Empty(t, backend.workspaceIDs, "existing custom storage does not need a refresh")
			if hasKey {
				require.Empty(t, backend.keyIDs)
			} else {
				require.Equal(t, []string{"admin-workspace"}, backend.keyIDs)
				require.Nil(t, admin.SigningKey)
			}
			cached, err := s.AdminWorkspace(context.Background())
			require.NoError(t, err)
			require.Same(t, workspace, cached)
			require.Equal(t, 1, backend.adminCalls)
		})
	}
}

func TestAdminWorkspaceStorageRefreshFailureIsRetryable(t *testing.T) {
	for _, lookupErr := range []error{errors.New("database unavailable"), context.Canceled, nil} {
		name := "missing workspace"
		if lookupErr != nil {
			name = lookupErr.Error()
		}
		t.Run(name, func(t *testing.T) {
			backend := &adminWorkspaceBackend{
				admin: &types.Workspace{Id: 7, ExternalId: "admin-workspace"}, fullErr: lookupErr, key: "test-signing-key",
			}
			s := &Service{backend: backend}
			workspace, err := s.AdminWorkspace(context.Background())
			require.Error(t, err)
			if lookupErr != nil {
				require.ErrorIs(t, err, lookupErr)
			}
			require.Nil(t, workspace)
			require.Nil(t, s.adminWorkspace)
			require.Empty(t, backend.keyIDs)

			backend.full, backend.fullErr = storedAdminWorkspace(), nil
			workspace, err = s.AdminWorkspace(context.Background())
			require.NoError(t, err)
			require.True(t, workspace.StorageAvailable())
			require.Equal(t, []uint{7, 7}, backend.workspaceIDs)
		})
	}
}
