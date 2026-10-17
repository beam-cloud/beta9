package secret

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type secretBackend struct {
	repository.BackendRepository
	getSecretByNameDecrypted func(context.Context, *types.Workspace, string) (*types.Secret, error)
}

func (b *secretBackend) GetSecretByNameDecrypted(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error) {
	return b.getSecretByNameDecrypted(ctx, workspace, name)
}

func TestGetSecretRejectsUnauthorizedAccess(t *testing.T) {
	for _, tc := range []struct {
		name     string
		authInfo *auth.AuthInfo
	}{
		{
			name: "restricted token",
			authInfo: &auth.AuthInfo{
				Workspace: &types.Workspace{Id: 1},
				Token:     &types.Token{TokenType: types.TokenTypeWorkspaceRestricted},
			},
		},
		{name: "missing auth info"},
		{name: "missing token", authInfo: &auth.AuthInfo{Workspace: &types.Workspace{Id: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &WorkspaceSecretService{backendRepo: &secretBackend{
				getSecretByNameDecrypted: func(context.Context, *types.Workspace, string) (*types.Secret, error) {
					t.Fatal("unauthorized request must not look up or decrypt secrets")
					return nil, nil
				},
			}}
			ctx := context.Background()
			if tc.authInfo != nil {
				ctx = auth.ContextWithAuthInfo(ctx, tc.authInfo)
			}

			response, err := service.GetSecret(ctx, &pb.GetSecretRequest{Name: "WORKSPACE_SECRET"})

			require.NoError(t, err)
			require.False(t, response.Ok)
			require.Equal(t, "Unauthorized Access", response.ErrMsg)
			require.Nil(t, response.Secret)
		})
	}
}

func TestGetSecretAllowsPrimaryToken(t *testing.T) {
	workspace := &types.Workspace{Id: 1}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: workspace,
		Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
	})
	secret := &types.Secret{
		Name:        "WORKSPACE_SECRET",
		Value:       "secret-value",
		Hosts:       []string{"api.example.com"},
		Placeholder: "beam_secret_a",
		CreatedAt:   time.Unix(1700000000, 0).UTC(),
		UpdatedAt:   time.Unix(1700000060, 0).UTC(),
	}
	lookupCalls := 0
	service := &WorkspaceSecretService{backendRepo: &secretBackend{
		getSecretByNameDecrypted: func(gotCtx context.Context, gotWorkspace *types.Workspace, name string) (*types.Secret, error) {
			lookupCalls++
			require.Equal(t, ctx, gotCtx)
			require.Same(t, workspace, gotWorkspace)
			require.Equal(t, secret.Name, name)
			return secret, nil
		},
	}}

	response, err := service.GetSecret(ctx, &pb.GetSecretRequest{Name: secret.Name})

	require.NoError(t, err)
	require.Equal(t, 1, lookupCalls)
	require.True(t, response.Ok)
	require.Empty(t, response.ErrMsg)
	require.NotNil(t, response.Secret)
	require.Equal(t, secret.Name, response.Secret.Name)
	require.Equal(t, secret.Value, response.Secret.Value)
	require.Equal(t, []string(secret.Hosts), response.Secret.Hosts)
	require.Equal(t, secret.Placeholder, response.Secret.Placeholder)
	require.Equal(t, secret.CreatedAt, response.Secret.CreatedAt.AsTime())
	require.Equal(t, secret.UpdatedAt, response.Secret.UpdatedAt.AsTime())
}
