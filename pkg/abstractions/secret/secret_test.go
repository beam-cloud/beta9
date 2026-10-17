package secret

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type secretBackend struct {
	repository.BackendRepository
	called bool
}

func (b *secretBackend) GetSecretByNameDecrypted(context.Context, *types.Workspace, string) (*types.Secret, error) {
	b.called = true
	return &types.Secret{Value: "secret-value"}, nil
}

func TestGetSecretPermission(t *testing.T) {
	for _, tokenType := range []string{types.TokenTypeWorkspaceRestricted, types.TokenTypeWorkspacePrimary} {
		t.Run(tokenType, func(t *testing.T) {
			repo := &secretBackend{}
			service := &WorkspaceSecretService{backendRepo: repo}
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
				Workspace: &types.Workspace{Id: 1},
				Token:     &types.Token{TokenType: tokenType},
			})

			response, err := service.GetSecret(ctx, &pb.GetSecretRequest{Name: "WORKSPACE_SECRET"})
			require.NoError(t, err)
			allowed := tokenType == types.TokenTypeWorkspacePrimary
			require.Equal(t, allowed, response.Ok)
			require.Equal(t, allowed, repo.called)
			if allowed {
				require.Empty(t, response.ErrMsg)
				require.NotNil(t, response.Secret)
				require.Equal(t, "secret-value", response.Secret.Value)
			} else {
				require.Equal(t, "Unauthorized Access", response.ErrMsg)
				require.Nil(t, response.Secret)
			}
		})
	}
}
