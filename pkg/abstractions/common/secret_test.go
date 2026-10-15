package abstractions

import (
	"context"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
)

type secretBackend struct {
	repository.BackendRepository
	secret types.Secret
}

func (b secretBackend) GetSecretsByNameDecrypted(context.Context, *types.Workspace, []string) ([]types.Secret, error) {
	return []types.Secret{b.secret}, nil
}
func TestContainerSecretInjection(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		brokered, hosts, invalid bool
	}{
		{"serverless", false, false, false},
		{"serverless with approved hosts", false, true, false},
		{"sandbox", true, true, false},
		{"VM", true, true, false},
		{"missing hosts", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			secret := types.Secret{Name: "KEY", Value: "real-value", Placeholder: "beam_secret_a"}
			if tc.hosts {
				secret.Hosts = []string{"api.example.com"}
			}
			result, err := ConfigureContainerRequestSecrets(context.Background(), secretBackend{secret: secret}, &types.Workspace{}, types.StubConfigV1{Secrets: []types.Secret{{Name: "KEY", EnvName: "ALIAS"}}}, tc.brokered)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.brokered {
				require.Equal(t, []string{"ALIAS=beam_secret_a"}, result.Env)
				require.Equal(t, "real-value", result.Bindings[0].Value)
			} else {
				require.Equal(t, []string{"ALIAS=real-value"}, result.Env)
				require.Empty(t, result.Bindings)
			}
		})
	}
}
