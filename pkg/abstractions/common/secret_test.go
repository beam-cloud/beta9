package abstractions

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConfigureContainerRequestSecretsRejectsMissingSigningKey(t *testing.T) {
	_, err := ConfigureContainerRequestSecrets(nil, types.StubConfigV1{})
	require.EqualError(t, err, "workspace signing key is unavailable")

	_, err = ConfigureContainerRequestSecrets(&types.Workspace{}, types.StubConfigV1{})
	require.EqualError(t, err, "workspace signing key is unavailable")
}
