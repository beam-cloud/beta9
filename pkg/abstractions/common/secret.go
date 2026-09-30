package abstractions

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

func ConfigureContainerRequestSecrets(
	ctx context.Context,
	backend repository.BackendRepository,
	workspace *types.Workspace,
	stubConfig types.StubConfigV1,
) ([]string, error) {
	if len(stubConfig.Secrets) == 0 {
		return []string{}, nil
	}

	names := make([]string, 0, len(stubConfig.Secrets))
	for _, binding := range stubConfig.Secrets {
		names = append(names, binding.Name)
	}

	// Resolve at launch so rotation cannot race an instance's cached config.
	secrets, err := backend.GetSecretsByNameDecrypted(ctx, workspace, names)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string, len(secrets))
	for _, secret := range secrets {
		values[secret.Name] = secret.Value
	}

	secretEnv := make([]string, 0, len(stubConfig.Secrets))
	for _, binding := range stubConfig.Secrets {
		value, ok := values[binding.Name]
		if !ok {
			return nil, fmt.Errorf("secret %q no longer exists", binding.Name)
		}
		secretEnv = append(secretEnv, fmt.Sprintf("%s=%s", binding.EnvVarName(), value))
	}

	return secretEnv, nil
}
