package abstractions

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerSecrets struct {
	Env      []string
	Bindings []types.SecretBinding
}

func ConfigureContainerRequestSecrets(
	ctx context.Context,
	backend repository.BackendRepository,
	workspace *types.Workspace,
	stubConfig types.StubConfigV1,
	brokered bool,
) (ContainerSecrets, error) {
	if len(stubConfig.Secrets) == 0 {
		return ContainerSecrets{}, nil
	}

	names := make([]string, 0, len(stubConfig.Secrets))
	for _, binding := range stubConfig.Secrets {
		names = append(names, binding.Name)
	}

	// Resolve at launch so rotation cannot race an instance's cached config.
	secrets, err := backend.GetSecretsByNameDecrypted(ctx, workspace, names)
	if err != nil {
		return ContainerSecrets{}, err
	}

	if brokered {
		if err := common.ValidateSandboxSecrets(secrets, stubConfig.AllowList); err != nil {
			return ContainerSecrets{}, err
		}
	}
	values := make(map[string]types.Secret, len(secrets))
	for _, secret := range secrets {
		values[secret.Name] = secret
	}

	result := ContainerSecrets{Env: make([]string, 0, len(stubConfig.Secrets))}
	for _, binding := range stubConfig.Secrets {
		secret, ok := values[binding.Name]
		if !ok {
			return ContainerSecrets{}, fmt.Errorf("secret %q no longer exists", binding.Name)
		}
		value := secret.Value
		if brokered && secret.Name != "BETA9_TOKEN" && secret.Name != "BEAM_TOKEN" {
			result.Bindings = append(result.Bindings, types.SecretBinding{EnvName: binding.EnvVarName(), SecretName: secret.Name, Placeholder: secret.Placeholder, Hosts: secret.Hosts, Value: secret.Value})
			value = secret.Placeholder
		}
		result.Env = append(result.Env, fmt.Sprintf("%s=%s", binding.EnvVarName(), value))
	}

	return result, nil
}
