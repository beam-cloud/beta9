package abstractions

import (
	"fmt"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func ConfigureContainerRequestSecrets(
	workspace *types.Workspace,
	stubConfig types.StubConfigV1,
) ([]string, error) {
	secretKey, err := common.ParseSecretKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	secretEnv := []string{}
	for _, secret := range stubConfig.Secrets {
		secretValue, err := common.Decrypt(secretKey, secret.Value)
		if err != nil {
			return nil, err
		}

		secretEnv = append(
			secretEnv,
			fmt.Sprintf("%s=%s", secret.Name, secretValue),
		)
	}

	return secretEnv, nil
}
