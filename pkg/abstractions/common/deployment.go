package abstractions

import (
	"context"
	"strconv"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

func ParseAndValidateDeploymentStubId(
	ctx context.Context,
	authInfo *auth.AuthInfo,
	stubId string,
	deploymentName string,
	version string,
	stubType string,
	backendRepo repository.BackendRepository,
) (string, error) {

	if deploymentName != "" {
		var deployment *types.DeploymentWithRelated

		if version == "" {
			var err error
			deployment, err = backendRepo.GetLatestDeploymentByName(ctx, authInfo.Workspace.Id, deploymentName, stubType, true)
			if err != nil {
				return "", apiv1.HTTPBadRequest("Invalid deployment")
			}
		} else {
			version, err := strconv.Atoi(version)
			if err != nil {
				return "", apiv1.HTTPBadRequest("Invalid deployment version")
			}

			deployment, err = backendRepo.GetDeploymentByNameAndVersion(ctx, authInfo.Workspace.Id, deploymentName, uint(version), stubType)
			if err != nil {
				return "", apiv1.HTTPBadRequest("Invalid deployment")
			}
		}

		if deployment == nil {
			return "", apiv1.HTTPBadRequest("Invalid deployment")
		}

		if !deployment.Active {
			return "", apiv1.HTTPBadRequest("Deployment is not active")
		}

		stubId = deployment.Stub.ExternalId
	}

	return stubId, nil
}
