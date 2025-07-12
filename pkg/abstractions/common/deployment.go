package abstractions

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	expirable "github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	deploymentStubCacheSize = 2000
	deploymentStubCacheTTL  = time.Minute * 5
)

func NewDeploymentStubCache() *expirable.LRU[string, string] {
	return expirable.NewLRU[string, string](deploymentStubCacheSize, nil, deploymentStubCacheTTL)
}

func makeCacheKey(authInfo *auth.AuthInfo, stubId, deploymentName, version, stubType string) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s", authInfo.Workspace.ExternalId, stubId, deploymentName, version, stubType)
}

func ParseAndValidateDeploymentStubId(
	ctx context.Context,
	deploymentStubCache *expirable.LRU[string, string],
	authInfo *auth.AuthInfo,
	stubId string,
	deploymentName string,
	version string,
	stubType string,
	backendRepo repository.BackendRepository,
) (string, error) {
	cacheKey := makeCacheKey(authInfo, stubId, deploymentName, version, stubType)

	if cached, ok := deploymentStubCache.Get(cacheKey); ok {
		return cached, nil
	}

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

	if stubId != "" {
		stub, err := backendRepo.GetStubByExternalId(ctx, stubId)
		if stub == nil || err != nil {
			return "", apiv1.HTTPBadRequest("Invalid stub")
		}

		stubConfigRaw := stub.Config
		stubConfig := &types.StubConfigV1{}

		if err := json.Unmarshal([]byte(stubConfigRaw), stubConfig); err != nil {
			return "", apiv1.HTTPBadRequest("Invalid stub")
		}

		if stubConfig.Pricing == nil && stubConfig.Authorized && authInfo.Workspace.ExternalId != stub.Workspace.ExternalId {
			return "", apiv1.HTTPUnauthorized("Invalid token")
		}
	}

	if stubId != "" {
		deploymentStubCache.Add(cacheKey, stubId)
	}

	return stubId, nil
}
