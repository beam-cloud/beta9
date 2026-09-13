package gatewayservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// hostedKinds are the deployments the fleet can run replicas of: model
// servers and the long-running execution services (endpoint, ASGI, task queue).
var hostedKinds = []string{types.StubTypeManagedEndpoint, types.StubTypeEndpoint, types.StubTypeASGI, types.StubTypeTaskQueue}

// managedEndpointStubConfig parses and validates the hosted declaration
// attached to a stub request. It returns (nil, nil) for ordinary stubs.
// Hosted stubs may only be created by cluster admins or from the configured
// system workspace, so the repo contract cannot be spoofed by a tenant.
// Model servers declare a full spec; other deployment kinds declare only
// their id, and their GPU requirement is the stub's own.
func (gws *GatewayService) managedEndpointStubConfig(ctx context.Context, authInfo *auth.AuthInfo, in *pb.GetOrCreateStubRequest) (*types.ManagedEndpointStubConfig, error) {
	stubType := types.StubType(in.StubType)
	if stubType.Kind() == types.StubTypePlatformDeployer {
		return nil, errors.New("platform deployer stubs are reserved for the control plane")
	}
	raw := strings.TrimSpace(in.ManagedEndpoint)
	modelServer := stubType.IsManagedEndpoint()
	switch {
	case raw == "" && !modelServer:
		return nil, nil
	case raw == "":
		return nil, errors.New("managed_endpoint spec is required for managed stubs")
	case !slices.Contains(hostedKinds, stubType.Kind()) || !stubType.IsDeployment() && !modelServer:
		return nil, fmt.Errorf("managed_endpoint is only valid for deployments of %s", strings.Join(hostedKinds, ", "))
	case !gws.appConfig.ManagedEndpoints.Enabled:
		return nil, errors.New("managed endpoints are not enabled on this cluster")
	case !gws.canManageEndpoints(ctx, authInfo):
		return nil, errors.New("managed endpoint stubs can only be deployed from the cluster admin workspace or with a cluster admin token")
	}

	var config types.ManagedEndpointStubConfig
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("invalid managed_endpoint spec: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("managed_endpoint must contain exactly one JSON object")
	}
	spec := config.Endpoint
	if spec == nil {
		return nil, errors.New("managed_endpoint.endpoint is required")
	}
	if modelServer && len(spec.Entrypoint) == 0 {
		spec.Entrypoint = in.Entrypoint
	}
	if !modelServer {
		spec.Gpu = map[string]types.GpuSpec{}
		for _, gpu := range gpuTypesForStubRequest(in) {
			spec.Gpu[string(gpu)] = types.GpuSpec{Count: max(in.GpuCount, 1)}
		}
	}
	spec.Normalize(modelServer)
	if err := spec.Validate(modelServer); err != nil {
		return nil, fmt.Errorf("invalid managed endpoint spec: %w", err)
	}
	// The stub name is the SDK's "<type>/<handler>" label; the deployment
	// (app) name is what must match the endpoint id.
	if in.AppName != "" && in.AppName != spec.ID {
		return nil, fmt.Errorf("app name %q must match endpoint id %q", in.AppName, spec.ID)
	}
	return &config, nil
}

// canManageEndpoints allows cluster admin tokens and any token of the cluster
// admin workspace (which owns every managed stub).
func (gws *GatewayService) canManageEndpoints(ctx context.Context, authInfo *auth.AuthInfo) bool {
	if authInfo == nil || authInfo.Token == nil || authInfo.Workspace == nil {
		return false
	}
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		return true
	}
	adminWorkspace, err := gws.backendRepo.GetAdminWorkspace(ctx)
	if err != nil || adminWorkspace == nil {
		return false
	}
	return adminWorkspace.Id == authInfo.Workspace.Id
}

// managedGpuTypes returns the GPU types a model server can run on so the stub
// runtime carries an accurate GPU requirement; the controller sets the exact
// type and count per replica from config.yaml. Other deployments (no engine
// kind) keep the GPU requirement they asked for.
func managedGpuTypes(config *types.ManagedEndpointStubConfig) []types.GpuType {
	if config == nil || config.Endpoint == nil || config.Endpoint.Kind == "" {
		return nil
	}
	var gpus []types.GpuType
	for key := range config.Endpoint.Gpu {
		if key != types.CPUInventoryKey {
			gpus = append(gpus, types.GpuType(key))
		}
	}
	return gpus
}

// managedDeployment returns the endpoint record a hosted stub already serves
// as, so redeploying an unchanged app is a no-op instead of a rollout.
func (gws *GatewayService) managedDeployment(ctx context.Context, stub *types.StubWithRelated, config *types.StubConfigV1) (*types.ManagedEndpoint, error) {
	if gws.endpointRepo == nil || config == nil || config.ManagedEndpoint == nil || config.ManagedEndpoint.Endpoint == nil {
		return nil, errors.New("managed endpoint spec missing from stub config")
	}
	existing, err := gws.endpointRepo.GetEndpoint(ctx, config.ManagedEndpoint.Endpoint.ID)
	if err != nil || existing == nil || !existing.Enabled() || existing.StubID != stub.ExternalId {
		return nil, err
	}
	return existing, nil
}

// registerManagedDeployment records a freshly deployed hosted stub as the
// endpoint's current version. Publication stays what config.yaml last
// applied; the controller rolls replicas over to the new version.
func (gws *GatewayService) registerManagedDeployment(ctx context.Context, stub *types.StubWithRelated, config *types.StubConfigV1, deployment *types.Deployment) error {
	spec := config.ManagedEndpoint.Endpoint
	existing, err := gws.endpointRepo.GetEndpoint(ctx, spec.ID)
	if err != nil {
		return err
	}
	record := &types.ManagedEndpoint{Spec: *spec, StubID: stub.ExternalId, StubType: stub.Type, Version: deployment.Version, Status: types.EndpointStatusActive}
	if existing != nil {
		record.CreatedAt, record.GitSHA, record.Publication, record.Published = existing.CreatedAt, existing.GitSHA, existing.Publication, existing.Published
	}
	return gws.endpointRepo.SaveEndpoint(ctx, record)
}
