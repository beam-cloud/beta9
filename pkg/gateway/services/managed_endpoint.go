package gatewayservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// managedEndpointStubConfig parses and validates the managed endpoint spec
// attached to a stub request. It returns (nil, nil) for ordinary stubs.
// Managed stubs may only be created by cluster admins or from the configured
// system workspace, so the repo contract cannot be spoofed by a tenant.
func (gws *GatewayService) managedEndpointStubConfig(ctx context.Context, authInfo *auth.AuthInfo, in *pb.GetOrCreateStubRequest) (*types.ManagedEndpointStubConfig, error) {
	raw := strings.TrimSpace(in.ManagedEndpoint)
	if !types.StubType(in.StubType).IsManagedEndpoint() {
		if raw != "" {
			return nil, fmt.Errorf("managed_endpoint spec is only valid for %s stubs", types.StubTypeManagedEndpoint)
		}
		return nil, nil
	}
	if !gws.appConfig.ManagedEndpoints.Enabled {
		return nil, errors.New("managed endpoints are not enabled on this cluster")
	}
	if !gws.canManageEndpoints(ctx, authInfo) {
		return nil, errors.New("managed endpoint stubs can only be deployed from the cluster admin workspace or with a cluster admin token")
	}
	if raw == "" {
		return nil, errors.New("managed_endpoint spec is required for managed stubs")
	}

	var config types.ManagedEndpointStubConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return nil, fmt.Errorf("invalid managed_endpoint spec: %w", err)
	}
	if config.Endpoint == nil {
		return nil, errors.New("managed_endpoint.endpoint is required")
	}
	if len(config.Endpoint.Entrypoint) == 0 {
		config.Endpoint.Entrypoint = in.Entrypoint
	}
	config.Endpoint.Normalize()
	if err := config.Endpoint.Validate(); err != nil {
		return nil, fmt.Errorf("invalid managed endpoint spec: %w", err)
	}
	// The stub name is the SDK's "<type>/<handler>" label; the deployment
	// (app) name is what must match the endpoint id.
	if in.AppName != "" && in.AppName != config.Endpoint.ID {
		return nil, fmt.Errorf("app name %q must match endpoint id %q", in.AppName, config.Endpoint.ID)
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

// managedGpuTypes returns the GPU types the endpoint can run on so the stub
// runtime carries an accurate GPU requirement; the controller sets the exact
// type and count per replica from fleet.yaml.
func managedGpuTypes(config *types.ManagedEndpointStubConfig) []types.GpuType {
	if config == nil || config.Endpoint == nil {
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

// registerManagedDeployment records a freshly deployed managed stub as the
// endpoint's current version. The controller rolls replicas over to it.
func (gws *GatewayService) registerManagedDeployment(ctx context.Context, stub *types.StubWithRelated, config *types.StubConfigV1, deployment *types.Deployment) error {
	if gws.endpointRepo == nil || config == nil || config.ManagedEndpoint == nil || config.ManagedEndpoint.Endpoint == nil {
		return errors.New("managed endpoint spec missing from stub config")
	}
	spec := config.ManagedEndpoint.Endpoint
	existing, err := gws.endpointRepo.GetEndpoint(ctx, spec.ID)
	if err != nil {
		return err
	}
	record := &types.ManagedEndpoint{
		Spec: *spec, StubID: stub.ExternalId, Version: deployment.Version,
		GitSHA: config.ManagedEndpoint.GitSHA, Status: types.EndpointStatusActive,
	}
	if existing != nil {
		record.CreatedAt = existing.CreatedAt
	}
	return gws.endpointRepo.SaveEndpoint(ctx, record)
}
