package gatewayservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// managedEndpointStubConfig parses and validates the managed endpoint /
// service spec attached to a stub request. It returns (nil, nil) for ordinary
// stubs. Managed stubs may only be created by cluster admins or from the
// configured system workspace, so the repo contract cannot be spoofed by a
// tenant workspace.
func (gws *GatewayService) managedEndpointStubConfig(ctx context.Context, authInfo *auth.AuthInfo, in *pb.GetOrCreateStubRequest) (*types.ManagedEndpointStubConfig, error) {
	stubType := types.StubType(in.StubType)
	raw := strings.TrimSpace(in.ManagedEndpoint)

	if !stubType.IsManaged() {
		if raw != "" {
			return nil, fmt.Errorf("managed_endpoint spec is only valid for %s or %s stubs", types.StubTypeManagedEndpoint, types.StubTypeManagedService)
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

	switch {
	case stubType.IsManagedEndpoint():
		if config.Endpoint == nil {
			return nil, errors.New("managed_endpoint.endpoint is required for managed_endpoint stubs")
		}
		config.Service = nil
		if len(config.Endpoint.Entrypoint) == 0 {
			config.Endpoint.Entrypoint = in.Entrypoint
		}
		config.Endpoint.Normalize()
		policy := gws.managedEndpointValidation()
		if gws.endpointRepo != nil {
			services, err := gws.endpointRepo.ListServices(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve managed services: %w", err)
			}
			policy.KnownServices = map[string]struct{}{}
			for _, service := range services {
				policy.KnownServices[service.Spec.Name] = struct{}{}
			}
		}
		if err := config.Endpoint.Validate(policy); err != nil {
			return nil, fmt.Errorf("invalid managed endpoint spec: %w", err)
		}
		// The stub name is the SDK's "<type>/<handler>" label; the deployment
		// (app) name is what must match the endpoint id.
		if in.AppName != "" && in.AppName != config.Endpoint.ID {
			return nil, fmt.Errorf("app name %q must match endpoint id %q", in.AppName, config.Endpoint.ID)
		}
	case stubType.IsManagedService():
		if config.Service == nil {
			return nil, errors.New("managed_endpoint.service is required for managed_service stubs")
		}
		config.Endpoint = nil
		if len(config.Service.Entrypoint) == 0 {
			config.Service.Entrypoint = in.Entrypoint
		}
		config.Service.Normalize()
		if err := config.Service.Validate(); err != nil {
			return nil, fmt.Errorf("invalid managed service spec: %w", err)
		}
		if in.AppName != "" && in.AppName != config.Service.Name {
			return nil, fmt.Errorf("app name %q must match service name %q", in.AppName, config.Service.Name)
		}
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

func (gws *GatewayService) managedEndpointValidation() types.ManagedEndpointValidation {
	policy := types.ManagedEndpointValidation{AllowedEngines: gws.appConfig.ManagedEndpoints.AllowedEngines}
	for _, kind := range gws.appConfig.ManagedEndpoints.AllowedKinds {
		policy.AllowedKinds = append(policy.AllowedKinds, types.EndpointKind(strings.ToLower(strings.TrimSpace(kind))))
	}
	return policy
}

// managedTargetGpuTypes returns the union of GPU types across every target so
// the stub runtime carries an accurate GPU requirement; the controller sets
// the exact type and count per replica.
func managedTargetGpuTypes(config *types.ManagedEndpointStubConfig) ([]types.GpuType, uint32) {
	if config == nil {
		return nil, 0
	}
	var targets []types.GpuTarget
	switch {
	case config.Endpoint != nil:
		for _, rt := range config.Endpoint.Targets() {
			targets = append(targets, rt.Target)
		}
	case config.Service != nil:
		targets = config.Service.Gpu
	}

	seen := map[types.GpuType]struct{}{}
	var gpus []types.GpuType
	var maxCount uint32
	for _, t := range targets {
		if t.IsCPU() {
			continue
		}
		gpu := types.GpuType(t.Type)
		if _, ok := seen[gpu]; !ok {
			seen[gpu] = struct{}{}
			gpus = append(gpus, gpu)
		}
		maxCount = max(maxCount, t.Count)
	}
	return gpus, maxCount
}

// registerManagedDeployment records a freshly deployed managed stub in the
// endpoint registry. The first version of an endpoint becomes active
// immediately; later versions are registered as canaries and the controller's
// rollout loop promotes or rolls them back.
func (gws *GatewayService) registerManagedDeployment(ctx context.Context, stub *types.StubWithRelated, config *types.StubConfigV1, deployment *types.Deployment) error {
	if gws.endpointRepo == nil || config == nil || config.ManagedEndpoint == nil {
		return errors.New("managed endpoint registry is unavailable")
	}
	now := time.Now()

	if service := config.ManagedEndpoint.Service; service != nil {
		existing, err := gws.endpointRepo.GetService(ctx, service.Name)
		if err != nil {
			return err
		}
		record := &types.ManagedService{Spec: *service, ManagedRecord: types.ManagedRecord{
			StubID: stub.ExternalId, Version: deployment.Version, GitSHA: config.ManagedEndpoint.GitSHA, Status: types.EndpointStatusActive,
		}}
		if existing != nil {
			record.CreatedAt = existing.CreatedAt
			if existing.Status == types.EndpointStatusDisabled {
				record.Status = existing.Status
			}
		}
		return gws.endpointRepo.SaveService(ctx, record)
	}

	spec := config.ManagedEndpoint.Endpoint
	if spec == nil {
		return errors.New("managed endpoint spec missing from stub config")
	}

	version := &types.EndpointVersion{
		EndpointID: spec.ID,
		Version:    deployment.Version,
		StubID:     stub.ExternalId,
		GitSHA:     config.ManagedEndpoint.GitSHA,
		State:      types.VersionStateCanary,
		CreatedAt:  now,
	}
	existing, err := gws.endpointRepo.GetEndpoint(ctx, spec.ID)
	if err != nil {
		return err
	}
	rollout, err := gws.endpointRepo.GetRollout(ctx, spec.ID)
	if err != nil {
		return err
	}
	if rollout == nil {
		rollout = &types.RolloutState{EndpointID: spec.ID, Phase: types.RolloutPhaseIdle}
	}

	firstVersion := existing == nil || existing.Status == types.EndpointStatusRetired || rollout.ActiveVersion == 0
	if firstVersion {
		version.State = types.VersionStateActive
		record := &types.ManagedEndpoint{Spec: *spec, ManagedRecord: types.ManagedRecord{
			StubID: stub.ExternalId, Version: deployment.Version, GitSHA: config.ManagedEndpoint.GitSHA, Status: types.EndpointStatusActive,
		}}
		if existing != nil {
			record.CreatedAt = existing.CreatedAt
		}
		if err := gws.endpointRepo.SaveEndpoint(ctx, record); err != nil {
			return err
		}
		rollout.ActiveVersion = deployment.Version
		rollout.CanaryVersion = 0
		rollout.Phase = types.RolloutPhaseIdle
		rollout.LastDecision = "initial deploy"
		rollout.LastDecisionAt = now
	} else {
		// A previous canary that never finished baking is superseded.
		if rollout.CanaryVersion != 0 && rollout.CanaryVersion != deployment.Version {
			if err := gws.retireManagedVersion(ctx, spec.ID, rollout.CanaryVersion, types.VersionStateRolledBack); err != nil {
				return err
			}
		}
		rollout.CanaryVersion = deployment.Version
		rollout.Phase = types.RolloutPhaseBaking
		rollout.BakeStartedAt = time.Time{}
		rollout.LastDecision = "new version deployed; canary pending"
		rollout.LastDecisionAt = now
	}

	if err := gws.endpointRepo.SaveVersion(ctx, version); err != nil {
		return err
	}
	return gws.endpointRepo.SaveRollout(ctx, rollout)
}

func (gws *GatewayService) retireManagedVersion(ctx context.Context, endpointID string, version uint, state types.EndpointVersionState) error {
	versions, err := gws.endpointRepo.ListVersions(ctx, endpointID)
	if err != nil {
		return err
	}
	for _, v := range versions {
		if v.Version != version {
			continue
		}
		v.State = state
		return gws.endpointRepo.SaveVersion(ctx, v)
	}
	return nil
}
