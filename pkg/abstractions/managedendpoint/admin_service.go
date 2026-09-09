package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
)

// EndpointAdminService: cluster-admin RPCs used by tuning agents and the
// frontend. Every handler returns ok=false with err_msg for domain errors and
// a gRPC status only for auth / transport failures.

const (
	defaultAckWait     = 30 * time.Second
	maxAckWait         = 5 * time.Minute
	experimentLockTTL  = 2 * time.Hour
	experimentPollStep = 500 * time.Millisecond
)

// gitopsTrigger is implemented by the GitOps reconciler.
type gitopsTrigger interface {
	Trigger(ctx context.Context, sha string) (bool, error)
}

func (s *Service) ListEndpoints(ctx context.Context, in *pb.ListEndpointsRequest) (*pb.ListEndpointsResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	endpoints, err := s.repo.ListEndpoints(ctx)
	if err != nil {
		return &pb.ListEndpointsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.ListEndpointsResponse{Ok: true}
	for _, endpoint := range endpoints {
		if !in.IncludeDisabled && !endpoint.Enabled {
			continue
		}
		summary, err := s.endpointSummary(ctx, endpoint)
		if err != nil {
			return &pb.ListEndpointsResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		out.Endpoints = append(out.Endpoints, summary)
	}
	return out, nil
}

func (s *Service) GetEndpoint(ctx context.Context, in *pb.GetEndpointRequest) (*pb.GetEndpointResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	endpoint, err := s.repo.GetEndpoint(ctx, in.EndpointId)
	if err != nil {
		return &pb.GetEndpointResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if endpoint == nil {
		return &pb.GetEndpointResponse{Ok: false, ErrMsg: "endpoint not found"}, nil
	}
	summary, err := s.endpointSummary(ctx, endpoint)
	if err != nil {
		return &pb.GetEndpointResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	replicas, err := s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return &pb.GetEndpointResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.GetEndpointResponse{Ok: true, Endpoint: summary}
	for _, r := range replicas {
		out.Replicas = append(out.Replicas, replicaToProto(r))
	}
	rollout, versions, err := s.rolloutWithVersions(ctx, endpoint.Spec.ID)
	if err != nil {
		return &pb.GetEndpointResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out.Rollout = rolloutToProto(rollout, versions)
	return out, nil
}

func (s *Service) rolloutWithVersions(ctx context.Context, endpointID string) (*types.RolloutState, []*types.EndpointVersion, error) {
	rollout, err := s.repo.GetRollout(ctx, endpointID)
	if err != nil {
		return nil, nil, err
	}
	versions, err := s.repo.ListVersions(ctx, endpointID)
	if err != nil {
		return nil, nil, err
	}
	return rollout, versions, nil
}

func (s *Service) ListReplicas(ctx context.Context, in *pb.ListReplicasRequest) (*pb.ListReplicasResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	var (
		replicas []*types.EndpointReplica
		err      error
	)
	if in.EndpointId == "" {
		replicas, err = s.repo.ListAllReplicas(ctx)
	} else {
		replicas, err = s.repo.ListReplicas(ctx, in.EndpointId)
	}
	if err != nil {
		return &pb.ListReplicasResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.ListReplicasResponse{Ok: true}
	for _, r := range replicas {
		if in.Status != "" && string(r.Status) != in.Status {
			continue
		}
		if in.Gpu != "" && r.GPU != normalizeGPUKey(in.Gpu) {
			continue
		}
		if in.Role != "" && r.Role != in.Role {
			continue
		}
		out.Replicas = append(out.Replicas, replicaToProto(r))
	}
	return out, nil
}

func (s *Service) GetMetrics(ctx context.Context, in *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	window := time.Duration(in.WindowSeconds) * time.Second
	if window <= 0 {
		window = 5 * time.Minute
	}
	gpu := ""
	if in.Gpu != "" {
		gpu = normalizeGPUKey(in.Gpu)
	}
	metrics, err := s.repo.GetRouteMetrics(ctx, in.EndpointId, gpu, 0, window)
	if err != nil {
		return &pb.GetMetricsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	replicas, err := s.repo.ListReplicas(ctx, in.EndpointId)
	if err != nil {
		return &pb.GetMetricsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	var selected []*types.EndpointReplica
	for _, r := range replicas {
		if r.Status.Terminal() {
			continue
		}
		if gpu != "" && r.GPU != gpu {
			continue
		}
		if in.ReplicaId != "" && r.ID != in.ReplicaId {
			continue
		}
		selected = append(selected, r)
	}
	out := &pb.GetMetricsResponse{Ok: true, Metrics: routeMetricsToProto(metrics, selected)}
	for _, r := range selected {
		out.Replicas = append(out.Replicas, replicaToProto(r))
	}
	return out, nil
}

func parseScope(scope string) (types.ConfigRevisionScope, error) {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "", "target":
		return types.ConfigScopeTarget, nil
	case "replica":
		return types.ConfigScopeReplica, nil
	}
	return "", fmt.Errorf("unknown scope %q (target|replica)", scope)
}

// scopeKey normalizes an admin-supplied scope key. Target keys accept
// "<gpu>", "<role>:<gpu>" and "<role>:<gpu>@v<N>"; without a version suffix
// they resolve to the endpoint's active version.
func (s *Service) scopeKey(ctx context.Context, endpointID string, scope types.ConfigRevisionScope, key string) (string, error) {
	if scope != types.ConfigScopeTarget {
		return key, nil
	}
	target, version := parseFleetKey(key)
	role, gpu := types.ReplicaRoleServe, target
	if idx := strings.Index(target, ":"); idx >= 0 {
		role, gpu = target[:idx], target[idx+1:]
	}
	if version == 0 {
		endpoint, err := s.repo.GetEndpoint(ctx, endpointID)
		if err != nil {
			return "", err
		}
		if endpoint == nil {
			return "", errors.New("endpoint not found")
		}
		version = endpoint.Version
	}
	return fleetKey(role, normalizeGPUKey(gpu), version), nil
}

func (s *Service) GetConfig(ctx context.Context, in *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	scope, err := parseScope(in.Scope)
	if err != nil {
		return &pb.GetConfigResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	key, err := s.scopeKey(ctx, in.EndpointId, scope, in.ScopeKey)
	if err != nil {
		return &pb.GetConfigResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	revision, err := s.repo.LatestConfigRevision(ctx, in.EndpointId, scope, key)
	if err != nil {
		return &pb.GetConfigResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if revision == nil {
		return &pb.GetConfigResponse{Ok: false, ErrMsg: "no config revision"}, nil
	}
	return &pb.GetConfigResponse{Ok: true, Revision: revisionToProto(revision)}, nil
}

func (s *Service) ListConfigRevisions(ctx context.Context, in *pb.ListConfigRevisionsRequest) (*pb.ListConfigRevisionsResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	scope, err := parseScope(in.Scope)
	if err != nil {
		return &pb.ListConfigRevisionsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	limit := int(in.Limit)
	if limit <= 0 {
		limit = 20
	}
	key, err := s.scopeKey(ctx, in.EndpointId, scope, in.ScopeKey)
	if err != nil {
		return &pb.ListConfigRevisionsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	revisions, err := s.repo.ListConfigRevisions(ctx, in.EndpointId, scope, key, limit)
	if err != nil {
		return &pb.ListConfigRevisionsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.ListConfigRevisionsResponse{Ok: true}
	for _, r := range revisions {
		out.Revisions = append(out.Revisions, revisionToProto(r))
	}
	return out, nil
}

func (s *Service) GetRollout(ctx context.Context, in *pb.GetRolloutRequest) (*pb.GetRolloutResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	rollout, versions, err := s.rolloutWithVersions(ctx, in.EndpointId)
	if err != nil {
		return &pb.GetRolloutResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if rollout == nil {
		return &pb.GetRolloutResponse{Ok: false, ErrMsg: "endpoint not found"}, nil
	}
	return &pb.GetRolloutResponse{Ok: true, Rollout: rolloutToProto(rollout, versions)}, nil
}

func (s *Service) PromoteRollout(ctx context.Context, in *pb.PromoteRolloutRequest) (*pb.RolloutActionResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	return s.rolloutAction(ctx, in.EndpointId, uint(in.Version), true)
}

func (s *Service) RollbackRollout(ctx context.Context, in *pb.RollbackRolloutRequest) (*pb.RolloutActionResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	return s.rolloutAction(ctx, in.EndpointId, uint(in.Version), false)
}

// rolloutAction promotes or rolls back. With a baking canary the action
// decides it; otherwise "promote <version>" re-activates a retired version
// and "rollback" re-activates the most recent retired version.
func (s *Service) rolloutAction(ctx context.Context, endpointID string, version uint, promote bool) (*pb.RolloutActionResponse, error) {
	endpoint, err := s.repo.GetEndpoint(ctx, endpointID)
	if err != nil || endpoint == nil {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: "endpoint not found"}, nil
	}
	rollout, versions, err := s.rolloutWithVersions(ctx, endpointID)
	if err != nil {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if rollout == nil {
		rollout = &types.RolloutState{EndpointID: endpointID, ActiveVersion: endpoint.Version, Phase: rolloutPhaseIdle}
	}

	who := "admin"
	switch {
	case rollout.CanaryVersion != 0 && (version == 0 || version == rollout.CanaryVersion):
		reason := "manual promote by " + who
		if !promote {
			reason = "manual rollback by " + who
		}
		if err := s.controller.finishRollout(ctx, endpoint, rollout, promote, reason); err != nil {
			return &pb.RolloutActionResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	case promote && version != 0 && version != endpoint.Version:
		if err := s.controller.activateVersion(ctx, endpoint, rollout, versions, version, "manual promote by "+who); err != nil {
			return &pb.RolloutActionResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	case !promote:
		target := version
		if target == 0 {
			for _, v := range versions {
				if v.State == types.VersionStateRetired && v.Version < endpoint.Version && v.Version > target {
					target = v.Version
				}
			}
		}
		if target == 0 || target == endpoint.Version {
			return &pb.RolloutActionResponse{Ok: false, ErrMsg: "no previous version to roll back to"}, nil
		}
		if err := s.controller.activateVersion(ctx, endpoint, rollout, versions, target, "manual rollback by "+who); err != nil {
			return &pb.RolloutActionResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	default:
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: "nothing to promote"}, nil
	}

	rollout, versions, err = s.rolloutWithVersions(ctx, endpointID)
	if err != nil {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.RolloutActionResponse{Ok: true, Rollout: rolloutToProto(rollout, versions)}, nil
}

func (s *Service) PinVersion(ctx context.Context, in *pb.PinVersionRequest) (*pb.RolloutActionResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	rollout, versions, err := s.rolloutWithVersions(ctx, in.EndpointId)
	if err != nil {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if rollout == nil {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: "endpoint not found"}, nil
	}
	if in.Version != 0 {
		found := false
		for _, v := range versions {
			if v.Version == uint(in.Version) {
				found = true
			}
		}
		if !found {
			return &pb.RolloutActionResponse{Ok: false, ErrMsg: fmt.Sprintf("version %d not found", in.Version)}, nil
		}
	}
	rollout.PinnedVersion = uint(in.Version)
	rollout.LastDecision = fmt.Sprintf("pinned version %d", in.Version)
	if in.Version == 0 {
		rollout.LastDecision = "unpinned"
	}
	rollout.LastDecisionAt = time.Now()
	if err := s.repo.SaveRollout(ctx, rollout); err != nil {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	s.emit(types.EventEndpointRollout, types.EventEndpointSchema{EndpointID: in.EndpointId, Action: "rollout.pinned", Version: uint(in.Version)})
	return &pb.RolloutActionResponse{Ok: true, Rollout: rolloutToProto(rollout, versions)}, nil
}

func (s *Service) GetGitOpsStatus(ctx context.Context, _ *pb.GetGitOpsStatusRequest) (*pb.GetGitOpsStatusResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	state, err := s.repo.GetGitOpsState(ctx)
	if err != nil {
		return &pb.GetGitOpsStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		state = &types.GitOpsState{RepoURL: s.config.Repo.URL, Ref: s.config.Repo.RefOrDefault()}
	}
	return &pb.GetGitOpsStatusResponse{Ok: true, State: gitopsToProto(state)}, nil
}

func (s *Service) TriggerGitOpsSync(ctx context.Context, in *pb.TriggerGitOpsSyncRequest) (*pb.TriggerGitOpsSyncResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	if s.gitops == nil {
		return &pb.TriggerGitOpsSyncResponse{Ok: false, ErrMsg: "gitops is not configured (managedEndpoints.repo.url)"}, nil
	}
	started, err := s.gitops.Trigger(ctx, in.Sha)
	if err != nil {
		return &pb.TriggerGitOpsSyncResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.TriggerGitOpsSyncResponse{Ok: true, Started: started}, nil
}

func (s *Service) SetEndpointEnabled(ctx context.Context, in *pb.SetEndpointEnabledRequest) (*pb.SetEndpointEnabledResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	endpoint, err := s.repo.GetEndpoint(ctx, in.EndpointId)
	if err != nil {
		return &pb.SetEndpointEnabledResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if endpoint == nil {
		return &pb.SetEndpointEnabledResponse{Ok: false, ErrMsg: "endpoint not found"}, nil
	}
	if endpoint.Enabled != in.Enabled {
		endpoint.Enabled = in.Enabled
		endpoint.Status = types.EndpointStatusActive
		if !in.Enabled {
			endpoint.Status = types.EndpointStatusDisabled
		}
		endpoint.UpdatedAt = time.Now()
		if err := s.repo.SaveEndpoint(ctx, endpoint); err != nil {
			return &pb.SetEndpointEnabledResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		action := "endpoint.enabled"
		if !in.Enabled {
			action = "endpoint.disabled"
		}
		s.emit(types.EventEndpointConfig, types.EventEndpointSchema{EndpointID: endpoint.Spec.ID, Action: action, Version: endpoint.Version})
	}
	summary, err := s.endpointSummary(ctx, endpoint)
	if err != nil {
		return &pb.SetEndpointEnabledResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.SetEndpointEnabledResponse{Ok: true, Endpoint: summary}, nil
}

func (s *Service) ListServices(ctx context.Context, _ *pb.ListServicesRequest) (*pb.ListServicesResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	services, err := s.repo.ListServices(ctx)
	if err != nil {
		return &pb.ListServicesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	replicas, err := s.repo.ListAllReplicas(ctx)
	if err != nil {
		return &pb.ListServicesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.ListServicesResponse{Ok: true}
	for _, service := range services {
		entry := serviceToProto(service)
		for _, r := range replicas {
			if r.EndpointID != serviceReplicaID(service.Spec.Name) || r.Status.Terminal() {
				continue
			}
			entry.TotalReplicas++
			if r.Status == types.ReplicaStatusReady {
				entry.ReadyReplicas++
			}
		}
		out.Services = append(out.Services, entry)
	}
	return out, nil
}

// validationPolicy mirrors the gateway's deploy-time validation.
func (s *Service) validationPolicy(ctx context.Context) types.ManagedEndpointValidation {
	policy := types.ManagedEndpointValidation{AllowedEngines: s.config.AllowedEngines, KnownServices: map[string]struct{}{}}
	for _, kind := range s.config.AllowedKinds {
		policy.AllowedKinds = append(policy.AllowedKinds, types.EndpointKind(strings.ToLower(strings.TrimSpace(kind))))
	}
	if services, err := s.repo.ListServices(ctx); err == nil {
		for _, service := range services {
			policy.KnownServices[service.Spec.Name] = struct{}{}
		}
	}
	return policy
}

func (s *Service) ValidateEndpointSpec(ctx context.Context, in *pb.ValidateEndpointSpecRequest) (*pb.ValidateEndpointSpecResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	kind := strings.ToLower(strings.TrimSpace(in.Kind))
	if kind == "" {
		kind = "endpoint"
	}
	switch kind {
	case "endpoint":
		var spec types.ManagedEndpointSpec
		if err := json.Unmarshal([]byte(in.SpecJson), &spec); err != nil {
			return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: "invalid spec json", Errors: []string{err.Error()}}, nil
		}
		spec.Normalize()
		if err := spec.Validate(s.validationPolicy(ctx)); err != nil {
			return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: err.Error(), Errors: splitErrors(err)}, nil
		}
		return &pb.ValidateEndpointSpecResponse{Ok: true, SpecJson: mustJSON(spec)}, nil
	case "service":
		var spec types.ManagedServiceSpec
		if err := json.Unmarshal([]byte(in.SpecJson), &spec); err != nil {
			return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: "invalid spec json", Errors: []string{err.Error()}}, nil
		}
		spec.Normalize()
		if err := spec.Validate(); err != nil {
			return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: err.Error(), Errors: splitErrors(err)}, nil
		}
		return &pb.ValidateEndpointSpecResponse{Ok: true, SpecJson: mustJSON(spec)}, nil
	}
	return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: fmt.Sprintf("unknown kind %q (endpoint|service)", in.Kind)}, nil
}

func splitErrors(err error) []string {
	if err == nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(err.Error(), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

// --- Experiments -----------------------------------------------------------

func (s *Service) experimentResponse(ctx context.Context, experiment *types.Experiment) *pb.ExperimentResponse {
	var replica *types.EndpointReplica
	if experiment != nil && experiment.ReplicaID != "" {
		replica, _ = s.repo.GetReplica(ctx, experiment.ReplicaID)
	}
	return &pb.ExperimentResponse{Ok: true, Experiment: experimentToProto(experiment, replica)}
}

func experimentError(msg string) *pb.ExperimentResponse {
	return &pb.ExperimentResponse{Ok: false, ErrMsg: msg}
}

func (s *Service) saveExperiment(ctx context.Context, experiment *types.Experiment) error {
	return s.repo.SaveExperiment(ctx, experiment, s.config.Tuning.ExperimentTTLOrDefault(), s.config.Tuning.KeepExperimentsOrDefault())
}

// StartExperiment launches a dedicated tuning replica for one target. The
// replica never receives public traffic; agents address it explicitly.
func (s *Service) StartExperiment(ctx context.Context, in *pb.StartExperimentRequest) (*pb.ExperimentResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	endpoint, err := s.repo.GetEndpoint(ctx, in.EndpointId)
	if err != nil {
		return experimentError(err.Error()), nil
	}
	if endpoint == nil {
		return experimentError("endpoint not found"), nil
	}
	if !endpoint.Spec.Harness.Enabled {
		return experimentError("endpoint does not enable the harness; live tuning is unavailable"), nil
	}
	role := strings.TrimSpace(in.Role)
	if role == "" {
		role = types.ReplicaRoleServe
	}
	gpu := normalizeGPUKey(in.Gpu)
	var target *types.RoleTarget
	for _, rt := range endpoint.Spec.Targets() {
		if rt.Role == role && rt.Target.Key() == gpu {
			t := rt
			target = &t
		}
	}
	if target == nil {
		return experimentError(fmt.Sprintf("endpoint has no target %s on %s", role, gpu)), nil
	}

	experimentID := "exp-" + uuid.New().String()[:12]
	acquired, holder, err := s.repo.AcquireExperimentLock(ctx, endpoint.Spec.ID, experimentID, experimentLockTTL)
	if err != nil {
		return experimentError(err.Error()), nil
	}
	if !acquired {
		return experimentError(fmt.Sprintf("experiment %s is already running on this endpoint", holder)), nil
	}

	baseline, err := s.repo.LatestConfigRevision(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, fleetKey(role, target.Target.Key(), endpoint.Version))
	if err != nil {
		_ = s.repo.ReleaseExperimentLock(ctx, endpoint.Spec.ID, experimentID)
		return experimentError(err.Error()), nil
	}
	var baselineRevision uint64
	if baseline != nil {
		baselineRevision = baseline.Revision
	}

	replicas, _ := s.repo.ListAllReplicas(ctx)
	services, missing := s.controller.serviceAddresses(ctx, endpoint.Spec.Services, replicas)
	if len(missing) > 0 {
		_ = s.repo.ReleaseExperimentLock(ctx, endpoint.Spec.ID, experimentID)
		return experimentError(fmt.Sprintf("required services not ready: %s", strings.Join(missing, ", "))), nil
	}

	replica, err := s.controller.startReplica(ctx, startSpec{
		EndpointID: endpoint.Spec.ID,
		Version:    endpoint.Version,
		StubID:     endpoint.StubID,
		Role:       role,
		Target:     target.Target,
		Port:       endpoint.Spec.Port,
		Protected:  true,
		Tuning:     true,
		Harness:    true,
		Entrypoint: endpoint.Spec.Entrypoint,
		Services:   services,
		KVCache:    endpoint.Spec.KVCache,
		GitSHA:     endpoint.GitSHA,
	})
	if err != nil {
		_ = s.repo.ReleaseExperimentLock(ctx, endpoint.Spec.ID, experimentID)
		return experimentError("start tuning replica: " + err.Error()), nil
	}

	experiment := &types.Experiment{
		ID:               experimentID,
		EndpointID:       endpoint.Spec.ID,
		GPU:              gpu,
		Role:             role,
		ReplicaID:        replica.ID,
		BaselineRevision: baselineRevision,
		CurrentRevision:  baselineRevision,
		Outcome:          types.ExperimentOutcomeRunning,
		Author:           in.Author,
		Budget:           in.Budget,
		StartedAt:        time.Now(),
	}
	if err := s.saveExperiment(ctx, experiment); err != nil {
		return experimentError(err.Error()), nil
	}
	s.emit(types.EventEndpointExperiment, types.EventEndpointSchema{
		EndpointID: endpoint.Spec.ID, Action: "experiment.started", Experiment: experimentID,
		ReplicaID: replica.ID, GPU: gpu, Role: role, Version: endpoint.Version, Revision: baselineRevision,
		Data: map[string]any{"author": in.Author, "budget": in.Budget},
	})

	if in.WaitSeconds > 0 {
		s.waitForReplicaReady(ctx, replica.ID, time.Duration(in.WaitSeconds)*time.Second)
	}
	return s.experimentResponse(ctx, experiment), nil
}

func (s *Service) waitForReplicaReady(ctx context.Context, replicaID string, wait time.Duration) *types.EndpointReplica {
	deadline := time.Now().Add(wait)
	for {
		replica, err := s.repo.GetReplica(ctx, replicaID)
		if err == nil && replica != nil && (replica.Status == types.ReplicaStatusReady || replica.Status.Terminal()) {
			return replica
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return replica
		}
		select {
		case <-ctx.Done():
			return replica
		case <-time.After(experimentPollStep):
		}
	}
}

func (s *Service) runningExperiment(ctx context.Context, id string) (*types.Experiment, *pb.ExperimentResponse) {
	experiment, err := s.repo.GetExperiment(ctx, id)
	if err != nil {
		return nil, experimentError(err.Error())
	}
	if experiment == nil {
		return nil, experimentError("experiment not found")
	}
	if experiment.Outcome != types.ExperimentOutcomeRunning {
		return nil, experimentError(fmt.Sprintf("experiment is %s", experiment.Outcome))
	}
	return experiment, nil
}

func (s *Service) GetExperiment(ctx context.Context, in *pb.GetExperimentRequest) (*pb.ExperimentResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	experiment, err := s.repo.GetExperiment(ctx, in.ExperimentId)
	if err != nil {
		return experimentError(err.Error()), nil
	}
	if experiment == nil {
		return experimentError("experiment not found"), nil
	}
	return s.experimentResponse(ctx, experiment), nil
}

func (s *Service) ListExperiments(ctx context.Context, in *pb.ListExperimentsRequest) (*pb.ListExperimentsResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	experiments, err := s.repo.ListExperiments(ctx, in.EndpointId, int(in.Limit))
	if err != nil {
		return &pb.ListExperimentsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.ListExperimentsResponse{Ok: true}
	for _, e := range experiments {
		if in.Gpu != "" && e.GPU != normalizeGPUKey(in.Gpu) {
			continue
		}
		out.Experiments = append(out.Experiments, experimentToProto(e, nil))
	}
	return out, nil
}

// applyReplicaConfig publishes a replica-scoped revision and waits for the
// harness ack, recording the outcome as an experiment step.
func (s *Service) applyReplicaConfig(ctx context.Context, experiment *types.Experiment, config map[string]any, wait time.Duration) (*types.ExperimentStep, error) {
	if wait <= 0 {
		wait = defaultAckWait
	}
	if wait > maxAckWait {
		wait = maxAckWait
	}
	revision := &types.EndpointConfigRevision{
		EndpointID: experiment.EndpointID,
		Scope:      types.ConfigScopeReplica,
		ScopeKey:   experiment.ReplicaID,
		Config:     config,
		Author:     "experiment:" + experiment.ID,
		Source:     types.ConfigSourceLive,
	}
	if err := s.repo.CreateConfigRevision(ctx, revision); err != nil {
		return nil, err
	}

	step := types.ExperimentStep{Revision: revision.Revision, Config: config, At: time.Now()}
	deadline := time.Now().Add(wait)
	for {
		ack, err := s.repo.GetConfigAck(ctx, experiment.ReplicaID, revision.Revision)
		if err == nil && ack != nil {
			step.Applied = ack.Applied
			step.Error = ack.Error
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			step.Error = "timed out waiting for harness ack"
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(experimentPollStep):
		}
	}
	if replica, err := s.repo.GetReplica(ctx, experiment.ReplicaID); err == nil && replica != nil {
		step.EngineMetrics = replica.Capacity
	}
	experiment.Steps = append(experiment.Steps, step)
	if step.Applied {
		experiment.CurrentRevision = revision.Revision
	}
	return &step, s.saveExperiment(ctx, experiment)
}

func (s *Service) ApplyExperimentConfig(ctx context.Context, in *pb.ApplyExperimentConfigRequest) (*pb.ExperimentResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	experiment, errResp := s.runningExperiment(ctx, in.ExperimentId)
	if errResp != nil {
		return errResp, nil
	}
	if in.ExpectedRevision != 0 && in.ExpectedRevision != experiment.CurrentRevision {
		return experimentError(fmt.Sprintf("revision conflict: current is %d", experiment.CurrentRevision)), nil
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(in.ConfigJson), &config); err != nil || config == nil {
		return experimentError("config_json must be a JSON object"), nil
	}
	step, err := s.applyReplicaConfig(ctx, experiment, config, time.Duration(in.WaitSeconds)*time.Second)
	if err != nil {
		return experimentError(err.Error()), nil
	}
	s.emit(types.EventEndpointExperiment, types.EventEndpointSchema{
		EndpointID: experiment.EndpointID, Action: "experiment.applied", Experiment: experiment.ID,
		ReplicaID: experiment.ReplicaID, GPU: experiment.GPU, Role: experiment.Role, Revision: step.Revision,
		Message: step.Error, Data: map[string]any{"applied": step.Applied, "config": config},
	})
	return s.experimentResponse(ctx, experiment), nil
}

func (s *Service) GetExperimentMetrics(ctx context.Context, in *pb.GetExperimentMetricsRequest) (*pb.GetExperimentMetricsResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	experiment, err := s.repo.GetExperiment(ctx, in.ExperimentId)
	if err != nil {
		return &pb.GetExperimentMetricsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if experiment == nil {
		return &pb.GetExperimentMetricsResponse{Ok: false, ErrMsg: "experiment not found"}, nil
	}
	replica, err := s.repo.GetReplica(ctx, experiment.ReplicaID)
	if err != nil {
		return &pb.GetExperimentMetricsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.GetExperimentMetricsResponse{Ok: true}
	if replica != nil {
		out.Capacity = capacityToProto(replica.Capacity)
		out.MetricsJson = mustJSON(map[string]any{
			"status":          replica.Status,
			"capacity":        replica.Capacity,
			"capabilities":    replica.Capabilities,
			"config_revision": replica.ConfigRevision,
			"last_heartbeat":  replica.LastHeartbeat,
		})
	}
	window := time.Duration(in.WindowSeconds) * time.Second
	if window <= 0 {
		window = 5 * time.Minute
	}
	if metrics, err := s.repo.GetRouteMetrics(ctx, experiment.EndpointID, experiment.GPU, 0, window); err == nil && metrics != nil {
		var replicas []*types.EndpointReplica
		if replica != nil {
			replicas = []*types.EndpointReplica{replica}
		}
		out.RouteMetrics = routeMetricsToProto(metrics, replicas)
	}
	return out, nil
}

func (s *Service) RecordExperimentBench(ctx context.Context, in *pb.RecordExperimentBenchRequest) (*pb.ExperimentResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	experiment, errResp := s.runningExperiment(ctx, in.ExperimentId)
	if errResp != nil {
		return errResp, nil
	}
	if !json.Valid([]byte(in.BenchJson)) {
		return experimentError("bench_json must be valid JSON"), nil
	}
	if len(experiment.Steps) == 0 {
		experiment.Steps = append(experiment.Steps, types.ExperimentStep{Revision: experiment.CurrentRevision, Applied: true, At: time.Now()})
	}
	experiment.Steps[len(experiment.Steps)-1].Bench = json.RawMessage(in.BenchJson)
	if err := s.saveExperiment(ctx, experiment); err != nil {
		return experimentError(err.Error()), nil
	}
	s.emit(types.EventEndpointExperiment, types.EventEndpointSchema{
		EndpointID: experiment.EndpointID, Action: "experiment.bench", Experiment: experiment.ID,
		ReplicaID: experiment.ReplicaID, Revision: experiment.CurrentRevision,
		Data: map[string]any{"bench": json.RawMessage(in.BenchJson)},
	})
	return s.experimentResponse(ctx, experiment), nil
}

func (s *Service) RevertExperiment(ctx context.Context, in *pb.RevertExperimentRequest) (*pb.ExperimentResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	experiment, errResp := s.runningExperiment(ctx, in.ExperimentId)
	if errResp != nil {
		return errResp, nil
	}
	config := map[string]any{}
	if experiment.BaselineRevision != 0 {
		baseline, err := s.repo.GetConfigRevision(ctx, experiment.EndpointID, experiment.BaselineRevision)
		if err != nil {
			return experimentError(err.Error()), nil
		}
		if baseline != nil && baseline.Config != nil {
			config = baseline.Config
		}
	}
	step, err := s.applyReplicaConfig(ctx, experiment, config, time.Duration(in.WaitSeconds)*time.Second)
	if err != nil {
		return experimentError(err.Error()), nil
	}
	s.emit(types.EventEndpointExperiment, types.EventEndpointSchema{
		EndpointID: experiment.EndpointID, Action: "experiment.reverted", Experiment: experiment.ID,
		ReplicaID: experiment.ReplicaID, Revision: step.Revision, Message: step.Error,
	})
	return s.experimentResponse(ctx, experiment), nil
}

// StopExperiment ends the session. "keep" promotes the last applied config
// to the fleet revision for the target; either way the tuning replica is
// stopped and the endpoint lock released.
func (s *Service) StopExperiment(ctx context.Context, in *pb.StopExperimentRequest) (*pb.ExperimentResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	experiment, errResp := s.runningExperiment(ctx, in.ExperimentId)
	if errResp != nil {
		return errResp, nil
	}
	outcome := types.ExperimentOutcome(strings.ToLower(strings.TrimSpace(in.Outcome)))
	switch outcome {
	case types.ExperimentOutcomeKeep, types.ExperimentOutcomeDiscard:
	case "":
		outcome = types.ExperimentOutcomeDiscard
	default:
		return experimentError("outcome must be keep or discard"), nil
	}

	if outcome == types.ExperimentOutcomeKeep {
		var config map[string]any
		for i := len(experiment.Steps) - 1; i >= 0; i-- {
			if experiment.Steps[i].Applied && experiment.Steps[i].Config != nil {
				config = experiment.Steps[i].Config
				break
			}
		}
		if config == nil {
			return experimentError("nothing to keep: no applied config step"), nil
		}
		endpoint, err := s.repo.GetEndpoint(ctx, experiment.EndpointID)
		if err != nil || endpoint == nil {
			return experimentError("endpoint not found"), nil
		}
		// The config was tuned against the tuning replica's version; only
		// promote it to the fleet while that version is still the active one.
		if replica, err := s.repo.GetReplica(ctx, experiment.ReplicaID); err == nil && replica != nil && replica.Version != endpoint.Version {
			return experimentError(fmt.Sprintf("endpoint moved from version %d to %d during the experiment; re-run against the new version", replica.Version, endpoint.Version)), nil
		}
		author := experiment.Author
		if author == "" {
			author = experiment.ID
		}
		revision := &types.EndpointConfigRevision{
			EndpointID: experiment.EndpointID,
			Scope:      types.ConfigScopeTarget,
			ScopeKey:   fleetKey(experiment.Role, experiment.GPU, endpoint.Version),
			Config:     config,
			Author:     liveAuthor(endpoint.Version, author),
			Source:     types.ConfigSourceLive,
		}
		if err := s.repo.CreateConfigRevision(ctx, revision); err != nil {
			return experimentError(err.Error()), nil
		}
		s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
			EndpointID: experiment.EndpointID, Action: "config.fleet", Version: endpoint.Version,
			Role: experiment.Role, GPU: experiment.GPU, Revision: revision.Revision, Experiment: experiment.ID,
			Data: map[string]any{"source": "live", "author": author, "config": config},
		})
	}

	if replica, err := s.repo.GetReplica(ctx, experiment.ReplicaID); err == nil && replica != nil && !replica.Status.Terminal() {
		_ = s.controller.drainReplica(ctx, replica, 0, false, "experiment "+string(outcome))
	}
	_ = s.repo.DeleteConfigRevisions(ctx, experiment.EndpointID, types.ConfigScopeReplica, experiment.ReplicaID)

	experiment.Outcome = outcome
	experiment.Notes = in.Notes
	experiment.EndedAt = time.Now()
	if err := s.saveExperiment(ctx, experiment); err != nil {
		return experimentError(err.Error()), nil
	}
	if err := s.repo.ReleaseExperimentLock(ctx, experiment.EndpointID, experiment.ID); err != nil && !errors.Is(err, errNotFound) {
		return experimentError(err.Error()), nil
	}
	s.emit(types.EventEndpointExperiment, types.EventEndpointSchema{
		EndpointID: experiment.EndpointID, Action: "experiment." + string(outcome), Experiment: experiment.ID,
		ReplicaID: experiment.ReplicaID, GPU: experiment.GPU, Role: experiment.Role, Message: in.Notes,
	})
	return s.experimentResponse(ctx, experiment), nil
}
