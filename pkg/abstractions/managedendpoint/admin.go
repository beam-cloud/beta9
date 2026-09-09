package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// EndpointAdminService: cluster-admin RPCs used by tuning agents and the
// frontend. Every handler returns ok=false with err_msg for domain errors and
// a gRPC status only for auth / transport failures. The same handlers are
// mirrored as REST under /api/v1/endpoints (see mountAdminRoutes).

const (
	defaultAckWait     = 30 * time.Second
	maxAckWait         = 5 * time.Minute
	defaultMetricsWin  = 5 * time.Minute
	experimentLockTTL  = 2 * time.Hour
	experimentPollStep = 500 * time.Millisecond
)

// --- endpoints ---------------------------------------------------------------

func (s *Service) ListEndpoints(ctx context.Context, in *pb.ListEndpointsRequest) (*pb.ListEndpointsResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	endpoints, err := s.repo.ListEndpoints(ctx)
	if err != nil {
		return &pb.ListEndpointsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	replicas, err := s.repo.ListAllReplicas(ctx)
	if err != nil {
		return &pb.ListEndpointsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := &pb.ListEndpointsResponse{Ok: true}
	for _, endpoint := range endpoints {
		if in.IncludeDisabled || endpoint.Enabled {
			out.Endpoints = append(out.Endpoints, endpointToProto(endpoint, replicas))
		}
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
	replicas, err := s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return &pb.GetEndpointResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	rollout, versions, err := s.rolloutWithVersions(ctx, endpoint.Spec.ID)
	if err != nil {
		return &pb.GetEndpointResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.GetEndpointResponse{
		Ok:       true,
		Endpoint: endpointToProto(endpoint, replicas),
		Replicas: replicasToProto(replicas),
		Rollout:  rolloutToProto(rollout, versions),
	}, nil
}

func (s *Service) rolloutWithVersions(ctx context.Context, endpointID string) (*types.RolloutState, []*types.EndpointVersion, error) {
	rollout, err := s.repo.GetRollout(ctx, endpointID)
	if err != nil {
		return nil, nil, err
	}
	versions, err := s.repo.ListVersions(ctx, endpointID)
	return rollout, versions, err
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
	gpu := normalizeGPUKey(in.Gpu)
	out := &pb.ListReplicasResponse{Ok: true}
	for _, r := range replicas {
		if (in.Status == "" || string(r.Status) == in.Status) && (gpu == "" || r.GPU == gpu) && (in.Role == "" || r.Role == in.Role) {
			out.Replicas = append(out.Replicas, replicaToProto(r))
		}
	}
	return out, nil
}

func (s *Service) GetMetrics(ctx context.Context, in *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	window := time.Duration(in.WindowSeconds) * time.Second
	if window <= 0 {
		window = defaultMetricsWin
	}
	gpu := normalizeGPUKey(in.Gpu)
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
		if !r.Status.Terminal() && (gpu == "" || r.GPU == gpu) && (in.ReplicaId == "" || r.ID == in.ReplicaId) {
			selected = append(selected, r)
		}
	}
	return &pb.GetMetricsResponse{Ok: true, Metrics: routeMetricsToProto(metrics, selected), Replicas: replicasToProto(selected)}, nil
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
		action := "endpoint.enabled"
		if !in.Enabled {
			endpoint.Status = types.EndpointStatusDisabled
			action = "endpoint.disabled"
		}
		endpoint.UpdatedAt = time.Now()
		if err := s.repo.SaveEndpoint(ctx, endpoint); err != nil {
			return &pb.SetEndpointEnabledResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		s.emit(types.EventEndpointConfig, types.EventEndpointSchema{EndpointID: endpoint.Spec.ID, Action: action, Version: endpoint.Version})
	}
	replicas, err := s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return &pb.SetEndpointEnabledResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.SetEndpointEnabledResponse{Ok: true, Endpoint: endpointToProto(endpoint, replicas)}, nil
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
		out.Services = append(out.Services, serviceToProto(service, replicas))
	}
	return out, nil
}

// --- config ------------------------------------------------------------------

// scopeKey normalizes an admin-supplied scope. Target keys accept "<gpu>",
// "<role>:<gpu>" and "<role>:<gpu>@v<N>"; without a version suffix they
// resolve to the endpoint's active version.
func (s *Service) scopeKey(ctx context.Context, endpointID, scope, key string) (types.ConfigRevisionScope, string, error) {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "replica":
		return types.ConfigScopeReplica, key, nil
	case "", "target":
	default:
		return "", "", fmt.Errorf("unknown scope %q (target|replica)", scope)
	}
	target, version := parseFleetKey(key)
	role, gpu := types.ReplicaRoleServe, target
	if idx := strings.Index(target, ":"); idx >= 0 {
		role, gpu = target[:idx], target[idx+1:]
	}
	if version == 0 {
		endpoint, err := s.repo.GetEndpoint(ctx, endpointID)
		if err != nil {
			return "", "", err
		}
		if endpoint == nil {
			return "", "", errors.New("endpoint not found")
		}
		version = endpoint.Version
	}
	return types.ConfigScopeTarget, fleetKey(role, normalizeGPUKey(gpu), version), nil
}

func (s *Service) GetConfig(ctx context.Context, in *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		return nil, err
	}
	scope, key, err := s.scopeKey(ctx, in.EndpointId, in.Scope, in.ScopeKey)
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
	scope, key, err := s.scopeKey(ctx, in.EndpointId, in.Scope, in.ScopeKey)
	if err != nil {
		return &pb.ListConfigRevisionsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	limit := int(in.Limit)
	if limit <= 0 {
		limit = 20
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
	var (
		spec      any
		validated error
	)
	switch kind := strings.ToLower(strings.TrimSpace(in.Kind)); kind {
	case "", "endpoint":
		var v types.ManagedEndpointSpec
		if err := json.Unmarshal([]byte(in.SpecJson), &v); err != nil {
			return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: "invalid spec json", Errors: []string{err.Error()}}, nil
		}
		v.Normalize()
		spec, validated = v, v.Validate(s.validationPolicy(ctx))
	case "service":
		var v types.ManagedServiceSpec
		if err := json.Unmarshal([]byte(in.SpecJson), &v); err != nil {
			return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: "invalid spec json", Errors: []string{err.Error()}}, nil
		}
		v.Normalize()
		spec, validated = v, v.Validate()
	default:
		return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: fmt.Sprintf("unknown kind %q (endpoint|service)", in.Kind)}, nil
	}
	if validated != nil {
		// errors.Join renders one error per line.
		return &pb.ValidateEndpointSpecResponse{Ok: false, ErrMsg: validated.Error(), Errors: strings.Split(validated.Error(), "\n")}, nil
	}
	return &pb.ValidateEndpointSpecResponse{Ok: true, SpecJson: mustJSON(spec)}, nil
}

// --- rollouts ----------------------------------------------------------------

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
	fail := func(msg string) (*pb.RolloutActionResponse, error) {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: msg}, nil
	}
	endpoint, err := s.repo.GetEndpoint(ctx, endpointID)
	if err != nil || endpoint == nil {
		return fail("endpoint not found")
	}
	rollout, versions, err := s.rolloutWithVersions(ctx, endpointID)
	if err != nil {
		return fail(err.Error())
	}
	if rollout == nil {
		rollout = &types.RolloutState{EndpointID: endpointID, ActiveVersion: endpoint.Version, Phase: types.RolloutPhaseIdle}
	}
	verb := "rollback"
	if promote {
		verb = "promote"
	}
	reason := "manual " + verb + " by admin"

	switch {
	case rollout.CanaryVersion != 0 && (version == 0 || version == rollout.CanaryVersion):
		err = s.controller.finishRollout(ctx, endpoint, rollout, promote, reason)
	case promote && version != 0 && version != endpoint.Version:
		err = s.controller.activateVersion(ctx, endpoint, rollout, versions, version, reason)
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
			return fail("no previous version to roll back to")
		}
		err = s.controller.activateVersion(ctx, endpoint, rollout, versions, target, reason)
	default:
		return fail("nothing to promote")
	}
	if err != nil {
		return fail(err.Error())
	}
	rollout, versions, err = s.rolloutWithVersions(ctx, endpointID)
	if err != nil {
		return fail(err.Error())
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
	if in.Version != 0 && findVersion(versions, uint(in.Version)) == nil {
		return &pb.RolloutActionResponse{Ok: false, ErrMsg: fmt.Sprintf("version %d not found", in.Version)}, nil
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

// --- gitops ------------------------------------------------------------------

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
	started, err := s.gitops.Trigger(in.Sha)
	if err != nil {
		return &pb.TriggerGitOpsSyncResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.TriggerGitOpsSyncResponse{Ok: true, Started: started}, nil
}

// --- experiments ---------------------------------------------------------------

func experimentError(msg string) *pb.ExperimentResponse {
	return &pb.ExperimentResponse{Ok: false, ErrMsg: msg}
}

func (s *Service) experimentResponse(ctx context.Context, experiment *types.Experiment) *pb.ExperimentResponse {
	var replica *types.EndpointReplica
	if experiment.ReplicaID != "" {
		replica, _ = s.repo.GetReplica(ctx, experiment.ReplicaID)
	}
	return &pb.ExperimentResponse{Ok: true, Experiment: experimentToProto(experiment, replica)}
}

func (s *Service) saveExperiment(ctx context.Context, experiment *types.Experiment) error {
	return s.repo.SaveExperiment(ctx, experiment, s.config.Tuning.ExperimentTTLOrDefault(), s.config.Tuning.KeepExperimentsOrDefault())
}

// runningExperiment loads an experiment that is still open.
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
			target = &rt
			break
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
	release := func() { _ = s.repo.ReleaseExperimentLock(ctx, endpoint.Spec.ID, experimentID) }

	baseline, err := s.repo.LatestConfigRevision(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, fleetKey(role, gpu, endpoint.Version))
	if err != nil {
		release()
		return experimentError(err.Error()), nil
	}
	var baselineRevision uint64
	if baseline != nil {
		baselineRevision = baseline.Revision
	}

	replicas, _ := s.repo.ListAllReplicas(ctx)
	services, missing := serviceAddresses(endpoint.Spec.Services, replicas)
	if len(missing) > 0 {
		release()
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
		release()
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
		s.waitFor(ctx, time.Duration(in.WaitSeconds)*time.Second, func() bool {
			r, err := s.repo.GetReplica(ctx, replica.ID)
			return err == nil && r != nil && (r.Status == types.ReplicaStatusReady || r.Status.Terminal())
		})
	}
	return s.experimentResponse(ctx, experiment), nil
}

// waitFor polls done until it returns true, the wait elapses or ctx ends.
func (s *Service) waitFor(ctx context.Context, wait time.Duration, done func() bool) bool {
	deadline := time.Now().Add(wait)
	for {
		if done() {
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(experimentPollStep):
		}
	}
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
	gpu := normalizeGPUKey(in.Gpu)
	out := &pb.ListExperimentsResponse{Ok: true}
	for _, e := range experiments {
		if gpu == "" || e.GPU == gpu {
			out.Experiments = append(out.Experiments, experimentToProto(e, nil))
		}
	}
	return out, nil
}

// applyReplicaConfig publishes a replica-scoped revision and waits for the
// harness ack, recording the outcome as an experiment step.
func (s *Service) applyReplicaConfig(ctx context.Context, experiment *types.Experiment, config map[string]any, wait time.Duration) (*types.ExperimentStep, error) {
	if wait <= 0 {
		wait = defaultAckWait
	}
	wait = min(wait, maxAckWait)
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
	acked := s.waitFor(ctx, wait, func() bool {
		ack, err := s.repo.GetConfigAck(ctx, experiment.ReplicaID, revision.Revision)
		if err != nil || ack == nil {
			return false
		}
		step.Applied, step.Error = ack.Applied, ack.Error
		return true
	})
	if !acked {
		step.Error = "timed out waiting for harness ack"
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
	var replicas []*types.EndpointReplica
	if replica != nil {
		replicas = []*types.EndpointReplica{replica}
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
		window = defaultMetricsWin
	}
	if metrics, err := s.repo.GetRouteMetrics(ctx, experiment.EndpointID, experiment.GPU, 0, window); err == nil {
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
		for i := len(experiment.Steps) - 1; i >= 0 && config == nil; i-- {
			if experiment.Steps[i].Applied {
				config = experiment.Steps[i].Config
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
			Author:     fmt.Sprintf("live@v%d:%s", endpoint.Version, author),
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

// --- REST mirror ---------------------------------------------------------------

// adminResponse is satisfied by every admin response message.
type adminResponse interface {
	proto.Message
	GetOk() bool
	GetErrMsg() string
}

var jsonMarshaler = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}

// mountAdminRoutes exposes the admin RPCs as JSON under group. Every route
// delegates to the gRPC method so the two surfaces cannot drift.
func (s *Service) mountAdminRoutes(group *echo.Group) {
	g := group.Group("", func(next echo.HandlerFunc) echo.HandlerFunc { return auth.WithClusterAdminAuth(next) })
	id := func(c echo.Context) string { return pathParam(c, "id") }
	exp := func(c echo.Context) string { return pathParam(c, "experiment") }

	g.GET("", rest(s.ListEndpoints, func(c echo.Context, in *pb.ListEndpointsRequest) {
		in.IncludeDisabled, _ = strconv.ParseBool(c.QueryParam("include_disabled"))
	}))
	g.GET("/", rest(s.ListEndpoints, func(c echo.Context, in *pb.ListEndpointsRequest) {
		in.IncludeDisabled, _ = strconv.ParseBool(c.QueryParam("include_disabled"))
	}))
	g.GET("/services", rest(s.ListServices, nil))
	g.GET("/gitops", rest(s.GetGitOpsStatus, nil))
	g.POST("/gitops/sync", rest(s.TriggerGitOpsSync, nil))
	g.POST("/validate", rest(s.ValidateEndpointSpec, nil))

	g.GET("/experiments/:experiment", rest(s.GetExperiment, func(c echo.Context, in *pb.GetExperimentRequest) { in.ExperimentId = exp(c) }))
	g.GET("/experiments/:experiment/metrics", rest(s.GetExperimentMetrics, func(c echo.Context, in *pb.GetExperimentMetricsRequest) {
		in.ExperimentId, in.WindowSeconds = exp(c), queryUint(c, "window_seconds")
	}))
	g.POST("/experiments/:experiment/config", rest(s.ApplyExperimentConfig, func(c echo.Context, in *pb.ApplyExperimentConfigRequest) { in.ExperimentId = exp(c) }))
	g.POST("/experiments/:experiment/bench", rest(s.RecordExperimentBench, func(c echo.Context, in *pb.RecordExperimentBenchRequest) { in.ExperimentId = exp(c) }))
	g.POST("/experiments/:experiment/revert", rest(s.RevertExperiment, func(c echo.Context, in *pb.RevertExperimentRequest) { in.ExperimentId = exp(c) }))
	g.POST("/experiments/:experiment/stop", rest(s.StopExperiment, func(c echo.Context, in *pb.StopExperimentRequest) { in.ExperimentId = exp(c) }))

	g.GET("/:id", rest(s.GetEndpoint, func(c echo.Context, in *pb.GetEndpointRequest) { in.EndpointId = id(c) }))
	g.GET("/:id/replicas", rest(s.ListReplicas, func(c echo.Context, in *pb.ListReplicasRequest) {
		in.EndpointId, in.Status, in.Gpu, in.Role = id(c), c.QueryParam("status"), c.QueryParam("gpu"), c.QueryParam("role")
	}))
	g.GET("/:id/metrics", rest(s.GetMetrics, func(c echo.Context, in *pb.GetMetricsRequest) {
		in.EndpointId, in.Gpu, in.ReplicaId, in.WindowSeconds = id(c), c.QueryParam("gpu"), c.QueryParam("replica_id"), queryUint(c, "window_seconds")
	}))
	g.GET("/:id/config", rest(s.GetConfig, func(c echo.Context, in *pb.GetConfigRequest) {
		in.EndpointId, in.Scope, in.ScopeKey = id(c), c.QueryParam("scope"), c.QueryParam("scope_key")
	}))
	g.GET("/:id/config/revisions", rest(s.ListConfigRevisions, func(c echo.Context, in *pb.ListConfigRevisionsRequest) {
		in.EndpointId, in.Scope, in.ScopeKey, in.Limit = id(c), c.QueryParam("scope"), c.QueryParam("scope_key"), queryUint(c, "limit")
	}))
	g.GET("/:id/rollout", rest(s.GetRollout, func(c echo.Context, in *pb.GetRolloutRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/rollout/promote", rest(s.PromoteRollout, func(c echo.Context, in *pb.PromoteRolloutRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/rollout/rollback", rest(s.RollbackRollout, func(c echo.Context, in *pb.RollbackRolloutRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/rollout/pin", rest(s.PinVersion, func(c echo.Context, in *pb.PinVersionRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/enabled", rest(s.SetEndpointEnabled, func(c echo.Context, in *pb.SetEndpointEnabledRequest) { in.EndpointId = id(c) }))
	g.GET("/:id/experiments", rest(s.ListExperiments, func(c echo.Context, in *pb.ListExperimentsRequest) {
		in.EndpointId, in.Gpu, in.Limit = id(c), c.QueryParam("gpu"), queryUint(c, "limit")
	}))
	g.POST("/:id/experiments", rest(s.StartExperiment, func(c echo.Context, in *pb.StartExperimentRequest) { in.EndpointId = id(c) }))
}

// rest adapts a gRPC handler to an echo route: the JSON body (if any) binds
// into the request, fill copies path/query params over it, and the response
// is written as protojson with ok=false mapped to 4xx.
func rest[Req any, PReq interface {
	*Req
	proto.Message
}, Resp adminResponse](method func(context.Context, PReq) (Resp, error), fill func(echo.Context, PReq)) echo.HandlerFunc {
	return func(c echo.Context) error {
		in := PReq(new(Req))
		if c.Request().Method != http.MethodGet {
			body, err := io.ReadAll(io.LimitReader(c.Request().Body, 4<<20))
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, err.Error())
			}
			if len(strings.TrimSpace(string(body))) > 0 {
				if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, in); err != nil {
					return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: "+err.Error())
				}
			}
		}
		if fill != nil {
			fill(c, in)
		}
		out, err := method(requestContext(c), in)
		if err != nil {
			return httpError(err)
		}
		body, err := jsonMarshaler.Marshal(out)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		code := http.StatusOK
		if !out.GetOk() {
			code = http.StatusBadRequest
			if strings.Contains(strings.ToLower(out.GetErrMsg()), "not found") {
				code = http.StatusNotFound
			}
		}
		return c.JSONBlob(code, body)
	}
}

// requestContext carries the echo auth info into the gRPC handlers.
func requestContext(c echo.Context) context.Context {
	if cc, ok := c.(*auth.HttpAuthContext); ok && cc.AuthInfo != nil {
		return auth.ContextWithAuthInfo(c.Request().Context(), cc.AuthInfo)
	}
	return c.Request().Context()
}

// httpError maps a gRPC status onto an HTTP error.
func httpError(err error) error {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.PermissionDenied, codes.Unauthenticated:
			return echo.NewHTTPError(http.StatusUnauthorized, st.Message())
		case codes.NotFound:
			return echo.NewHTTPError(http.StatusNotFound, st.Message())
		case codes.FailedPrecondition, codes.InvalidArgument:
			return echo.NewHTTPError(http.StatusBadRequest, st.Message())
		}
	}
	return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
}

// pathParam decodes a path segment; endpoint ids contain "/".
func pathParam(c echo.Context, name string) string {
	raw := c.Param(name)
	if decoded, err := url.PathUnescape(raw); err == nil {
		return decoded
	}
	return raw
}

func queryUint(c echo.Context, name string) uint32 {
	v, _ := strconv.ParseUint(c.QueryParam(name), 10, 32)
	return uint32(v)
}
