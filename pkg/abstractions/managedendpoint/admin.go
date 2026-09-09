package managedendpoint

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// EndpointAdminService: cluster-admin RPCs used by tuning agents and the
// frontend. Domain errors come back as ok=false with err_msg; a gRPC status is
// only returned for auth failures. The same handlers are mirrored as REST
// under /api/v1/endpoints (see mountAdminRoutes).

const (
	defaultAckWait    = 30 * time.Second
	maxAckWait        = 5 * time.Minute
	defaultMetricsWin = 5 * time.Minute
	pollStep          = 500 * time.Millisecond
)

// adminResponse is satisfied by every admin response message.
type adminResponse interface {
	proto.Message
	GetOk() bool
	GetErrMsg() string
}

// admin runs fn under cluster-admin auth and maps its error onto out's
// ok/err_msg fields.
func admin[T adminResponse](s *Service, ctx context.Context, out T, fn func() error) (T, error) {
	if err := s.authorizeAdmin(ctx); err != nil {
		var zero T
		return zero, err
	}
	err := fn()
	m := out.ProtoReflect()
	fields := m.Descriptor().Fields()
	m.Set(fields.ByName("ok"), protoreflect.ValueOfBool(err == nil))
	if err != nil {
		m.Set(fields.ByName("err_msg"), protoreflect.ValueOfString(err.Error()))
	}
	return out, nil
}

var errEndpointNotFound = errors.New("endpoint not found")

func (s *Service) endpoint(ctx context.Context, id string) (*types.ManagedEndpoint, error) {
	endpoint, err := s.repo.GetEndpoint(ctx, id)
	if err == nil && endpoint == nil {
		err = errEndpointNotFound
	}
	return endpoint, err
}

func (s *Service) rolloutProto(ctx context.Context, endpointID string) (*pb.RolloutState, error) {
	rollout, err := s.repo.GetRollout(ctx, endpointID)
	if err != nil {
		return nil, err
	}
	versions, err := s.repo.ListVersions(ctx, endpointID)
	return rolloutToProto(rollout, versions), err
}

// waitFor polls done until it returns true, wait elapses or ctx ends.
func waitFor(ctx context.Context, wait time.Duration, done func() bool) bool {
	deadline := time.Now().Add(wait)
	for !done() {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(pollStep):
		}
	}
	return true
}

// --- endpoints ---------------------------------------------------------------

func (s *Service) ListEndpoints(ctx context.Context, in *pb.ListEndpointsRequest) (*pb.ListEndpointsResponse, error) {
	out := &pb.ListEndpointsResponse{}
	return admin(s, ctx, out, func() error {
		endpoints, err := s.repo.ListEndpoints(ctx)
		if err != nil {
			return err
		}
		replicas, err := s.repo.ListAllReplicas(ctx)
		for _, endpoint := range endpoints {
			if in.IncludeDisabled || endpoint.Enabled {
				out.Endpoints = append(out.Endpoints, endpointToProto(endpoint, replicas))
			}
		}
		return err
	})
}

func (s *Service) GetEndpoint(ctx context.Context, in *pb.GetEndpointRequest) (*pb.GetEndpointResponse, error) {
	out := &pb.GetEndpointResponse{}
	return admin(s, ctx, out, func() error {
		endpoint, err := s.endpoint(ctx, in.EndpointId)
		if err != nil {
			return err
		}
		replicas, err := s.repo.ListReplicas(ctx, endpoint.Spec.ID)
		if err != nil {
			return err
		}
		out.Endpoint, out.Replicas = endpointToProto(endpoint, replicas), replicasToProto(replicas)
		out.Rollout, err = s.rolloutProto(ctx, endpoint.Spec.ID)
		return err
	})
}

func (s *Service) ListReplicas(ctx context.Context, in *pb.ListReplicasRequest) (*pb.ListReplicasResponse, error) {
	out := &pb.ListReplicasResponse{}
	return admin(s, ctx, out, func() error {
		var replicas []*types.EndpointReplica
		var err error
		if in.EndpointId == "" {
			replicas, err = s.repo.ListAllReplicas(ctx)
		} else {
			replicas, err = s.repo.ListReplicas(ctx, in.EndpointId)
		}
		gpu := normalizeGPUKey(in.Gpu)
		for _, r := range replicas {
			if (in.Status == "" || string(r.Status) == in.Status) && (gpu == "" || r.GPU == gpu) && (in.Role == "" || r.Role == in.Role) {
				out.Replicas = append(out.Replicas, replicaToProto(r))
			}
		}
		return err
	})
}

func (s *Service) GetMetrics(ctx context.Context, in *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	out := &pb.GetMetricsResponse{}
	return admin(s, ctx, out, func() error {
		window := cmp.Or(time.Duration(in.WindowSeconds)*time.Second, defaultMetricsWin)
		gpu := normalizeGPUKey(in.Gpu)
		metrics, err := s.repo.GetRouteMetrics(ctx, in.EndpointId, gpu, 0, window)
		if err != nil {
			return err
		}
		replicas, err := s.repo.ListReplicas(ctx, in.EndpointId)
		replicas = slices.DeleteFunc(replicas, func(r *types.EndpointReplica) bool {
			return r.Status.Terminal() || (gpu != "" && r.GPU != gpu) || (in.ReplicaId != "" && r.ID != in.ReplicaId)
		})
		out.Metrics, out.Replicas = routeMetricsToProto(metrics, replicas), replicasToProto(replicas)
		return err
	})
}

func (s *Service) SetEndpointEnabled(ctx context.Context, in *pb.SetEndpointEnabledRequest) (*pb.SetEndpointEnabledResponse, error) {
	out := &pb.SetEndpointEnabledResponse{}
	return admin(s, ctx, out, func() error {
		endpoint, err := s.endpoint(ctx, in.EndpointId)
		if err != nil {
			return err
		}
		if endpoint.Enabled != in.Enabled {
			endpoint.Enabled, endpoint.Status, endpoint.UpdatedAt = in.Enabled, types.EndpointStatusDisabled, time.Now()
			action := "endpoint.disabled"
			if in.Enabled {
				endpoint.Status, action = types.EndpointStatusActive, "endpoint.enabled"
			}
			if err := s.repo.SaveEndpoint(ctx, endpoint); err != nil {
				return err
			}
			s.emit(types.EventEndpointConfig, types.EventEndpointSchema{EndpointID: endpoint.Spec.ID, Action: action, Version: endpoint.Version})
		}
		replicas, err := s.repo.ListReplicas(ctx, endpoint.Spec.ID)
		out.Endpoint = endpointToProto(endpoint, replicas)
		return err
	})
}

func (s *Service) ListServices(ctx context.Context, _ *pb.ListServicesRequest) (*pb.ListServicesResponse, error) {
	out := &pb.ListServicesResponse{}
	return admin(s, ctx, out, func() error {
		services, err := s.repo.ListServices(ctx)
		if err != nil {
			return err
		}
		replicas, err := s.repo.ListAllReplicas(ctx)
		for _, service := range services {
			out.Services = append(out.Services, serviceToProto(service, replicas))
		}
		return err
	})
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
	role, gpu, ok := strings.Cut(target, ":")
	if !ok {
		role, gpu = types.ReplicaRoleServe, target
	}
	if version == 0 {
		endpoint, err := s.endpoint(ctx, endpointID)
		if err != nil {
			return "", "", err
		}
		version = endpoint.Version
	}
	return types.ConfigScopeTarget, fleetKey(role, normalizeGPUKey(gpu), version), nil
}

func (s *Service) GetConfig(ctx context.Context, in *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
	out := &pb.GetConfigResponse{}
	return admin(s, ctx, out, func() error {
		scope, key, err := s.scopeKey(ctx, in.EndpointId, in.Scope, in.ScopeKey)
		if err != nil {
			return err
		}
		revision, err := s.repo.LatestConfigRevision(ctx, in.EndpointId, scope, key)
		if err == nil && revision == nil {
			return errors.New("no config revision")
		}
		out.Revision = revisionToProto(revision)
		return err
	})
}

func (s *Service) ListConfigRevisions(ctx context.Context, in *pb.ListConfigRevisionsRequest) (*pb.ListConfigRevisionsResponse, error) {
	out := &pb.ListConfigRevisionsResponse{}
	return admin(s, ctx, out, func() error {
		scope, key, err := s.scopeKey(ctx, in.EndpointId, in.Scope, in.ScopeKey)
		if err != nil {
			return err
		}
		revisions, err := s.repo.ListConfigRevisions(ctx, in.EndpointId, scope, key, int(cmp.Or(in.Limit, 20)))
		for _, r := range revisions {
			out.Revisions = append(out.Revisions, revisionToProto(r))
		}
		return err
	})
}

// SetConfig publishes a live harness config revision. Target scope updates
// the fleet for the active version; replica scope updates one replica and
// waits for its harness ack.
func (s *Service) SetConfig(ctx context.Context, in *pb.SetConfigRequest) (*pb.SetConfigResponse, error) {
	out := &pb.SetConfigResponse{}
	return admin(s, ctx, out, func() error {
		endpoint, err := s.endpoint(ctx, in.EndpointId)
		if err != nil {
			return err
		}
		if !endpoint.Spec.Harness.Enabled {
			return errors.New("endpoint does not enable the harness; live config is unavailable")
		}
		scope, key, err := s.scopeKey(ctx, endpoint.Spec.ID, in.Scope, in.ScopeKey)
		if err != nil {
			return err
		}
		var config map[string]any
		if err := json.Unmarshal([]byte(in.ConfigJson), &config); err != nil || config == nil {
			return errors.New("config_json must be a JSON object")
		}
		if scope == types.ConfigScopeReplica {
			replica, err := s.repo.GetReplica(ctx, key)
			if err != nil {
				return err
			}
			if replica == nil || replica.EndpointID != endpoint.Spec.ID || !replica.Alive() {
				return fmt.Errorf("replica %q is not a live replica of %s", key, endpoint.Spec.ID)
			}
		}
		revision := &types.EndpointConfigRevision{
			EndpointID: endpoint.Spec.ID, Scope: scope, ScopeKey: key, Config: config,
			Author: cmp.Or(in.Author, "admin"), Source: types.ConfigSourceLive,
		}
		if err := s.repo.CreateConfigRevision(ctx, revision); err != nil {
			return err
		}
		out.Revision = revisionToProto(revision)
		s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
			EndpointID: endpoint.Spec.ID, Action: "config." + string(scope), Version: endpoint.Version,
			Revision: revision.Revision, Data: map[string]any{"scope_key": key, "author": revision.Author, "config": config},
		})
		if scope != types.ConfigScopeReplica {
			return nil
		}
		wait := min(cmp.Or(time.Duration(in.WaitSeconds)*time.Second, defaultAckWait), maxAckWait)
		out.Acked = waitFor(ctx, wait, func() bool {
			ack, err := s.repo.GetConfigAck(ctx, key, revision.Revision)
			if err != nil || ack == nil {
				return false
			}
			out.Applied, out.ApplyError = ack.Applied, ack.Error
			return true
		})
		return nil
	})
}

// --- rollouts ----------------------------------------------------------------

func (s *Service) PromoteRollout(ctx context.Context, in *pb.PromoteRolloutRequest) (*pb.RolloutActionResponse, error) {
	return s.rolloutAction(ctx, in.EndpointId, uint(in.Version), true)
}

func (s *Service) RollbackRollout(ctx context.Context, in *pb.RollbackRolloutRequest) (*pb.RolloutActionResponse, error) {
	return s.rolloutAction(ctx, in.EndpointId, uint(in.Version), false)
}

// rolloutAction promotes or rolls back. With a baking canary the action
// decides it; otherwise "promote <version>" re-activates a retired version
// and "rollback" re-activates the most recent retired version.
func (s *Service) rolloutAction(ctx context.Context, endpointID string, version uint, promote bool) (*pb.RolloutActionResponse, error) {
	out := &pb.RolloutActionResponse{}
	return admin(s, ctx, out, func() error {
		endpoint, err := s.endpoint(ctx, endpointID)
		if err != nil {
			return err
		}
		rollout, err := s.repo.GetRollout(ctx, endpointID)
		if err != nil {
			return err
		}
		versions, err := s.repo.ListVersions(ctx, endpointID)
		if err != nil {
			return err
		}
		if rollout == nil {
			rollout = &types.RolloutState{EndpointID: endpointID, ActiveVersion: endpoint.Version, Phase: types.RolloutPhaseIdle}
		}
		reason := "manual rollback by admin"
		if promote {
			reason = "manual promote by admin"
		}
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
				return errors.New("no previous version to roll back to")
			}
			err = s.controller.activateVersion(ctx, endpoint, rollout, versions, target, reason)
		default:
			return errors.New("nothing to promote")
		}
		if err != nil {
			return err
		}
		out.Rollout, err = s.rolloutProto(ctx, endpointID)
		return err
	})
}

func (s *Service) PinVersion(ctx context.Context, in *pb.PinVersionRequest) (*pb.RolloutActionResponse, error) {
	out := &pb.RolloutActionResponse{}
	return admin(s, ctx, out, func() error {
		rollout, err := s.repo.GetRollout(ctx, in.EndpointId)
		if err != nil {
			return err
		}
		if rollout == nil {
			return errEndpointNotFound
		}
		versions, err := s.repo.ListVersions(ctx, in.EndpointId)
		if err != nil {
			return err
		}
		if in.Version != 0 && findVersion(versions, uint(in.Version)) == nil {
			return fmt.Errorf("version %d not found", in.Version)
		}
		rollout.PinnedVersion = uint(in.Version)
		rollout.LastDecision, rollout.LastDecisionAt = "unpinned", time.Now()
		if in.Version != 0 {
			rollout.LastDecision = fmt.Sprintf("pinned version %d", in.Version)
		}
		if err := s.repo.SaveRollout(ctx, rollout); err != nil {
			return err
		}
		s.emit(types.EventEndpointRollout, types.EventEndpointSchema{EndpointID: in.EndpointId, Action: "rollout.pinned", Version: uint(in.Version)})
		out.Rollout = rolloutToProto(rollout, versions)
		return nil
	})
}

// --- gitops ------------------------------------------------------------------

func (s *Service) GetGitOpsStatus(ctx context.Context, _ *pb.GetGitOpsStatusRequest) (*pb.GetGitOpsStatusResponse, error) {
	out := &pb.GetGitOpsStatusResponse{}
	return admin(s, ctx, out, func() error {
		state, err := s.repo.GetGitOpsState(ctx)
		if state == nil {
			state = &types.GitOpsState{RepoURL: s.config.Repo.URL, Ref: s.config.Repo.Ref}
		}
		out.State = gitopsToProto(state)
		return err
	})
}

func (s *Service) TriggerGitOpsSync(ctx context.Context, in *pb.TriggerGitOpsSyncRequest) (*pb.TriggerGitOpsSyncResponse, error) {
	out := &pb.TriggerGitOpsSyncResponse{}
	return admin(s, ctx, out, func() (err error) {
		if s.gitops == nil {
			return errors.New("gitops is not configured (managedEndpoints.repo.url)")
		}
		out.Started, err = s.gitops.Trigger(in.Sha)
		return err
	})
}

// --- tuning ------------------------------------------------------------------

// StartTuningReplica starts one dedicated, protected replica for a target.
// It never receives public traffic; agents address it with
// X-Beam-Endpoint-Replica and push configs with replica-scoped SetConfig.
func (s *Service) StartTuningReplica(ctx context.Context, in *pb.StartTuningReplicaRequest) (*pb.ReplicaResponse, error) {
	out := &pb.ReplicaResponse{}
	return admin(s, ctx, out, func() error {
		endpoint, err := s.endpoint(ctx, in.EndpointId)
		if err != nil {
			return err
		}
		if !endpoint.Spec.Harness.Enabled {
			return errors.New("endpoint does not enable the harness; live tuning is unavailable")
		}
		role, gpu := cmp.Or(strings.TrimSpace(in.Role), types.ReplicaRoleServe), normalizeGPUKey(in.Gpu)
		targets := endpoint.Spec.Targets()
		idx := slices.IndexFunc(targets, func(rt types.RoleTarget) bool { return rt.Role == role && rt.Target.Key() == gpu })
		if idx < 0 {
			return fmt.Errorf("endpoint has no target %s on %s", role, gpu)
		}
		live, err := s.repo.ListAllReplicas(ctx)
		if err != nil {
			return err
		}
		for _, r := range live {
			if r.EndpointID == endpoint.Spec.ID && r.Tuning && r.Alive() {
				return fmt.Errorf("tuning replica %s is already running; stop it first", r.ID)
			}
		}
		services, missing := serviceAddresses(endpoint.Spec.Services, live)
		if len(missing) > 0 {
			return fmt.Errorf("required services not ready: %s", strings.Join(missing, ", "))
		}
		spec := s.controller.endpointStartSpec(endpoint, targets[idx], services)
		spec.Protected, spec.Tuning, spec.Evictable = true, true, false
		replica, err := s.controller.startReplica(ctx, spec)
		if err != nil {
			return err
		}
		waitFor(ctx, time.Duration(in.WaitSeconds)*time.Second, func() bool {
			current, err := s.repo.GetReplica(ctx, replica.ID)
			if err != nil || current == nil {
				return true
			}
			replica = current
			return replica.Status == types.ReplicaStatusReady || replica.Status.Terminal()
		})
		out.Replica = replicaToProto(replica)
		return nil
	})
}

// StopReplica drains and stops any replica; the controller refills the
// target on its next tick if the endpoint still wants the capacity.
func (s *Service) StopReplica(ctx context.Context, in *pb.StopReplicaRequest) (*pb.ReplicaResponse, error) {
	out := &pb.ReplicaResponse{}
	return admin(s, ctx, out, func() error {
		replica, err := s.repo.GetReplica(ctx, in.ReplicaId)
		if err != nil {
			return err
		}
		if replica == nil {
			return errors.New("replica not found")
		}
		if err := s.controller.drainReplica(ctx, replica, in.DrainSeconds, false, "stopped by admin"); err != nil {
			return err
		}
		if replica.Tuning {
			_ = s.repo.DeleteConfigRevisions(ctx, replica.EndpointID, types.ConfigScopeReplica, replica.ID)
		}
		out.Replica = replicaToProto(replica)
		return nil
	})
}

// --- REST mirror ---------------------------------------------------------------

var jsonMarshaler = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}

// mountAdminRoutes exposes the admin RPCs as JSON under group. Every route
// delegates to the gRPC method so the two surfaces cannot drift.
func (s *Service) mountAdminRoutes(group *echo.Group) {
	g := group.Group("", func(next echo.HandlerFunc) echo.HandlerFunc { return auth.WithClusterAdminAuth(next) })
	id := func(c echo.Context) string { return pathParam(c, "id") }
	list := rest(s.ListEndpoints, func(c echo.Context, in *pb.ListEndpointsRequest) {
		in.IncludeDisabled, _ = strconv.ParseBool(c.QueryParam("include_disabled"))
	})

	g.GET("", list)
	g.GET("/", list)
	g.GET("/services", rest(s.ListServices, nil))
	g.GET("/gitops", rest(s.GetGitOpsStatus, nil))
	g.POST("/gitops/sync", rest(s.TriggerGitOpsSync, nil))
	g.POST("/replicas/:replica/stop", rest(s.StopReplica, func(c echo.Context, in *pb.StopReplicaRequest) { in.ReplicaId = pathParam(c, "replica") }))

	// Endpoint IDs may contain one "/" (vendor/slug). Echo matches :id on the
	// raw, still-escaped path, so callers send acme%2Fmodel and pathParam
	// unescapes it; see TestAdminRESTEndpointIDWithSlash.
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
	g.POST("/:id/config", rest(s.SetConfig, func(c echo.Context, in *pb.SetConfigRequest) { in.EndpointId = id(c) }))
	g.GET("/:id/config/revisions", rest(s.ListConfigRevisions, func(c echo.Context, in *pb.ListConfigRevisionsRequest) {
		in.EndpointId, in.Scope, in.ScopeKey, in.Limit = id(c), c.QueryParam("scope"), c.QueryParam("scope_key"), queryUint(c, "limit")
	}))
	g.POST("/:id/rollout/promote", rest(s.PromoteRollout, func(c echo.Context, in *pb.PromoteRolloutRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/rollout/rollback", rest(s.RollbackRollout, func(c echo.Context, in *pb.RollbackRolloutRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/rollout/pin", rest(s.PinVersion, func(c echo.Context, in *pb.PinVersionRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/enabled", rest(s.SetEndpointEnabled, func(c echo.Context, in *pb.SetEndpointEnabledRequest) { in.EndpointId = id(c) }))
	g.POST("/:id/tuning", rest(s.StartTuningReplica, func(c echo.Context, in *pb.StartTuningReplicaRequest) { in.EndpointId = id(c) }))
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
		ctx := c.Request().Context()
		if cc, ok := c.(*auth.HttpAuthContext); ok && cc.AuthInfo != nil {
			ctx = auth.ContextWithAuthInfo(ctx, cc.AuthInfo)
		}
		out, err := method(ctx, in)
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

// httpError maps a gRPC status onto an HTTP error.
func httpError(err error) error {
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated:
		return echo.NewHTTPError(http.StatusUnauthorized, status.Convert(err).Message())
	case codes.NotFound:
		return echo.NewHTTPError(http.StatusNotFound, status.Convert(err).Message())
	case codes.FailedPrecondition, codes.InvalidArgument:
		return echo.NewHTTPError(http.StatusBadRequest, status.Convert(err).Message())
	}
	return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
}

// pathParam decodes a path segment; endpoint ids contain "/".
func pathParam(c echo.Context, name string) string {
	if decoded, err := url.PathUnescape(c.Param(name)); err == nil {
		return decoded
	}
	return c.Param(name)
}

func queryUint(c echo.Context, name string) uint32 {
	v, _ := strconv.ParseUint(c.QueryParam(name), 10, 32)
	return uint32(v)
}
