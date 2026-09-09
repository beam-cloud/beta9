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
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// EndpointAdminService: cluster-admin RPCs for tuning agents and the frontend,
// mirrored as REST under /api/v1/endpoints. Domain errors are ok=false with
// err_msg; a gRPC status is only returned for auth failures.

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

func (s *Service) endpoint(ctx context.Context, id string) (*types.ManagedEndpoint, error) {
	endpoint, err := s.repo.GetEndpoint(ctx, id)
	if err == nil && endpoint == nil {
		err = fmt.Errorf("endpoint %s: %w", id, errNotFound)
	}
	return endpoint, err
}

func (s *Service) replica(ctx context.Context, id string) (*types.EndpointReplica, error) {
	replica, err := s.repo.GetReplica(ctx, id)
	if err == nil && replica == nil {
		err = fmt.Errorf("replica %s: %w", id, errNotFound)
	}
	return replica, err
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

func (s *Service) ListEndpoints(ctx context.Context, _ *pb.ListEndpointsRequest) (*pb.ListEndpointsResponse, error) {
	out := &pb.ListEndpointsResponse{}
	return admin(s, ctx, out, func() error {
		endpoints, err := s.repo.ListEndpoints(ctx)
		if err != nil {
			return err
		}
		fleet, err := s.repo.GetFleet(ctx)
		if err != nil {
			return err
		}
		replicas, err := s.repo.ListAllReplicas(ctx)
		for _, endpoint := range endpoints {
			out.Endpoints = append(out.Endpoints, endpointToProto(endpoint, fleet, replicas))
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
		fleet, err := s.repo.GetFleet(ctx)
		if err != nil {
			return err
		}
		replicas, err := s.repo.ListReplicas(ctx, endpoint.Spec.ID)
		out.Endpoint, out.Replicas = endpointToProto(endpoint, fleet, replicas), replicasToProto(replicas)
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
			if (in.Status == "" || string(r.Status) == in.Status) && (gpu == "" || r.GPU == gpu) {
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
		replicas, err := s.repo.ListReplicas(ctx, in.EndpointId)
		if err != nil {
			return err
		}
		replicas = slices.DeleteFunc(replicas, func(r *types.EndpointReplica) bool {
			return r.Status.Terminal() || (gpu != "" && r.GPU != gpu) || (in.ReplicaId != "" && r.ID != in.ReplicaId)
		})
		if in.ReplicaId != "" {
			if len(replicas) != 1 {
				return fmt.Errorf("replica %s: %w", in.ReplicaId, errNotFound)
			}
			gpu = replicas[0].GPU
		}
		metrics, err := s.repo.GetRouteMetrics(ctx, in.EndpointId, gpu, in.ReplicaId, in.ConfigRevision, window)
		if err != nil {
			return err
		}
		out.Metrics, out.Replicas = routeMetricsToProto(metrics, replicas), replicasToProto(replicas)
		return err
	})
}

// SetReplicaConfig pushes a live config to one replica and waits for the harness to ack it.
func (s *Service) SetReplicaConfig(ctx context.Context, in *pb.SetReplicaConfigRequest) (*pb.SetReplicaConfigResponse, error) {
	out := &pb.SetReplicaConfigResponse{}
	return admin(s, ctx, out, func() error {
		replica, err := s.replica(ctx, in.ReplicaId)
		if err != nil {
			return err
		}
		if !replica.Alive() {
			return fmt.Errorf("replica is %s", replica.Status)
		}
		if !replica.HarnessEnabled {
			return errors.New("replica has no harness; live config is unavailable")
		}
		var config map[string]any
		if err := json.Unmarshal([]byte(in.ConfigJson), &config); err != nil || config == nil {
			return errors.New("config_json must be a JSON object")
		}
		actor := "cluster-admin"
		if info, ok := auth.AuthInfoFromContext(ctx); ok && info.Token != nil {
			actor = info.Token.ExternalId
		}
		replica, err = s.updateReplica(ctx, replica.ID, func(r *types.EndpointReplica) {
			r.Config.Revision++
			r.Config.Config, r.Config.Author, r.Config.Actor, r.Config.SetAt = json.RawMessage(mustJSON(config)), cmp.Or(in.Author, "admin"), actor, time.Now()
		})
		if err != nil {
			return err
		}
		// The harness picks the revision up on wakeup or its next keepalive read.
		s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
			EndpointID: replica.EndpointID, Action: "config.set", ReplicaID: replica.ID, ContainerID: replica.ContainerID, GPU: replica.GPU, Version: replica.Version,
			Revision: replica.Config.Revision, Data: map[string]any{"author": replica.Config.Author, "actor": actor, "config": config},
		})
		if err := s.repo.NotifyReplicaConfig(ctx, replica.ID, replica.Config.Revision); err != nil {
			log.Warn().Err(err).Str("replica_id", replica.ID).Msg("managed endpoints: config wakeup failed; harness keepalive will pick it up")
		}
		wait := time.Duration(in.WaitSeconds) * time.Second
		if wait <= 0 {
			wait = defaultAckWait
		}
		waitFor(ctx, min(wait, maxAckWait), func() bool {
			current, err := s.repo.GetReplica(ctx, replica.ID)
			if err != nil || current == nil {
				return true
			}
			replica = current
			return current.Config.Acked()
		})
		out.Replica = replicaToProto(replica)
		return nil
	})
}

// StopReplica drains and stops a replica; the controller refills on its next tick.
func (s *Service) StopReplica(ctx context.Context, in *pb.StopReplicaRequest) (*pb.StopReplicaResponse, error) {
	out := &pb.StopReplicaResponse{}
	return admin(s, ctx, out, func() error {
		replica, err := s.replica(ctx, in.ReplicaId)
		if err != nil {
			return err
		}
		if err := s.controller.drainReplica(ctx, replica, in.DrainSeconds, false, "stopped by admin"); err != nil {
			return err
		}
		out.Replica = replicaToProto(replica)
		return nil
	})
}

func (s *Service) GetGitOpsStatus(ctx context.Context, _ *pb.GetGitOpsStatusRequest) (*pb.GetGitOpsStatusResponse, error) {
	out := &pb.GetGitOpsStatusResponse{}
	return admin(s, ctx, out, func() error {
		state, err := s.repo.GetGitOpsState(ctx)
		if err != nil {
			return err
		}
		if state == nil {
			state = &types.GitOpsState{RepoURL: s.config.Repo.URL, Ref: s.config.Repo.Ref}
		}
		fleet, err := s.repo.GetFleet(ctx)
		if err != nil {
			return err
		}
		out.State, out.FleetJson = gitopsToProto(state), mustJSON(fleet.Endpoints)
		return nil
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

var jsonMarshaler = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}

// mountAdminRoutes exposes the admin RPCs as JSON under group.
func (s *Service) mountAdminRoutes(group *echo.Group) {
	g := group.Group("", func(next echo.HandlerFunc) echo.HandlerFunc { return auth.WithClusterAdminAuth(next) })
	id := func(c echo.Context) string { return pathParam(c, "id") }
	list := rest(s.ListEndpoints, nil)

	g.GET("", list)
	g.GET("/", list)
	g.GET("/gitops", rest(s.GetGitOpsStatus, nil))
	g.POST("/gitops/sync", rest(s.TriggerGitOpsSync, nil))
	g.GET("/replicas", rest(s.ListReplicas, func(c echo.Context, in *pb.ListReplicasRequest) {
		in.Status, in.Gpu = c.QueryParam("status"), c.QueryParam("gpu")
	}))
	g.POST("/replicas/:replica/config", rest(s.SetReplicaConfig, func(c echo.Context, in *pb.SetReplicaConfigRequest) { in.ReplicaId = pathParam(c, "replica") }))
	g.POST("/replicas/:replica/stop", rest(s.StopReplica, func(c echo.Context, in *pb.StopReplicaRequest) { in.ReplicaId = pathParam(c, "replica") }))

	// Endpoint ids contain a "/", so callers send acme%2Fmodel and pathParam unescapes it.
	g.GET("/:id", rest(s.GetEndpoint, func(c echo.Context, in *pb.GetEndpointRequest) { in.EndpointId = id(c) }))
	g.GET("/:id/replicas", rest(s.ListReplicas, func(c echo.Context, in *pb.ListReplicasRequest) {
		in.EndpointId, in.Status, in.Gpu = id(c), c.QueryParam("status"), c.QueryParam("gpu")
	}))
	g.GET("/:id/metrics", rest(s.GetMetrics, func(c echo.Context, in *pb.GetMetricsRequest) {
		in.EndpointId, in.Gpu, in.ReplicaId, in.WindowSeconds = id(c), c.QueryParam("gpu"), c.QueryParam("replica_id"), queryUint(c, "window_seconds")
		in.ConfigRevision = uint64(queryUint(c, "config_revision"))
	}))
}

// rest adapts a gRPC handler to an echo route; fill copies path/query params
// over the bound request body.
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
