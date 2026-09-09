package managedendpoint

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// REST mirror of EndpointAdminService under /api/v1/endpoints for the
// frontend and curl. Every handler delegates to the gRPC method so the two
// surfaces cannot drift; responses are protojson.

var jsonMarshaler = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}

func registerAdminRoutes(group *echo.Group, s *Service) {
	g := group.Group("", func(next echo.HandlerFunc) echo.HandlerFunc {
		return auth.WithClusterAdminAuth(next)
	})

	g.GET("", s.httpListEndpoints)
	g.GET("/", s.httpListEndpoints)
	g.GET("/services", s.httpListServices)
	g.GET("/gitops", s.httpGitOpsStatus)
	g.POST("/gitops/sync", s.httpGitOpsSync)
	g.POST("/validate", s.httpValidate)

	g.GET("/experiments/:experiment", s.httpGetExperiment)
	g.GET("/experiments/:experiment/metrics", s.httpExperimentMetrics)
	g.POST("/experiments/:experiment/config", s.httpApplyExperimentConfig)
	g.POST("/experiments/:experiment/bench", s.httpRecordBench)
	g.POST("/experiments/:experiment/revert", s.httpRevertExperiment)
	g.POST("/experiments/:experiment/stop", s.httpStopExperiment)

	g.GET("/:id", s.httpGetEndpoint)
	g.GET("/:id/replicas", s.httpListReplicas)
	g.GET("/:id/metrics", s.httpGetMetrics)
	g.GET("/:id/config", s.httpGetConfig)
	g.GET("/:id/config/revisions", s.httpListConfigRevisions)
	g.GET("/:id/rollout", s.httpGetRollout)
	g.POST("/:id/rollout/promote", s.httpPromote)
	g.POST("/:id/rollout/rollback", s.httpRollback)
	g.POST("/:id/rollout/pin", s.httpPin)
	g.POST("/:id/enabled", s.httpSetEnabled)
	g.GET("/:id/experiments", s.httpListExperiments)
	g.POST("/:id/experiments", s.httpStartExperiment)
}

func httpCtx(ctx echo.Context) context.Context {
	if cc, ok := ctx.(*auth.HttpAuthContext); ok && cc.AuthInfo != nil {
		return auth.ContextWithAuthInfo(ctx.Request().Context(), cc.AuthInfo)
	}
	return ctx.Request().Context()
}

func pathID(ctx echo.Context, name string) string {
	raw := ctx.Param(name)
	if decoded, err := url.PathUnescape(raw); err == nil {
		return decoded
	}
	return raw
}

func queryUint(ctx echo.Context, name string) uint32 {
	v, _ := strconv.ParseUint(ctx.QueryParam(name), 10, 32)
	return uint32(v)
}

func queryBool(ctx echo.Context, name string) bool {
	v, _ := strconv.ParseBool(ctx.QueryParam(name))
	return v
}

// respond writes a proto response. ok=false bodies map to 4xx/5xx by message.
func respond(ctx echo.Context, msg proto.Message, err error) error {
	if err != nil {
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
	body, mErr := jsonMarshaler.Marshal(msg)
	if mErr != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, mErr.Error())
	}
	code := http.StatusOK
	if !responseOK(msg) {
		code = http.StatusBadRequest
		if strings.Contains(strings.ToLower(responseErr(msg)), "not found") {
			code = http.StatusNotFound
		}
	}
	return ctx.JSONBlob(code, body)
}

func responseOK(msg proto.Message) bool {
	if msg == nil {
		return false
	}
	fd := msg.ProtoReflect().Descriptor().Fields().ByName("ok")
	if fd == nil {
		return true
	}
	return msg.ProtoReflect().Get(fd).Bool()
}

func responseErr(msg proto.Message) string {
	fd := msg.ProtoReflect().Descriptor().Fields().ByName("err_msg")
	if fd == nil {
		return ""
	}
	return msg.ProtoReflect().Get(fd).String()
}

func bindJSON(ctx echo.Context, msg proto.Message) error {
	body, err := io.ReadAll(io.LimitReader(ctx.Request().Body, 4<<20))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, msg); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: "+err.Error())
	}
	return nil
}

func (s *Service) httpListEndpoints(ctx echo.Context) error {
	out, err := s.ListEndpoints(httpCtx(ctx), &pb.ListEndpointsRequest{IncludeDisabled: queryBool(ctx, "include_disabled")})
	return respond(ctx, out, err)
}

func (s *Service) httpGetEndpoint(ctx echo.Context) error {
	out, err := s.GetEndpoint(httpCtx(ctx), &pb.GetEndpointRequest{EndpointId: pathID(ctx, "id")})
	return respond(ctx, out, err)
}

func (s *Service) httpListReplicas(ctx echo.Context) error {
	out, err := s.ListReplicas(httpCtx(ctx), &pb.ListReplicasRequest{
		EndpointId: pathID(ctx, "id"),
		Status:     ctx.QueryParam("status"),
		Gpu:        ctx.QueryParam("gpu"),
		Role:       ctx.QueryParam("role"),
	})
	return respond(ctx, out, err)
}

func (s *Service) httpGetMetrics(ctx echo.Context) error {
	out, err := s.GetMetrics(httpCtx(ctx), &pb.GetMetricsRequest{
		EndpointId:    pathID(ctx, "id"),
		Gpu:           ctx.QueryParam("gpu"),
		ReplicaId:     ctx.QueryParam("replica_id"),
		WindowSeconds: queryUint(ctx, "window_seconds"),
	})
	return respond(ctx, out, err)
}

func (s *Service) httpGetConfig(ctx echo.Context) error {
	out, err := s.GetConfig(httpCtx(ctx), &pb.GetConfigRequest{
		EndpointId: pathID(ctx, "id"),
		Scope:      ctx.QueryParam("scope"),
		ScopeKey:   ctx.QueryParam("scope_key"),
	})
	return respond(ctx, out, err)
}

func (s *Service) httpListConfigRevisions(ctx echo.Context) error {
	out, err := s.ListConfigRevisions(httpCtx(ctx), &pb.ListConfigRevisionsRequest{
		EndpointId: pathID(ctx, "id"),
		Scope:      ctx.QueryParam("scope"),
		ScopeKey:   ctx.QueryParam("scope_key"),
		Limit:      queryUint(ctx, "limit"),
	})
	return respond(ctx, out, err)
}

func (s *Service) httpGetRollout(ctx echo.Context) error {
	out, err := s.GetRollout(httpCtx(ctx), &pb.GetRolloutRequest{EndpointId: pathID(ctx, "id")})
	return respond(ctx, out, err)
}

func (s *Service) httpPromote(ctx echo.Context) error {
	in := &pb.PromoteRolloutRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.EndpointId = pathID(ctx, "id")
	out, err := s.PromoteRollout(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpRollback(ctx echo.Context) error {
	in := &pb.RollbackRolloutRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.EndpointId = pathID(ctx, "id")
	out, err := s.RollbackRollout(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpPin(ctx echo.Context) error {
	in := &pb.PinVersionRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.EndpointId = pathID(ctx, "id")
	out, err := s.PinVersion(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpSetEnabled(ctx echo.Context) error {
	in := &pb.SetEndpointEnabledRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.EndpointId = pathID(ctx, "id")
	out, err := s.SetEndpointEnabled(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpListServices(ctx echo.Context) error {
	out, err := s.ListServices(httpCtx(ctx), &pb.ListServicesRequest{})
	return respond(ctx, out, err)
}

func (s *Service) httpGitOpsStatus(ctx echo.Context) error {
	out, err := s.GetGitOpsStatus(httpCtx(ctx), &pb.GetGitOpsStatusRequest{})
	return respond(ctx, out, err)
}

func (s *Service) httpGitOpsSync(ctx echo.Context) error {
	in := &pb.TriggerGitOpsSyncRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	out, err := s.TriggerGitOpsSync(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpValidate(ctx echo.Context) error {
	in := &pb.ValidateEndpointSpecRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	out, err := s.ValidateEndpointSpec(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpStartExperiment(ctx echo.Context) error {
	in := &pb.StartExperimentRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.EndpointId = pathID(ctx, "id")
	out, err := s.StartExperiment(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpListExperiments(ctx echo.Context) error {
	out, err := s.ListExperiments(httpCtx(ctx), &pb.ListExperimentsRequest{
		EndpointId: pathID(ctx, "id"),
		Gpu:        ctx.QueryParam("gpu"),
		Limit:      queryUint(ctx, "limit"),
	})
	return respond(ctx, out, err)
}

func (s *Service) httpGetExperiment(ctx echo.Context) error {
	out, err := s.GetExperiment(httpCtx(ctx), &pb.GetExperimentRequest{ExperimentId: pathID(ctx, "experiment")})
	return respond(ctx, out, err)
}

func (s *Service) httpExperimentMetrics(ctx echo.Context) error {
	out, err := s.GetExperimentMetrics(httpCtx(ctx), &pb.GetExperimentMetricsRequest{
		ExperimentId:  pathID(ctx, "experiment"),
		WindowSeconds: queryUint(ctx, "window_seconds"),
	})
	return respond(ctx, out, err)
}

func (s *Service) httpApplyExperimentConfig(ctx echo.Context) error {
	in := &pb.ApplyExperimentConfigRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.ExperimentId = pathID(ctx, "experiment")
	out, err := s.ApplyExperimentConfig(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpRecordBench(ctx echo.Context) error {
	in := &pb.RecordExperimentBenchRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.ExperimentId = pathID(ctx, "experiment")
	out, err := s.RecordExperimentBench(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpRevertExperiment(ctx echo.Context) error {
	in := &pb.RevertExperimentRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.ExperimentId = pathID(ctx, "experiment")
	out, err := s.RevertExperiment(httpCtx(ctx), in)
	return respond(ctx, out, err)
}

func (s *Service) httpStopExperiment(ctx echo.Context) error {
	in := &pb.StopExperimentRequest{}
	if err := bindJSON(ctx, in); err != nil {
		return err
	}
	in.ExperimentId = pathID(ctx, "experiment")
	out, err := s.StopExperiment(httpCtx(ctx), in)
	return respond(ctx, out, err)
}
