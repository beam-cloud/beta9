// Package managedendpoint implements the managed endpoints platform: the
// registry of GitOps-deployed inference endpoints, the controller that fills
// them onto spare GPU capacity, the harness and admin gRPC services, and the
// OpenRouter-compatible /v1 route.
package managedendpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// containerPrefix must not collide with other abstractions' prefixes (the
	// serverless endpoint service claims "endpoint-*" containers by prefix).
	containerPrefix = "managed"

	// Environment injected into every replica container.
	EnvEndpointID     = "BEAM_ENDPOINT_ID"
	EnvReplicaID      = "BEAM_REPLICA_ID"
	EnvReplicaRole    = "BEAM_ENDPOINT_ROLE"
	EnvGpuTarget      = "BEAM_GPU_TARGET"
	EnvLocality       = "BEAM_LOCALITY"
	EnvEndpointPort   = "BEAM_ENDPOINT_PORT"
	EnvHarnessEnabled = "BEAM_HARNESS_ENABLED"
	EnvHarnessConfig  = "BEAM_HARNESS_CONFIG"
	EnvServicePrefix  = "BEAM_SERVICE_"
)

var (
	errNotEnabled  = errors.New("managed endpoints are not enabled")
	errNotFound    = errors.New("not found")
	errUnavailable = errors.New("registry unavailable")
)

// Opts wires the service into the gateway.
type Opts struct {
	Config           types.AppConfig
	BackendRepo      repository.BackendRepository
	ContainerRepo    repository.ContainerRepository
	WorkerRepo       repository.WorkerRepository
	WorkerPoolRepo   repository.WorkerPoolRepository
	WorkspaceRepo    repository.WorkspaceRepository
	EndpointRepo     repository.ManagedEndpointRepository
	EventRepo        repository.EventRepository
	UsageMetricsRepo repository.UsageMetricsRepository
	Scheduler        *scheduler.Scheduler
	RedisClient      *common.RedisClient
	Tailscale        *network.Tailscale
	// RouteGroup is the root echo group (/v1 is mounted under it).
	RouteGroup *echo.Group
	// AdminRouteGroup receives the /api/v1/endpoints REST mirror.
	AdminRouteGroup *echo.Group
	// DrainContext ends in-flight /v1 requests on gateway shutdown.
	DrainContext context.Context
}

// Service is the managed endpoints control plane inside one gateway replica.
type Service struct {
	ctx        context.Context
	config     types.ManagedEndpointsConfig
	appConfig  types.AppConfig
	backend    repository.BackendRepository
	containers repository.ContainerRepository
	workers    repository.WorkerRepository
	pools      repository.WorkerPoolRepository
	repo       repository.ManagedEndpointRepository
	events     repository.EventRepository
	usage      repository.UsageMetricsRepository
	scheduler  *scheduler.Scheduler
	rdb        *common.RedisClient
	tailscale  *network.Tailscale
	drainCtx   context.Context

	controller *controller
	router     *router
	gitops     gitopsTrigger

	adminWorkspaceMu sync.Mutex
	adminWorkspace   *types.Workspace
	runtimeTokenMu   sync.Mutex
	runtimeToken     string

	pb.UnimplementedEndpointHarnessServiceServer
	pb.UnimplementedEndpointAdminServiceServer
}

// New constructs the service. When managed endpoints are disabled the
// returned service registers no routes and every RPC fails with
// FailedPrecondition, so callers can register it unconditionally.
func New(ctx context.Context, opts Opts) (*Service, error) {
	s := &Service{
		ctx:        ctx,
		config:     opts.Config.ManagedEndpoints,
		appConfig:  opts.Config,
		backend:    opts.BackendRepo,
		containers: opts.ContainerRepo,
		workers:    opts.WorkerRepo,
		pools:      opts.WorkerPoolRepo,
		repo:       opts.EndpointRepo,
		events:     opts.EventRepo,
		usage:      opts.UsageMetricsRepo,
		scheduler:  opts.Scheduler,
		rdb:        opts.RedisClient,
		tailscale:  opts.Tailscale,
		drainCtx:   opts.DrainContext,
	}
	if s.drainCtx == nil {
		s.drainCtx = ctx
	}
	if !s.config.Enabled {
		return s, nil
	}
	if s.repo == nil {
		if s.rdb == nil {
			return nil, errors.New("managed endpoints require redis")
		}
		s.repo = repository.NewManagedEndpointRedisRepository(s.rdb)
	}

	s.controller = newController(s)
	s.router = newRouter(s)
	reconciler := newGitOps(s)
	if reconciler != nil {
		s.gitops = reconciler
	}
	authMiddleware := auth.AuthMiddleware(opts.BackendRepo, opts.WorkspaceRepo)
	if opts.RouteGroup != nil {
		s.router.mount(opts.RouteGroup, authMiddleware)
	}
	if opts.AdminRouteGroup != nil {
		authed := opts.AdminRouteGroup.Group("", authMiddleware)
		registerAdminRoutes(authed, s)
		if reconciler != nil {
			reconciler.mount(opts.AdminRouteGroup, authed)
		}
	}

	go s.controller.run(ctx)
	if reconciler != nil {
		go reconciler.run(ctx)
	}
	return s, nil
}

// Enabled reports whether the platform is active on this cluster.
func (s *Service) Enabled() bool {
	return s != nil && s.config.Enabled && s.repo != nil
}

func (s *Service) requireEnabled() error {
	if !s.Enabled() {
		return status.Error(codes.FailedPrecondition, errNotEnabled.Error())
	}
	return nil
}

// AdminWorkspace returns the cluster admin workspace, which owns every
// managed stub and replica.
func (s *Service) AdminWorkspace(ctx context.Context) (*types.Workspace, error) {
	s.adminWorkspaceMu.Lock()
	defer s.adminWorkspaceMu.Unlock()
	if s.adminWorkspace != nil {
		return s.adminWorkspace, nil
	}
	workspace, err := s.backend.GetAdminWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	if workspace == nil {
		return nil, errors.New("cluster admin workspace not found")
	}
	// The cached admin record omits the signing key, which container mounts
	// and secret decryption need. Copy the record before filling it in; the
	// backend's cached pointer is shared.
	copied := *workspace
	if copied.SigningKey == nil || *copied.SigningKey == "" {
		withKey, err := s.backend.GetWorkspaceByExternalIdWithSigningKey(ctx, workspace.ExternalId)
		if err != nil {
			return nil, err
		}
		copied.SigningKey = withKey.SigningKey
	}
	s.adminWorkspace = &copied
	return &copied, nil
}

// runtimeTokenKey returns a reusable restricted token of the admin workspace
// that replica containers use to call EndpointHarnessService.
func (s *Service) runtimeTokenKey(ctx context.Context) (string, error) {
	s.runtimeTokenMu.Lock()
	defer s.runtimeTokenMu.Unlock()
	if s.runtimeToken != "" {
		return s.runtimeToken, nil
	}
	workspace, err := s.AdminWorkspace(ctx)
	if err != nil {
		return "", err
	}
	tokens, err := s.backend.ListTokens(ctx, workspace.Id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	for _, token := range tokens {
		if token.Active && !token.DisabledByClusterAdmin && token.TokenType == types.TokenTypeWorkspaceRestricted {
			s.runtimeToken = token.Key
			return token.Key, nil
		}
	}
	token, err := s.backend.CreateToken(ctx, workspace.Id, types.TokenTypeWorkspaceRestricted, true)
	if err != nil {
		return "", err
	}
	s.runtimeToken = token.Key
	return token.Key, nil
}

// authorizeAdmin accepts cluster admin tokens only.
func (s *Service) authorizeAdmin(ctx context.Context) error {
	if err := s.requireEnabled(); err != nil {
		return err
	}
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return status.Error(codes.PermissionDenied, "cluster admin token required")
	}
	return nil
}

// authorizeHarness accepts any active token of the admin workspace (replica
// containers carry a restricted one) or a cluster admin token.
func (s *Service) authorizeHarness(ctx context.Context) error {
	if err := s.requireEnabled(); err != nil {
		return err
	}
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || authInfo.Workspace == nil {
		return status.Error(codes.Unauthenticated, "token required")
	}
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		return nil
	}
	workspace, err := s.AdminWorkspace(ctx)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if workspace.Id != authInfo.Workspace.Id {
		return status.Error(codes.PermissionDenied, "harness calls must come from a managed endpoint replica")
	}
	return nil
}

func (s *Service) emit(eventType string, event types.EventEndpointSchema) {
	if s.events == nil {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	s.events.PushEndpointEvent(eventType, event)
}

func (s *Service) emitRoute(event types.EventEndpointRouteSchema) {
	if s.events == nil {
		return
	}
	s.events.PushEndpointRouteEvent(event)
}

// effectiveRevision resolves the config a replica should run: a replica-scoped
// revision (tuning) wins over the fleet revision for its role/target.
func (s *Service) effectiveRevision(ctx context.Context, replica *types.EndpointReplica) (*types.EndpointConfigRevision, error) {
	if replica == nil {
		return nil, nil
	}
	if replica.Tuning {
		revision, err := s.repo.LatestConfigRevision(ctx, replica.EndpointID, types.ConfigScopeReplica, replica.ID)
		if err != nil {
			return nil, err
		}
		if revision != nil {
			return revision, nil
		}
	}
	return s.repo.LatestConfigRevision(ctx, replica.EndpointID, types.ConfigScopeTarget, fleetKey(replica.Role, replica.GPU, replica.Version))
}

func targetKey(role, gpu string) string {
	if role == "" {
		role = types.ReplicaRoleServe
	}
	return role + ":" + gpu
}

// fleetKey is the target-scope key for fleet config. It carries the endpoint
// version so an active fleet and a baking canary each follow their own
// revision stream instead of fighting over one "latest".
func fleetKey(role, gpu string, version uint) string {
	return fmt.Sprintf("%s@v%d", targetKey(role, gpu), version)
}

// parseFleetKey splits "serve:cpu@v3" into its target key and version.
// Version is 0 when the key has no suffix.
func parseFleetKey(key string) (string, uint) {
	idx := strings.LastIndex(key, "@v")
	if idx < 0 {
		return key, 0
	}
	var version uint
	if _, err := fmt.Sscanf(key[idx+2:], "%d", &version); err != nil {
		return key, 0
	}
	return key[:idx], version
}

// endpointSummary builds the proto listing entry with replica counts.
func (s *Service) endpointSummary(ctx context.Context, endpoint *types.ManagedEndpoint) (*pb.ManagedEndpoint, error) {
	replicas, err := s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return nil, err
	}
	out := endpointToProto(endpoint)
	for _, replica := range replicas {
		if replica.Status.Terminal() {
			continue
		}
		out.TotalReplicas++
		if replica.Status == types.ReplicaStatusReady {
			out.ReadyReplicas++
		}
	}
	return out, nil
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, errNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, errUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func notFound(kind, id string) error {
	return fmt.Errorf("%s %q: %w", kind, id, errNotFound)
}

func normalizeGPUKey(gpu string) string {
	gpu = strings.TrimSpace(gpu)
	if gpu == "" {
		return ""
	}
	if gpu == "cpu" {
		return gpu
	}
	if idx := strings.LastIndex(gpu, "x"); idx > 0 {
		return string(types.NormalizeGPUType(gpu[:idx])) + gpu[idx:]
	}
	return string(types.NormalizeGPUType(gpu)) + "x1"
}

func replicaLog(replica *types.EndpointReplica) *zerolog.Logger {
	logger := log.With().
		Str("endpoint_id", replica.EndpointID).
		Str("replica_id", replica.ID).
		Str("container_id", replica.ContainerID).
		Str("gpu", replica.GPU).
		Str("role", replica.Role).
		Logger()
	return &logger
}
