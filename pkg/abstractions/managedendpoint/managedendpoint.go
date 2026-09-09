// Package managedendpoint implements the managed endpoints platform: the
// registry of GitOps-deployed inference endpoints, the controller that fills
// them onto spare GPU capacity, the harness and admin gRPC services, and the
// OpenRouter-compatible /v1 route.
package managedendpoint

import (
	"context"
	"database/sql"
	"encoding/json"
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
	EnvKVCache        = "BEAM_KV_CACHE"
	// EnvDrainSeconds is how long the engine has after SIGTERM (eviction or
	// scale-down) before the worker kills it. Engines without the harness
	// still get a correct drain window from this alone.
	EnvDrainSeconds  = "BEAM_DRAIN_SECONDS"
	EnvServicePrefix = "BEAM_SERVICE_"
)

var (
	errNotEnabled = errors.New("managed endpoints are not enabled")
	errNotFound   = errors.New("not found")
)

// Opts wires the service into the gateway.
type Opts struct {
	Config           types.AppConfig
	BackendRepo      repository.BackendRepository
	ContainerRepo    repository.ContainerRepository
	WorkerRepo       repository.WorkerRepository
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
	repo       repository.ManagedEndpointRepository
	events     repository.EventRepository
	usage      repository.UsageMetricsRepository
	scheduler  *scheduler.Scheduler
	rdb        *common.RedisClient
	tailscale  *network.Tailscale
	drainCtx   context.Context

	controller *controller
	router     *router
	gitops     *gitops // nil when no repo is configured

	adminMu        sync.Mutex
	adminWorkspace *types.Workspace
	runtimeToken   string

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
		repo:       opts.EndpointRepo,
		events:     opts.EventRepo,
		usage:      opts.UsageMetricsRepo,
		scheduler:  opts.Scheduler,
		rdb:        opts.RedisClient,
		tailscale:  opts.Tailscale,
		drainCtx:   opts.DrainContext,
	}
	s.config.ApplyDefaults()
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
	s.gitops = newGitOps(s)

	authMiddleware := auth.AuthMiddleware(opts.BackendRepo, opts.WorkspaceRepo)
	if opts.RouteGroup != nil {
		s.router.mount(opts.RouteGroup, authMiddleware)
	}
	if opts.AdminRouteGroup != nil {
		authed := opts.AdminRouteGroup.Group("", authMiddleware)
		s.mountAdminRoutes(authed)
		if s.gitops != nil {
			s.gitops.mount(opts.AdminRouteGroup, authed)
		}
	}

	go s.controller.run(ctx)
	if s.gitops != nil {
		go s.gitops.run(ctx)
	}
	return s, nil
}

// Enabled reports whether the platform is active on this cluster.
func (s *Service) Enabled() bool {
	return s != nil && s.config.Enabled && s.repo != nil
}

// AdminWorkspace returns the cluster admin workspace, which owns every
// managed stub and replica. The signing key is filled in because container
// mounts and secret decryption need it.
func (s *Service) AdminWorkspace(ctx context.Context) (*types.Workspace, error) {
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
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
	copied := *workspace // the backend's cached pointer is shared
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
	workspace, err := s.AdminWorkspace(ctx)
	if err != nil {
		return "", err
	}
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	if s.runtimeToken != "" {
		return s.runtimeToken, nil
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
	if !s.Enabled() {
		return status.Error(codes.FailedPrecondition, errNotEnabled.Error())
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
	if !s.Enabled() {
		return status.Error(codes.FailedPrecondition, errNotEnabled.Error())
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

// replicaEvent emits a replica lifecycle event carrying the replica's identity.
func (s *Service) replicaEvent(replica *types.EndpointReplica, action, message string, data map[string]any) {
	s.emit(types.EventEndpointReplica, types.EventEndpointSchema{
		EndpointID: replica.EndpointID, Action: action, ReplicaID: replica.ID, ContainerID: replica.ContainerID,
		GPU: replica.GPU, Role: replica.Role, Version: replica.Version, WorkerID: replica.WorkerID,
		PoolName: replica.PoolName, Locality: replica.Locality, Message: message, Data: data,
	})
}

// effectiveRevision resolves the config a replica should run: a replica-scoped
// revision (tuning) wins over the fleet revision for its role/target.
func (s *Service) effectiveRevision(ctx context.Context, replica *types.EndpointReplica) (*types.EndpointConfigRevision, error) {
	if replica.Tuning {
		revision, err := s.repo.LatestConfigRevision(ctx, replica.EndpointID, types.ConfigScopeReplica, replica.ID)
		if err != nil || revision != nil {
			return revision, err
		}
	}
	return s.repo.LatestConfigRevision(ctx, replica.EndpointID, types.ConfigScopeTarget, fleetKey(replica.Role, replica.GPU, replica.Version))
}

// targetKey identifies a (role, gpu target) within an endpoint: "serve:H100x1".
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

// normalizeGPUKey canonicalizes an admin-supplied target key ("h100" ->
// "H100x1", "a100x2" -> "A100x2", "cpu" -> "cpu").
func normalizeGPUKey(gpu string) string {
	gpu = strings.TrimSpace(gpu)
	if gpu == "" || gpu == "cpu" {
		return gpu
	}
	if idx := strings.LastIndex(gpu, "x"); idx > 0 {
		return string(types.NormalizeGPUType(gpu[:idx])) + gpu[idx:]
	}
	return string(types.NormalizeGPUType(gpu)) + "x1"
}

// rpcError maps registry errors onto gRPC statuses.
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, errNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
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

// --- proto conversion --------------------------------------------------------

func unixMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func mustJSON(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(data)
}

func capacityToProto(c types.ReplicaCapacity) *pb.ReplicaCapacity {
	out := &pb.ReplicaCapacity{
		InFlight:            c.InFlight,
		MaxConcurrency:      c.MaxConcurrency,
		Running:             c.Running,
		Waiting:             c.Waiting,
		KvCacheFreeMilli:    c.KVCacheFreeMilli,
		DecodeTokensPerSec:  c.DecodeTokensPerSec,
		PromptTokensPerSec:  c.PromptTokensPerSec,
		TtftMs:              c.TTFTMs,
		TpotMs:              c.TPOTMs,
		PrefixCacheHitMilli: c.PrefixCacheHitMilli,
	}
	if c.KVTransfer != nil {
		out.KvTransferJson = mustJSON(c.KVTransfer)
	}
	return out
}

func capacityFromProto(c *pb.ReplicaCapacity) types.ReplicaCapacity {
	if c == nil {
		return types.ReplicaCapacity{}
	}
	out := types.ReplicaCapacity{
		InFlight:            c.InFlight,
		MaxConcurrency:      c.MaxConcurrency,
		Running:             c.Running,
		Waiting:             c.Waiting,
		KVCacheFreeMilli:    c.KvCacheFreeMilli,
		DecodeTokensPerSec:  c.DecodeTokensPerSec,
		PromptTokensPerSec:  c.PromptTokensPerSec,
		TTFTMs:              c.TtftMs,
		TPOTMs:              c.TpotMs,
		PrefixCacheHitMilli: c.PrefixCacheHitMilli,
	}
	if c.KvTransferJson != "" {
		var kv types.KVTransferStats
		if err := json.Unmarshal([]byte(c.KvTransferJson), &kv); err == nil {
			out.KVTransfer = &kv
		}
	}
	return out
}

func revisionToProto(r *types.EndpointConfigRevision) *pb.ConfigRevision {
	if r == nil {
		return nil
	}
	return &pb.ConfigRevision{
		Revision:        r.Revision,
		EndpointId:      r.EndpointID,
		Scope:           string(r.Scope),
		ScopeKey:        r.ScopeKey,
		ConfigJson:      mustJSON(r.Config),
		Author:          r.Author,
		Source:          string(r.Source),
		CreatedAtUnixMs: unixMs(r.CreatedAt),
	}
}

func replicaToProto(r *types.EndpointReplica) *pb.EndpointReplica {
	if r == nil {
		return nil
	}
	return &pb.EndpointReplica{
		Id:                  r.ID,
		EndpointId:          r.EndpointID,
		Version:             uint32(r.Version),
		Role:                r.Role,
		Gpu:                 r.GPU,
		GpuCount:            r.GPUCount,
		Locality:            r.Locality,
		PoolName:            r.PoolName,
		ContainerId:         r.ContainerID,
		WorkerId:            r.WorkerID,
		Address:             r.Address,
		Status:              string(r.Status),
		Protected:           r.Protected,
		Tuning:              r.Tuning,
		HarnessEnabled:      r.HarnessEnabled,
		ConfigRevision:      r.ConfigRevision,
		Capacity:            capacityToProto(r.Capacity),
		CapabilitiesJson:    string(r.Capabilities),
		StartedAtUnixMs:     unixMs(r.StartedAt),
		ReadyAtUnixMs:       unixMs(r.ReadyAt),
		LastHeartbeatUnixMs: unixMs(r.LastHeartbeat),
		StatusReason:        r.StatusReason,
	}
}

func replicasToProto(replicas []*types.EndpointReplica) []*pb.EndpointReplica {
	out := make([]*pb.EndpointReplica, 0, len(replicas))
	for _, r := range replicas {
		out = append(out, replicaToProto(r))
	}
	return out
}

// endpointToProto builds the listing entry, counting live replicas.
func endpointToProto(e *types.ManagedEndpoint, replicas []*types.EndpointReplica) *pb.ManagedEndpoint {
	out := &pb.ManagedEndpoint{
		Id:              e.Spec.ID,
		SpecJson:        mustJSON(e.Spec),
		StubId:          e.StubID,
		Version:         uint32(e.Version),
		GitSha:          e.GitSHA,
		Enabled:         e.Enabled,
		Status:          string(e.Status),
		CreatedAtUnixMs: unixMs(e.CreatedAt),
		UpdatedAtUnixMs: unixMs(e.UpdatedAt),
	}
	out.ReadyReplicas, out.TotalReplicas = countReplicas(replicas, e.Spec.ID)
	return out
}

// countReplicas returns the ready and non-terminal replica counts for an id.
func countReplicas(replicas []*types.EndpointReplica, id string) (ready, total uint32) {
	for _, r := range replicas {
		if r.EndpointID == id && !r.Status.Terminal() {
			total++
			if r.Status == types.ReplicaStatusReady {
				ready++
			}
		}
	}
	return ready, total
}

func serviceToProto(s *types.ManagedService, replicas []*types.EndpointReplica) *pb.ManagedService {
	out := &pb.ManagedService{
		Name:     s.Spec.Name,
		SpecJson: mustJSON(s.Spec),
		StubId:   s.StubID,
		Version:  uint32(s.Version),
		GitSha:   s.GitSHA,
		Enabled:  s.Enabled,
		Status:   string(s.Status),
	}
	out.ReadyReplicas, out.TotalReplicas = countReplicas(replicas, serviceReplicaID(s.Spec.Name))
	return out
}

func rolloutToProto(r *types.RolloutState, versions []*types.EndpointVersion) *pb.RolloutState {
	if r == nil {
		return nil
	}
	out := &pb.RolloutState{
		EndpointId:           r.EndpointID,
		ActiveVersion:        uint32(r.ActiveVersion),
		CanaryVersion:        uint32(r.CanaryVersion),
		PinnedVersion:        uint32(r.PinnedVersion),
		Phase:                string(r.Phase),
		BakeStartedAtUnixMs:  unixMs(r.BakeStartedAt),
		LastDecision:         r.LastDecision,
		LastDecisionAtUnixMs: unixMs(r.LastDecisionAt),
	}
	for _, v := range versions {
		out.Versions = append(out.Versions, &pb.EndpointVersion{
			EndpointId:      v.EndpointID,
			Version:         uint32(v.Version),
			StubId:          v.StubID,
			GitSha:          v.GitSHA,
			State:           string(v.State),
			CreatedAtUnixMs: unixMs(v.CreatedAt),
		})
	}
	return out
}

func gitopsToProto(state *types.GitOpsState) *pb.GitOpsState {
	out := &pb.GitOpsState{
		RepoUrl:         state.RepoURL,
		Ref:             state.Ref,
		LastSha:         state.LastSHA,
		TargetSha:       state.TargetSHA,
		LastRunAtUnixMs: unixMs(state.LastRunAt),
		LastError:       state.LastError,
		Running:         state.Running,
	}
	for _, e := range state.PerEndpoint {
		out.Endpoints = append(out.Endpoints, &pb.GitOpsEndpointState{
			Path:            e.Path,
			Id:              e.ID,
			Kind:            e.Kind,
			AppliedSha:      e.AppliedSHA,
			Status:          string(e.Status),
			Error:           e.Error,
			StubId:          e.StubID,
			Version:         uint32(e.Version),
			UpdatedAtUnixMs: unixMs(e.UpdatedAt),
		})
	}
	return out
}

// routeMetricsToProto combines a route window with the ready replicas'
// engine-reported capacity.
func routeMetricsToProto(m *types.RouteMetrics, replicas []*types.EndpointReplica) *pb.EndpointMetrics {
	if m == nil {
		return nil
	}
	out := &pb.EndpointMetrics{
		EndpointId:       m.EndpointID,
		Gpu:              m.GPU,
		WindowSeconds:    uint32(m.Window.Seconds()),
		Requests:         m.Requests,
		Errors:           m.Errors,
		PromptTokens:     m.PromptTokens,
		CompletionTokens: m.CompletionTokens,
		TtftMs:           m.MeanTTFTMs(),
		TpotMs:           m.MeanTPOTMs(),
		CostMicroUsd:     m.CostMicroUSD,
	}
	if m.Requests > 0 {
		out.QueueWaitMs = m.QueueWaitSumMs / m.Requests
	}
	var aggregate types.ReplicaCapacity
	for _, replica := range replicas {
		if replica.Status != types.ReplicaStatusReady {
			continue
		}
		out.ReadyReplicas++
		aggregate.InFlight += replica.Capacity.InFlight
		aggregate.MaxConcurrency += replica.Capacity.MaxConcurrency
		aggregate.Running += replica.Capacity.Running
		aggregate.Waiting += replica.Capacity.Waiting
		aggregate.DecodeTokensPerSec += replica.Capacity.DecodeTokensPerSec
		aggregate.PromptTokensPerSec += replica.Capacity.PromptTokensPerSec
	}
	out.DecodeTokensPerSec = aggregate.DecodeTokensPerSec
	out.AggregateCapacity = capacityToProto(aggregate)
	return out
}
