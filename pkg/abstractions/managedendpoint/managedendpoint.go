// Package managedendpoint implements the managed endpoints platform: the
// registry of GitOps-deployed inference endpoints, the controller that fills
// them onto spare GPU capacity, the harness and admin gRPC services, and the
// OpenRouter-compatible /v1 route.
package managedendpoint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// containerPrefix must not collide with other abstractions' prefixes (the
	// serverless endpoint service claims "endpoint-*" containers by prefix).
	containerPrefix = "managed"

	// Environment injected into every replica container.
	EnvEndpointID     = "BEAM_ENDPOINT_ID"
	EnvReplicaID      = "BEAM_REPLICA_ID"
	EnvReplicaSecret  = "BEAM_REPLICA_SECRET" // presented on harness RPCs as x-beam-replica-secret
	EnvGpu            = "BEAM_GPU"
	EnvLocality       = "BEAM_LOCALITY"
	EnvEndpointPort   = "BEAM_ENDPOINT_PORT"
	EnvHarnessEnabled = "BEAM_HARNESS_ENABLED"
	EnvHarnessConfig  = "BEAM_HARNESS_CONFIG"
	// EnvDrainSeconds is how long the engine has after SIGTERM (eviction or
	// scale-down) before the worker kills it. Engines without the harness
	// still get a correct drain window from this alone.
	EnvDrainSeconds = "BEAM_DRAIN_SECONDS"
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

	transports sync.Map // replica address -> *http.Transport; dropped when the replica finishes

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

// Replicas hold no workspace token. The only credential a replica container
// receives is its own secret, minted here and delivered as BEAM_REPLICA_SECRET;
// harness RPCs are exempt from token auth and are authorized by that secret
// alone, so a compromised model host learns nothing beyond its own replica.
const replicaSecretHeader = "x-beam-replica-secret"

func newReplicaSecret() (secret, hash string) {
	buf := make([]byte, 32)
	_, _ = rand.Read(buf)
	secret = hex.EncodeToString(buf)
	return secret, hashReplicaSecret(secret)
}

func hashReplicaSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// transport returns the pooled transport for one replica address. Every hop
// to a replica (proxying, health, metrics) dials through it, so provider
// route:// addresses and tailscale backends behave the same everywhere.
func (s *Service) transport(address string) *http.Transport {
	if t, ok := s.transports.Load(address); ok {
		return t.(*http.Transport)
	}
	transport := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := network.ConnectToBackend(ctx, address, replicaDialTimeout, s.tailscale, s.appConfig.Tailscale, s.containers)
			if err != nil {
				return nil, err
			}
			// -1: no read deadline; streams are bounded by the request context.
			abstractions.SetConnOptions(conn, true, 30*time.Second, -1)
			return conn, nil
		},
	}
	actual, loaded := s.transports.LoadOrStore(address, transport)
	if loaded {
		transport.CloseIdleConnections()
	}
	return actual.(*http.Transport)
}

// probeClient is the short-timeout client the controller probes a replica with.
func (s *Service) probeClient(address string) *http.Client {
	return &http.Client{Transport: s.transport(address), Timeout: probeTimeout}
}

func (s *Service) forgetTransport(address string) {
	if t, ok := s.transports.LoadAndDelete(address); ok {
		t.(*http.Transport).CloseIdleConnections()
	}
}

// harnessReplica authorizes a harness call for one replica by its secret.
func (s *Service) harnessReplica(ctx context.Context, replica *types.EndpointReplica) error {
	if !s.Enabled() {
		return status.Error(codes.FailedPrecondition, errNotEnabled.Error())
	}
	if replica == nil {
		return status.Error(codes.NotFound, "replica not found")
	}
	presented := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(replicaSecretHeader); len(values) > 0 {
			presented = strings.TrimSpace(values[0])
		}
	}
	if replica.SecretHash == "" || presented == "" ||
		subtle.ConstantTimeCompare([]byte(hashReplicaSecret(presented)), []byte(replica.SecretHash)) != 1 {
		return status.Error(codes.PermissionDenied, "replica secret required")
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
		GPU: replica.GPU, Version: replica.Version, WorkerID: replica.WorkerID,
		PoolName: replica.PoolName, Locality: replica.Locality, Message: message, Data: data,
	})
}

// normalizeGPUKey canonicalizes an admin-supplied GPU key ("h100" -> "H100",
// "" -> "" so filters stay optional).
func normalizeGPUKey(gpu string) string {
	if strings.TrimSpace(gpu) == "" {
		return ""
	}
	return types.GPUKey(gpu)
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
	return &pb.ReplicaCapacity{
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
}

func capacityFromProto(c *pb.ReplicaCapacity) types.ReplicaCapacity {
	if c == nil {
		return types.ReplicaCapacity{}
	}
	return types.ReplicaCapacity{
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
}

func configToProto(c types.ReplicaConfig) *pb.ReplicaConfig {
	if c.Revision == 0 {
		return nil
	}
	return &pb.ReplicaConfig{
		Revision:      c.Revision,
		ConfigJson:    string(c.Config),
		Author:        c.Author,
		SetAtUnixMs:   unixMs(c.SetAt),
		AckedRevision: c.AckedRevision,
		Applied:       c.Applied,
		Error:         c.Error,
		EffectiveJson: string(c.Effective),
		AckedAtUnixMs: unixMs(c.AckedAt),
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
		Gpu:                 r.GPU,
		GpuCount:            r.GPUCount,
		Locality:            r.Locality,
		PoolName:            r.PoolName,
		ContainerId:         r.ContainerID,
		WorkerId:            r.WorkerID,
		MachineId:           r.MachineID,
		ProviderWorkspaceId: r.ProviderWorkspaceID,
		Address:             r.Address,
		Status:              string(r.Status),
		StatusReason:        r.StatusReason,
		HarnessEnabled:      r.HarnessEnabled,
		Config:              configToProto(r.Config),
		Capacity:            capacityToProto(r.Capacity),
		CapabilitiesJson:    string(r.Capabilities),
		EngineMetricsJson:   string(r.EngineMetrics),
		StartedAtUnixMs:     unixMs(r.StartedAt),
		ReadyAtUnixMs:       unixMs(r.ReadyAt),
		LastHeartbeatUnixMs: unixMs(r.LastHeartbeat),
	}
}

func replicasToProto(replicas []*types.EndpointReplica) []*pb.EndpointReplica {
	out := make([]*pb.EndpointReplica, 0, len(replicas))
	for _, r := range replicas {
		out = append(out, replicaToProto(r))
	}
	return out
}

// endpointToProto builds the listing entry with live replica counts and the
// fleet's replica targets for the endpoint.
func endpointToProto(e *types.ManagedEndpoint, fleet *types.Fleet, replicas []*types.EndpointReplica) *pb.ManagedEndpoint {
	out := &pb.ManagedEndpoint{
		Id:              e.Spec.ID,
		SpecJson:        mustJSON(e.Spec),
		StubId:          e.StubID,
		Version:         uint32(e.Version),
		GitSha:          e.GitSHA,
		Status:          string(e.Status),
		CreatedAtUnixMs: unixMs(e.CreatedAt),
		UpdatedAtUnixMs: unixMs(e.UpdatedAt),
	}
	if fleet != nil {
		replicas := fleet.Replicas[e.Spec.ID]
		if replicas == nil {
			replicas = map[string]uint32{}
		}
		out.ReplicasJson = mustJSON(replicas)
	}
	for _, r := range replicas {
		if r.EndpointID == e.Spec.ID && !r.Status.Terminal() {
			out.TotalReplicas++
			if r.Status == types.ReplicaStatusReady {
				out.ReadyReplicas++
			}
		}
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
		FleetError:      state.FleetError,
		Running:         state.Running,
	}
	for _, e := range state.PerEndpoint {
		out.Endpoints = append(out.Endpoints, &pb.GitOpsEndpointState{
			Path:            e.Path,
			Id:              e.ID,
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
