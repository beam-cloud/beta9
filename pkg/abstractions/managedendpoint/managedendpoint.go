// Package managedendpoint hosts inference endpoints deployed from the
// endpoints repo on spare GPU capacity and serves them under an
// OpenAI-compatible /v1 route.
package managedendpoint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	containerPrefix = "managed" // "endpoint-*" is claimed by the serverless endpoint service

	// Environment injected into every replica container.
	EnvEndpointID     = "BEAM_ENDPOINT_ID"
	EnvReplicaID      = "BEAM_REPLICA_ID"
	EnvReplicaSecret  = "BEAM_REPLICA_SECRET" // presented on harness RPCs as x-beam-replica-secret
	EnvGpu            = "BEAM_GPU"
	EnvLocality       = "BEAM_LOCALITY"
	EnvEndpointPort   = "BEAM_ENDPOINT_PORT"
	EnvHarnessEnabled = "BEAM_HARNESS_ENABLED"
	EnvHarnessConfig  = "BEAM_HARNESS_CONFIG"
	EnvDrainSeconds   = "BEAM_DRAIN_SECONDS" // grace after SIGTERM before the worker kills the engine
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
	// DrainContext stops admission and queued /v1 requests during shutdown.
	// Active generations run until the service context ends after HTTP draining.
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
	billing    *billing

	adminMu        sync.Mutex
	adminWorkspace *types.Workspace

	transports sync.Map // replica address -> *http.Transport; dropped when the replica finishes

	pb.UnimplementedEndpointHarnessServiceServer
	pb.UnimplementedEndpointAdminServiceServer
}

// New constructs the service. When disabled it registers no routes and every
// RPC fails with FailedPrecondition.
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
	s.billing = newBilling(s)

	authMiddleware := auth.AuthMiddleware(opts.BackendRepo, opts.WorkspaceRepo)
	if opts.RouteGroup != nil {
		s.router.mount(opts.RouteGroup, authMiddleware)
	}
	if opts.AdminRouteGroup != nil {
		authed := opts.AdminRouteGroup.Group("", authMiddleware)
		s.mountAdminRoutes(authed)
	}

	go s.controller.run(ctx)
	go s.billing.run(ctx)
	return s, nil
}

// Enabled reports whether the platform is active on this cluster.
func (s *Service) Enabled() bool {
	return s != nil && s.config.Enabled && s.repo != nil
}

// AdminWorkspace is the cluster admin workspace that owns every managed stub and replica.
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
	if !workspace.StorageAvailable() {
		// The backend can retain a pre-migration admin workspace in its cache.
		// Reload storage from the database so an operator's migration takes
		// effect without restarting the gateway.
		workspace, err = s.backend.GetWorkspace(ctx, workspace.Id)
		if err != nil {
			return nil, fmt.Errorf("refresh cluster admin workspace storage: %w", err)
		}
		if workspace == nil || !workspace.StorageAvailable() {
			return nil, errors.New("managed endpoints require workspace storage for the cluster admin workspace; configure storage and migrate existing objects and volumes before starting hosted workloads")
		}
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
	if !ok || !clusterAdmin(authInfo) {
		return status.Error(codes.PermissionDenied, "cluster admin token required")
	}
	return nil
}

func clusterAdmin(a *auth.AuthInfo) bool {
	return a != nil && a.Token != nil && a.Token.TokenType == types.TokenTypeClusterAdmin
}

// workspaceCaller is a request authenticated with a workspace token of any kind.
func workspaceCaller(a *auth.AuthInfo) bool {
	return a != nil && a.Workspace != nil && a.Token != nil
}

// A replica's only credential is its own secret (BEAM_REPLICA_SECRET), which
// authorizes the harness RPCs.
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

// transport is the pooled transport for one replica address (route:// and tailscale aware).
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
	// Endpoint control events belong to the admin workspace. Without this
	// scope the event store writes a global type stream that the authenticated
	// history API cannot read. The workspace is cached after the first lookup.
	if event.WorkspaceID == "" {
		workspace, err := s.AdminWorkspace(s.ctx)
		if err != nil {
			log.Error().Err(err).Str("event_type", eventType).Msg("managed endpoints: could not scope audit event")
		} else {
			event.WorkspaceID = workspace.ExternalId
		}
	}
	if event.StubID == "" {
		event.StubID, _ = common.ExtractStubIdFromStubScopedContainerId(event.ContainerID)
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
		PoolName: replica.PoolName, Message: message, Data: data,
	})
}

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

// Leases. Both the per-replica capacity slot and the per-endpoint demand
// record are Redis sorted sets with one expiring member per request, so a
// crashed gateway's requests expire on their own while other traffic renews
// the key, and a late release cannot touch a newer request's reservation.

const (
	leaseTTL                    = time.Minute
	leaseRenewInterval          = 20 * time.Second
	leaseOpTimeout              = time.Second
	demandIdleTimeout           = 5 * time.Minute
	serverlessStartupTimeout    = 10 * time.Minute
	serverlessAdmissionHeadroom = 128
)

var (
	errSlotLeaseLost   = errors.New("hosted endpoint capacity lease lost")
	errDemandLeaseLost = errors.New("on-demand request lease expired")
	errDemandLimit     = errors.New("on-demand endpoint request limit reached")
)

// leaseOp is what a request does to a lease set.
type leaseOp string

const (
	leaseAcquire leaseOp = "acquire" // a request takes its member
	leaseRenew   leaseOp = "renew"   // a long request extends its member
	leaseRelease leaseOp = "release" // the request finished
	demandWake   leaseOp = "wake"    // a rejected request asks for a cold endpoint to start
	demandRead   leaseOp = "read"    // the controller samples demand
)

const slotScript = `
local now = redis.call('TIME')
now = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if ARGV[1] == 'release' then
  local removed = redis.call('ZREM', KEYS[1], ARGV[2])
  if redis.call('ZCARD', KEYS[1]) == 0 then redis.call('DEL', KEYS[1]) end
  return removed
end
local held = redis.call('ZSCORE', KEYS[1], ARGV[2])
if ARGV[1] == 'renew' and not held then return -1 end
if ARGV[1] == 'acquire' and not held and tonumber(ARGV[4]) > 0 then
  if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then return 0 end
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`

func slotKey(replicaID string) string { return "managed_endpoint:slots:" + replicaID }

// slot is one inflight reservation on a replica, bounding MaxConcurrency
// across gateways. It reports whether the request holds the slot afterwards.
func (s *Service) slot(ctx context.Context, replicaID string, op leaseOp, requestID string, capacity int64) (bool, error) {
	if s.rdb == nil || replicaID == "" || requestID == "" {
		return false, errors.New("hosted capacity requires redis and replica/request identities")
	}
	ctx, cancel := context.WithTimeout(ctx, leaseOpTimeout)
	defer cancel()
	result, err := s.rdb.Eval(ctx, slotScript, []string{slotKey(replicaID)}, string(op), requestID, leaseTTL.Milliseconds(), capacity).Int64()
	if err != nil {
		return false, err
	}
	if result < 0 {
		return false, errSlotLeaseLost
	}
	return result == 1, nil
}

// The demand set also carries idle and startup deadlines as the members
// ~idle and ~wake, so the controller reads active requests, whether the
// endpoint was used recently, and whether a rejected request asked for it.
const demandScript = `
local now = redis.call('TIME')
now = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local held = redis.call('ZSCORE', KEYS[1], ARGV[2])
if ARGV[1] == 'renew' and not held then return {-1, 0} end
if ARGV[1] == 'acquire' and not held then
  local count = redis.call('ZCARD', KEYS[1])
  if redis.call('ZSCORE', KEYS[1], '~idle') then count = count - 1 end
  if redis.call('ZSCORE', KEYS[1], '~wake') then count = count - 1 end
  if count >= tonumber(ARGV[5]) then return {-2, 0} end
end
if ARGV[1] == 'acquire' or ARGV[1] == 'renew' or (ARGV[1] == 'release' and held) then
  if ARGV[1] ~= 'release' then
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[2])
  else
    redis.call('ZREM', KEYS[1], ARGV[2])
  end
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), '~idle')
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[3]))
end
if ARGV[1] == 'wake' then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), '~wake')
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), '~idle')
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[3]))
end
local warm = redis.call('ZSCORE', KEYS[1], '~idle') and 1 or 0
local pending = redis.call('ZSCORE', KEYS[1], '~wake') and 1 or 0
return {redis.call('ZCARD', KEYS[1]) - warm - pending, warm, pending}
`

// demand records a serverless endpoint's live requests. readyCapacity is the
// finite serving capacity seen at admission; admissions beyond it get a
// bounded headroom so a full engine still records a scale-out signal, and
// an existing request can always renew.
func (s *Service) demand(ctx context.Context, endpointID string, op leaseOp, requestID string, readyCapacity int64) (*endpointDemand, error) {
	if s.rdb == nil {
		return nil, errors.New("on-demand endpoints require redis")
	}
	ctx, cancel := context.WithTimeout(ctx, leaseOpTimeout)
	defer cancel()
	limit := serverlessAdmissionHeadroom + min(max(readyCapacity, 0), math.MaxInt64-serverlessAdmissionHeadroom)
	values, err := s.rdb.Eval(ctx, demandScript, []string{"managed_endpoint:demand:" + endpointID},
		string(op), requestID, leaseTTL.Milliseconds(), demandIdleTimeout.Milliseconds(), limit).Int64Slice()
	if err != nil {
		return nil, err
	}
	switch {
	case values[0] == -2:
		return nil, errDemandLimit
	case values[0] < 0:
		return nil, errDemandLeaseLost
	}
	return &endpointDemand{active: values[0], warm: values[1] == 1, pending: values[2] == 1}, nil
}

// keepAlive renews a lease on every tick until stop is called or the service
// ends. A failed renewal cancels the returned context with cause; stop joins
// the loop so a late renewal can never revive completed work.
func (s *Service) keepAlive(parent context.Context, ticks <-chan time.Time, renew func(context.Context) error, cause error) (ctx context.Context, stop func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.ctx.Done():
				cancel(context.Canceled)
				return
			case <-ticks:
				if err := renew(ctx); err != nil {
					cancel(cause)
					return
				}
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled); <-done }
}
