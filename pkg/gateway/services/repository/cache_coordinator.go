package repository_services

import (
	"context"
	"crypto/subtle"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/metadata"
)

var errCacheCoordinatorUnauthorized = errors.New("unauthorized cache coordinator request")

const cacheLocalityScopeSeparator = "/"

func configuredCacheCoordinatorToken(configured string) string {
	if token := os.Getenv(types.CacheCoordinatorTokenEnv); token != "" {
		return token
	}
	return configured
}

func (s *WorkerRepositoryService) authorizeCacheRepositoryRequest(ctx context.Context) error {
	if authInfo, ok := auth.AuthInfoFromContext(ctx); ok && authInfo != nil && authInfo.Token != nil {
		if authInfo.Token.TokenType == types.TokenTypeWorker {
			return nil
		}
		return errCacheCoordinatorUnauthorized
	}

	if s == nil || s.cacheCoordinatorToken == "" {
		return errCacheCoordinatorUnauthorized
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md["authorization"]) == 0 {
		return errCacheCoordinatorUnauthorized
	}

	token := strings.TrimPrefix(md["authorization"][0], "Bearer ")
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.cacheCoordinatorToken)) != 1 {
		return errCacheCoordinatorUnauthorized
	}

	return nil
}

// scopedCacheLocality isolates cache coordinator and metadata state per
// customer workspace for private-pool workers: their locality is rewritten to
// "<workspaceID>/<locality>" so colliding pool names across customers never
// share cache hosts, locks, or stubs. Admin-workspace worker tokens (in-cluster
// and external-pool workers) are exempt so they stay in the same unscoped
// namespace as the cache-server daemonset, which authenticates with the
// coordinator token and therefore has no workspace to scope by.
//
// Pool names are passed through untouched: an empty pool name simply means a
// locality-wide lookup, which is already workspace-isolated by the scoped
// locality, and the coordinator rejects registrations without a pool name.
func (s *WorkerRepositoryService) scopedCacheLocality(ctx context.Context, locality string) string {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || authInfo.Token.TokenType != types.TokenTypeWorker {
		return locality
	}
	if authInfo.Workspace == nil || authInfo.Workspace.ExternalId == "" || locality == "" {
		return locality
	}

	workspaceID := authInfo.Workspace.ExternalId
	if s.isAdminWorkspaceExternalId(ctx, workspaceID) {
		return locality
	}

	prefix := workspaceID + cacheLocalityScopeSeparator
	if strings.HasPrefix(locality, prefix) {
		return locality
	}
	return prefix + locality
}

// isAdminWorkspaceExternalId reports whether the given workspace external ID is
// the admin workspace, resolving it once and caching the result. When the admin
// workspace cannot be resolved this fails closed (returns false) so workspace
// scoping is preserved.
func (s *WorkerRepositoryService) isAdminWorkspaceExternalId(ctx context.Context, externalID string) bool {
	if s == nil || s.backendRepo == nil || externalID == "" {
		return false
	}

	s.adminWorkspaceMu.Lock()
	resolved, adminID := s.adminWorkspaceResolved, s.adminWorkspaceExternalId
	s.adminWorkspaceMu.Unlock()

	if !resolved {
		workspace, err := s.backendRepo.GetAdminWorkspace(ctx)
		if err != nil || workspace == nil {
			return false
		}
		adminID = workspace.ExternalId
		s.adminWorkspaceMu.Lock()
		s.adminWorkspaceResolved = true
		s.adminWorkspaceExternalId = adminID
		s.adminWorkspaceMu.Unlock()
	}

	return adminID != "" && adminID == externalID
}

func (s *WorkerRepositoryService) RegisterCacheHost(ctx context.Context, req *pb.RegisterCacheHostRequest) (*pb.RegisterCacheHostResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheCoordinator == nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil || req.Host == nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: "host is required"}, nil
	}

	host := cacheCoordinatorHostFromProto(req.Host)
	host.Locality = s.scopedCacheLocality(ctx, host.Locality)
	ttl := time.Duration(req.TtlSeconds) * time.Second
	if err := s.cacheCoordinator.RegisterHost(ctx, host, ttl); err != nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RegisterCacheHostResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) UnregisterCacheHost(ctx context.Context, req *pb.UnregisterCacheHostRequest) (*pb.UnregisterCacheHostResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheCoordinator == nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	if err := s.cacheCoordinator.UnregisterHost(ctx, req.PoolName, s.scopedCacheLocality(ctx, req.Locality), req.LogicalHostId, req.RegistrationId); err != nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.UnregisterCacheHostResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) ListCacheHosts(ctx context.Context, req *pb.ListCacheHostsRequest) (*pb.ListCacheHostsResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheCoordinator == nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	locality := s.scopedCacheLocality(ctx, req.Locality)
	hosts, err := s.cacheCoordinator.ListHosts(ctx, req.PoolName, locality)
	if err != nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if len(hosts) == 0 && req.PoolName != "" {
		// Fall back to locality-wide listing so hosts registered under another
		// pool name (e.g. the cache-server daemonset) remain discoverable when
		// the worker's own pool has no registered cache hosts.
		hosts, err = s.cacheCoordinator.ListHosts(ctx, "", locality)
		if err != nil {
			return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	resp := &pb.ListCacheHostsResponse{
		Ok:    true,
		Hosts: make([]*pb.CacheCoordinatorHost, 0, len(hosts)),
	}
	for _, host := range hosts {
		resp.Hosts = append(resp.Hosts, cacheCoordinatorHostToProto(host))
	}
	return resp, nil
}

func cacheCoordinatorHostFromProto(host *pb.CacheCoordinatorHost) cache.CoordinatorHost {
	if host == nil {
		return cache.CoordinatorHost{}
	}
	return cache.CoordinatorHost{
		LogicalHostID:    host.LogicalHostId,
		RegistrationID:   host.RegistrationId,
		PoolName:         host.PoolName,
		Locality:         host.Locality,
		NodeID:           host.NodeId,
		CachePathID:      host.CachePathId,
		Addr:             host.Addr,
		PrivateAddr:      host.PrivateAddr,
		CapacityUsagePct: float64(host.CapacityUsagePct),
	}
}

func cacheCoordinatorHostToProto(host cache.CoordinatorHost) *pb.CacheCoordinatorHost {
	return &pb.CacheCoordinatorHost{
		LogicalHostId:    host.LogicalHostID,
		RegistrationId:   host.RegistrationID,
		PoolName:         host.PoolName,
		Locality:         host.Locality,
		NodeId:           host.NodeID,
		CachePathId:      host.CachePathID,
		Addr:             host.Addr,
		PrivateAddr:      host.PrivateAddr,
		CapacityUsagePct: float32(host.CapacityUsagePct),
	}
}
