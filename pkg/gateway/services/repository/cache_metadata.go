package repository_services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *WorkerRepositoryService) SetCacheClientLock(ctx context.Context, req *pb.SetCacheClientLockRequest) (*pb.SetCacheClientLockResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.SetCacheClientLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.SetCacheClientLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.SetClientLock(ctx, req.Hash, req.HostId); err != nil {
		return &pb.SetCacheClientLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.SetCacheClientLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheClientLock(ctx context.Context, req *pb.RemoveCacheClientLockRequest) (*pb.RemoveCacheClientLockResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.RemoveCacheClientLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheClientLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveClientLock(ctx, req.Hash, req.HostId); err != nil {
		return &pb.RemoveCacheClientLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheClientLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetCacheStoreFromContentLock(ctx context.Context, req *pb.SetCacheStoreFromContentLockRequest) (*pb.SetCacheStoreFromContentLockResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.SetCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.SetCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.SetStoreFromContentLock(ctx, req.Locality, req.SourcePath); err != nil {
		return &pb.SetCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.SetCacheStoreFromContentLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheStoreFromContentLock(ctx context.Context, req *pb.RemoveCacheStoreFromContentLockRequest) (*pb.RemoveCacheStoreFromContentLockResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.RemoveCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveStoreFromContentLock(ctx, req.Locality, req.SourcePath); err != nil {
		return &pb.RemoveCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheStoreFromContentLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RefreshCacheStoreFromContentLock(ctx context.Context, req *pb.RefreshCacheStoreFromContentLockRequest) (*pb.RefreshCacheStoreFromContentLockResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.RefreshCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.RefreshCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RefreshStoreFromContentLock(ctx, req.Locality, req.SourcePath); err != nil {
		return &pb.RefreshCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RefreshCacheStoreFromContentLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) AddRecentCacheStub(ctx context.Context, req *pb.AddRecentCacheStubRequest) (*pb.AddRecentCacheStubResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.AddRecentCacheStubResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.AddRecentCacheStubResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.AddRecentStub(ctx, req.Locality, req.WorkspaceId, req.StubId, time.Duration(req.TtlSeconds)*time.Second); err != nil {
		return &pb.AddRecentCacheStubResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AddRecentCacheStubResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) ListRecentCacheStubs(ctx context.Context, req *pb.ListRecentCacheStubsRequest) (*pb.ListRecentCacheStubsResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ListRecentCacheStubsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.ListRecentCacheStubsResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	stubs, err := s.cacheMetadata.ListRecentStubs(ctx, req.Locality, time.Duration(req.TtlSeconds)*time.Second, int(req.Limit))
	if err != nil {
		return &pb.ListRecentCacheStubsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	resp := &pb.ListRecentCacheStubsResponse{Ok: true, Stubs: make([]*pb.RecentCacheStub, 0, len(stubs))}
	for _, stub := range stubs {
		resp.Stubs = append(resp.Stubs, &pb.RecentCacheStub{
			WorkspaceId:  stub.WorkspaceID,
			StubId:       stub.StubID,
			LastSeenUnix: stub.LastSeen.Unix(),
		})
	}
	return resp, nil
}

func (s *WorkerRepositoryService) MarkCacheStubReported(ctx context.Context, req *pb.MarkCacheStubReportedRequest) (*pb.MarkCacheStubReportedResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.MarkCacheStubReportedResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.MarkCacheStubReportedResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	claimed, err := s.cacheMetadata.MarkStubReported(ctx, req.Locality, req.StubId, time.Duration(req.TtlSeconds)*time.Second)
	if err != nil {
		return &pb.MarkCacheStubReportedResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.MarkCacheStubReportedResponse{Ok: true, Claimed: claimed}, nil
}

func (s *WorkerRepositoryService) AcquireCacheReconcileLock(ctx context.Context, req *pb.AcquireCacheReconcileLockRequest) (*pb.AcquireCacheReconcileLockResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.AcquireCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.AcquireCacheReconcileLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	acquired, err := s.cacheMetadata.AcquireReconcileLock(ctx, req.Locality, req.LogicalHost, req.Hash, int(req.TtlSeconds))
	if err != nil {
		return &pb.AcquireCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AcquireCacheReconcileLockResponse{Ok: true, Acquired: acquired}, nil
}

func (s *WorkerRepositoryService) ReleaseCacheReconcileLock(ctx context.Context, req *pb.ReleaseCacheReconcileLockRequest) (*pb.ReleaseCacheReconcileLockResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ReleaseCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.ReleaseCacheReconcileLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.ReleaseReconcileLock(ctx, req.Locality, req.LogicalHost, req.Hash); err != nil {
		return &pb.ReleaseCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.ReleaseCacheReconcileLockResponse{Ok: true}, nil
}

// GetCacheOriginCredentials vends short-lived origin credentials for cache
// reconciliation so workers never store registry or workspace storage secrets.
// Credentials are resolved from the backend/config here and used in-memory by
// the worker only; they are never persisted to disk, Redis, or S2.
func (s *WorkerRepositoryService) GetCacheOriginCredentials(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) (*pb.GetCacheOriginCredentialsResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.GetCacheOriginCredentialsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	resp := &pb.GetCacheOriginCredentialsResponse{Ok: true}

	// Workspace storage credentials (used for geesefs/volume content fetches).
	if s.backendRepo != nil && req.WorkspaceId != "" {
		if ws, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId); err == nil {
			if full, err := s.backendRepo.GetWorkspace(ctx, ws.Id); err == nil && full.StorageAvailable() {
				st := full.Storage
				resp.WorkspaceStorage = &pb.CacheWorkspaceStorageCredentials{
					EndpointUrl:    derefString(st.EndpointUrl),
					Region:         derefString(st.Region),
					BucketName:     derefString(st.BucketName),
					AccessKey:      derefString(st.AccessKey),
					SecretKey:      derefString(st.SecretKey),
					ForcePathStyle: true,
				}
			}
		}
	}

	// Short-lived registry credentials for OCI layer pulls from the build registry.
	resp.RegistryCredentials = s.buildRegistryCredentials(ctx, req.Registry)

	return resp, nil
}

func (s *WorkerRepositoryService) buildRegistryCredentials(ctx context.Context, registry string) string {
	imageCfg := s.appConfig.ImageService
	buildRegistry := imageCfg.BuildRegistry
	if buildRegistry == "" || registry == "" {
		return ""
	}
	// Only vend build-registry credentials for the configured build registry host.
	if !strings.Contains(registry, buildRegistry) && !strings.Contains(buildRegistry, registry) {
		return ""
	}

	dummyImageRef := fmt.Sprintf("%s/%s:dummy", buildRegistry, imageCfg.BuildRepositoryName)
	creds := imageCfg.BuildRegistryCredentials
	if creds.Type != "" && len(creds.Credentials) > 0 {
		if token, err := reg.GetRegistryTokenForImage(dummyImageRef, creds.Credentials); err == nil && token != "" {
			return token
		}
	}
	if reg.IsECRRegistry(buildRegistry) {
		if token, err := reg.GetAmbientECRTokenForImage(ctx, dummyImageRef); err == nil && token != "" {
			return token
		}
	}
	return ""
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (s *WorkerRepositoryService) SetCacheFsNode(ctx context.Context, req *pb.SetCacheFsNodeRequest) (*pb.SetCacheFsNodeResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.SetCacheFsNodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.SetCacheFsNodeResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil || req.Metadata == nil {
		return &pb.SetCacheFsNodeResponse{Ok: false, ErrorMsg: "metadata is required"}, nil
	}
	if err := s.cacheMetadata.SetFsNode(ctx, req.Id, cache.FSMetadataFromWorkerCacheProto(req.Metadata)); err != nil {
		return &pb.SetCacheFsNodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.SetCacheFsNodeResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) GetCacheFsNode(ctx context.Context, req *pb.GetCacheFsNodeRequest) (*pb.GetCacheFsNodeResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.GetCacheFsNodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.GetCacheFsNodeResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	metadata, err := s.cacheMetadata.GetFsNode(ctx, req.Id)
	if err != nil {
		return &pb.GetCacheFsNodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.GetCacheFsNodeResponse{Ok: true, Metadata: metadata.ToWorkerCacheProto()}, nil
}

func (s *WorkerRepositoryService) AddCacheFsNodeChild(ctx context.Context, req *pb.AddCacheFsNodeChildRequest) (*pb.AddCacheFsNodeChildResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.AddCacheFsNodeChildResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.AddCacheFsNodeChildResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.AddFsNodeChild(ctx, req.Pid, req.Id); err != nil {
		return &pb.AddCacheFsNodeChildResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AddCacheFsNodeChildResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheFsNode(ctx context.Context, req *pb.RemoveCacheFsNodeRequest) (*pb.RemoveCacheFsNodeResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.RemoveCacheFsNodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheFsNodeResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveFsNode(ctx, req.Id); err != nil {
		return &pb.RemoveCacheFsNodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheFsNodeResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheFsNodeChild(ctx context.Context, req *pb.RemoveCacheFsNodeChildRequest) (*pb.RemoveCacheFsNodeChildResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.RemoveCacheFsNodeChildResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheFsNodeChildResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveFsNodeChild(ctx, req.Pid, req.Id); err != nil {
		return &pb.RemoveCacheFsNodeChildResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheFsNodeChildResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) GetCacheFsNodeChildren(ctx context.Context, req *pb.GetCacheFsNodeChildrenRequest) (*pb.GetCacheFsNodeChildrenResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.GetCacheFsNodeChildrenResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheMetadata == nil {
		return &pb.GetCacheFsNodeChildrenResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	children, err := s.cacheMetadata.GetFsNodeChildren(ctx, req.Id)
	if err != nil {
		return &pb.GetCacheFsNodeChildrenResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	resp := &pb.GetCacheFsNodeChildrenResponse{Ok: true, Children: make([]*pb.WorkerCacheFSMetadata, 0, len(children))}
	for _, child := range children {
		resp.Children = append(resp.Children, child.ToWorkerCacheProto())
	}
	return resp, nil
}
