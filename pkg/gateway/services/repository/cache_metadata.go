package repository_services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
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

// authorizeCacheMetadata authorizes a cache coordinator request and ensures the
// metadata store is configured, returning a single error for handlers to map to
// their response type.
func (s *WorkerRepositoryService) authorizeCacheMetadata(ctx context.Context) error {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return err
	}
	if s.cacheMetadata == nil {
		return cache.ErrCoordinatorUnavailable
	}
	return nil
}

func (s *WorkerRepositoryService) AddRecentCacheStub(ctx context.Context, req *pb.AddRecentCacheStubRequest) (*pb.AddRecentCacheStubResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.AddRecentCacheStubResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if err := s.cacheMetadata.AddRecentStub(ctx, req.Locality, req.WorkspaceId, req.StubId, time.Duration(req.TtlSeconds)*time.Second); err != nil {
		return &pb.AddRecentCacheStubResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AddRecentCacheStubResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) ListRecentCacheStubs(ctx context.Context, req *pb.ListRecentCacheStubsRequest) (*pb.ListRecentCacheStubsResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.ListRecentCacheStubsResponse{Ok: false, ErrorMsg: err.Error()}, nil
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
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.MarkCacheStubReportedResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	claimed, err := s.cacheMetadata.MarkStubReported(ctx, req.Locality, req.StubId, time.Duration(req.TtlSeconds)*time.Second)
	if err != nil {
		return &pb.MarkCacheStubReportedResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.MarkCacheStubReportedResponse{Ok: true, Claimed: claimed}, nil
}

func (s *WorkerRepositoryService) AcquireCacheReconcileLock(ctx context.Context, req *pb.AcquireCacheReconcileLockRequest) (*pb.AcquireCacheReconcileLockResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.AcquireCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	acquired, err := s.cacheMetadata.AcquireReconcileLock(ctx, req.Locality, req.LogicalHost, req.Hash, int(req.TtlSeconds))
	if err != nil {
		return &pb.AcquireCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AcquireCacheReconcileLockResponse{Ok: true, Acquired: acquired}, nil
}

func (s *WorkerRepositoryService) ReleaseCacheReconcileLock(ctx context.Context, req *pb.ReleaseCacheReconcileLockRequest) (*pb.ReleaseCacheReconcileLockResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.ReleaseCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
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
	if err := authorizeOriginCredentialWorkspace(ctx, req.WorkspaceId); err != nil {
		return &pb.GetCacheOriginCredentialsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	imageArchiveStorage, imageArchiveObjectKey := s.imageArchiveStorageCredentials(req.ImageId)
	return &pb.GetCacheOriginCredentialsResponse{
		Ok:                    true,
		WorkspaceStorage:      s.workspaceStorageCredentials(ctx, req.WorkspaceId),
		ImageArchiveStorage:   imageArchiveStorage,
		ImageArchiveObjectKey: imageArchiveObjectKey,
		// Short-lived registry credentials for direct OCI pulls. Private image
		// credentials are resolved by image/workspace; build-registry credentials
		// are vended only for the exact configured build registry host.
		RegistryCredentials: s.registryOriginCredentials(ctx, req.WorkspaceId, req.ImageId, req.Registry),
	}, nil
}

func authorizeOriginCredentialWorkspace(ctx context.Context, workspaceID string) error {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || authInfo.Token.TokenType != types.TokenTypeWorker {
		return nil
	}
	if authInfo.Workspace == nil || authInfo.Workspace.ExternalId == "" {
		return fmt.Errorf("worker token is not scoped to a workspace")
	}
	if workspaceID == "" || workspaceID != authInfo.Workspace.ExternalId {
		return fmt.Errorf("worker token cannot request credentials for workspace %q", workspaceID)
	}
	return nil
}

func (s *WorkerRepositoryService) PruneStaleCacheCheckpoints(ctx context.Context, _ *pb.PruneStaleCacheCheckpointsRequest) (*pb.PruneStaleCacheCheckpointsResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.backendRepo == nil {
		return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: "backend repository is unavailable"}, nil
	}

	activeKeys, err := s.activeRecentCacheStubKeys(ctx)
	if err != nil {
		return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	checkpoints, err := s.backendRepo.ListStaleCheckpoints(ctx, activeKeys)
	if err != nil {
		return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	pruneIDs := make([]string, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if checkpoint.OriginKey != "" {
			workspace, err := s.backendRepo.GetWorkspace(ctx, checkpoint.WorkspaceId)
			if err != nil {
				return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
			}
			if !workspace.StorageAvailable() {
				return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: fmt.Sprintf("workspace storage is unavailable for checkpoint %s", checkpoint.CheckpointId)}, nil
			}
			storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
			if err != nil {
				return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
			}
			if err := storageClient.Delete(ctx, checkpoint.OriginKey); err != nil {
				return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
			}
		}
		pruneIDs = append(pruneIDs, checkpoint.CheckpointId)
	}

	pruned, err := s.backendRepo.PruneCheckpoints(ctx, pruneIDs)
	if err != nil {
		return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.PruneStaleCacheCheckpointsResponse{Ok: true, Pruned: int32(len(pruned))}, nil
}

type anyLocalityRecentStubStore interface {
	ListRecentStubsAnyLocality(ctx context.Context, ttl time.Duration) ([]cache.RecentStub, error)
}

func (s *WorkerRepositoryService) activeRecentCacheStubKeys(ctx context.Context) ([]string, error) {
	store, ok := s.cacheMetadata.(anyLocalityRecentStubStore)
	if !ok {
		return nil, fmt.Errorf("cache metadata store cannot list recent stubs across localities")
	}

	stubs, err := store.ListRecentStubsAnyLocality(ctx, s.recentCacheStubTTL())
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(stubs))
	seen := map[string]struct{}{}
	for _, stub := range stubs {
		if stub.WorkspaceID == "" || stub.StubID == "" {
			continue
		}
		key := cache.RecentStubKey(stub.WorkspaceID, stub.StubID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, nil
}

func (s *WorkerRepositoryService) recentCacheStubTTL() time.Duration {
	seconds := s.appConfig.Cache.Reconciliation.RecentStubTTLSeconds
	if seconds <= 0 {
		seconds = cache.DefaultReconcileRecentStubTTLS
	}
	return time.Duration(seconds) * time.Second
}

// workspaceStorageCredentials resolves a workspace's (decrypted) object storage
// credentials, or nil when unavailable. Used for geesefs/volume content fetches.
func (s *WorkerRepositoryService) workspaceStorageCredentials(ctx context.Context, workspaceID string) *pb.CacheWorkspaceStorageCredentials {
	if s.backendRepo == nil || workspaceID == "" {
		return nil
	}

	ws, err := s.backendRepo.GetWorkspaceByExternalId(ctx, workspaceID)
	if err != nil {
		return nil
	}
	full, err := s.backendRepo.GetWorkspace(ctx, ws.Id)
	if err != nil || !full.StorageAvailable() {
		return nil
	}

	st := full.Storage
	return &pb.CacheWorkspaceStorageCredentials{
		EndpointUrl:    derefString(st.EndpointUrl),
		Region:         derefString(st.Region),
		BucketName:     derefString(st.BucketName),
		AccessKey:      derefString(st.AccessKey),
		SecretKey:      derefString(st.SecretKey),
		ForcePathStyle: true,
	}
}

func (s *WorkerRepositoryService) imageArchiveStorageCredentials(imageID string) (*pb.CacheWorkspaceStorageCredentials, string) {
	if s.appConfig.ImageService.RegistryStore != reg.S3ImageRegistryStore {
		return nil, ""
	}

	st := s.appConfig.ImageService.Registries.S3
	if st.BucketName == "" {
		return nil, ""
	}

	objectKey := ""
	if imageID != "" {
		objectKey = fmt.Sprintf("%s.%s", imageID, reg.RemoteImageFileExtension)
	}
	return &pb.CacheWorkspaceStorageCredentials{
		EndpointUrl:    st.Endpoint,
		Region:         st.Region,
		BucketName:     st.BucketName,
		AccessKey:      st.AccessKey,
		SecretKey:      st.SecretKey,
		ForcePathStyle: st.ForcePathStyle,
	}, objectKey
}

func (s *WorkerRepositoryService) buildRegistryCredentials(ctx context.Context, registry string) string {
	imageCfg := s.appConfig.ImageService
	buildRegistry := imageCfg.BuildRegistry
	if buildRegistry == "" || registry == "" {
		return ""
	}
	// Only vend build-registry credentials when the requested registry host is
	// exactly the configured build registry. Substring matching would leak
	// credentials to look-alike hosts (e.g. "<build-registry>.evil.com").
	if !registryHostsEqual(registry, buildRegistry) {
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

func (s *WorkerRepositoryService) registryOriginCredentials(ctx context.Context, workspaceID, imageID, registry string) string {
	if credentials := s.imageRegistryCredentials(ctx, workspaceID, imageID); credentials != "" {
		return credentials
	}
	return s.buildRegistryCredentials(ctx, registry)
}

func (s *WorkerRepositoryService) imageRegistryCredentials(ctx context.Context, workspaceID, imageID string) string {
	if s.backendRepo == nil || workspaceID == "" || imageID == "" {
		return ""
	}

	secretName, _, err := s.backendRepo.GetImageCredentialSecret(ctx, imageID)
	if err != nil || secretName == "" {
		return ""
	}

	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, workspaceID)
	if err != nil {
		return ""
	}
	fullWorkspace, err := s.backendRepo.GetWorkspace(ctx, workspace.Id)
	if err != nil {
		return ""
	}

	secret, err := s.backendRepo.GetSecretByNameDecrypted(ctx, fullWorkspace, secretName)
	if err != nil || secret == nil {
		return ""
	}
	return secret.Value
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// registryHostsEqual reports whether two registry references resolve to the same
// host[:port], ignoring scheme and any path component.
func registryHostsEqual(a, b string) bool {
	return normalizeRegistryHost(a) == normalizeRegistryHost(b)
}

func normalizeRegistryHost(registry string) string {
	registry = strings.TrimPrefix(registry, "https://")
	registry = strings.TrimPrefix(registry, "http://")
	registry = strings.Trim(registry, "/")
	if i := strings.IndexByte(registry, '/'); i >= 0 {
		registry = registry[:i]
	}
	return registry
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
