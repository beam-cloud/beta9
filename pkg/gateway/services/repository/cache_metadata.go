package repository_services

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
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
	if err := s.cacheMetadata.SetStoreFromContentLock(ctx, s.scopedCacheLocality(ctx, req.Locality), req.SourcePath); err != nil {
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
	if err := s.cacheMetadata.RemoveStoreFromContentLock(ctx, s.scopedCacheLocality(ctx, req.Locality), req.SourcePath); err != nil {
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
	if err := s.cacheMetadata.RefreshStoreFromContentLock(ctx, s.scopedCacheLocality(ctx, req.Locality), req.SourcePath); err != nil {
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
	if err := s.cacheMetadata.AddRecentStub(ctx, s.scopedCacheLocality(ctx, req.Locality), req.WorkspaceId, req.StubId, time.Duration(req.TtlSeconds)*time.Second); err != nil {
		return &pb.AddRecentCacheStubResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AddRecentCacheStubResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) ListRecentCacheStubs(ctx context.Context, req *pb.ListRecentCacheStubsRequest) (*pb.ListRecentCacheStubsResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.ListRecentCacheStubsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	stubs, err := s.cacheMetadata.ListRecentStubs(ctx, s.scopedCacheLocality(ctx, req.Locality), time.Duration(req.TtlSeconds)*time.Second, int(req.Limit))
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

func (s *WorkerRepositoryService) AcquireCacheReconcileLock(ctx context.Context, req *pb.AcquireCacheReconcileLockRequest) (*pb.AcquireCacheReconcileLockResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.AcquireCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	acquired, err := s.cacheMetadata.AcquireReconcileLock(ctx, s.scopedCacheLocality(ctx, req.Locality), req.LogicalHost, req.Hash, int(req.TtlSeconds))
	if err != nil {
		return &pb.AcquireCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AcquireCacheReconcileLockResponse{Ok: true, Acquired: acquired}, nil
}

func (s *WorkerRepositoryService) ReleaseCacheReconcileLock(ctx context.Context, req *pb.ReleaseCacheReconcileLockRequest) (*pb.ReleaseCacheReconcileLockResponse, error) {
	if err := s.authorizeCacheMetadata(ctx); err != nil {
		return &pb.ReleaseCacheReconcileLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if err := s.cacheMetadata.ReleaseReconcileLock(ctx, s.scopedCacheLocality(ctx, req.Locality), req.LogicalHost, req.Hash); err != nil {
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
	if req == nil {
		return &pb.GetCacheOriginCredentialsResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}
	if err := s.authorizeOriginCredentials(ctx, req); err != nil {
		return &pb.GetCacheOriginCredentialsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if req.ImageArchiveUploadId != "" {
		url, err := s.imageArchiveUploadURL(ctx, req)
		if err != nil {
			return &pb.GetCacheOriginCredentialsResponse{ErrorMsg: err.Error()}, nil
		}
		return &pb.GetCacheOriginCredentialsResponse{Ok: true, ImageArchiveUploadUrl: url}, nil
	}

	registryCredentials := s.registryOriginCredentials(ctx, req.WorkspaceId, req.ImageId, req.Registry)
	if req.Registry != "" {
		// Direct OCI pulls only need registry credentials. Resolving workspace
		// storage and presigning image archives here adds database and object
		// store round trips to every cold mount without producing used data.
		resp := &pb.GetCacheOriginCredentialsResponse{
			Ok:                    true,
			RegistryCredentials:   registryCredentials,
			BuildRegistry:         s.appConfig.ImageService.EffectiveBuildRegistry(),
			BuildRepositoryName:   s.appConfig.ImageService.BuildRepositoryName,
			BuildRegistryInsecure: s.appConfig.ImageService.BuildRegistryInsecure,
		}
		// Untrusted workers reach the build registry only through the gateway's
		// registry proxy, with their own token.
		if worker, ok := untrustedWorker(ctx); ok {
			gatewayURL := s.appConfig.GatewayService.HTTP.GetExternalURL()
			resp.BuildRegistry = normalizeRegistryHost(gatewayURL)
			resp.BuildRegistryInsecure = strings.HasPrefix(gatewayURL, "http://")
			if registryHostsEqual(req.Registry, resp.BuildRegistry) {
				resp.RegistryCredentials = "beta9:" + worker.Token.Key
			}
		}
		return resp, nil
	}

	imageArchiveObjectKey, imageArchiveURL, imageArchiveDataURL := s.imageArchiveCredentials(ctx, req.ImageId)
	return &pb.GetCacheOriginCredentialsResponse{
		Ok:                    true,
		WorkspaceStorage:      s.workspaceStorageCredentials(ctx, req.WorkspaceId),
		ImageArchiveObjectKey: imageArchiveObjectKey,
		ImageArchiveUrl:       imageArchiveURL,
		ImageArchiveDataUrl:   imageArchiveDataURL,
		// Short-lived registry credentials for direct OCI pulls. Private image
		// credentials are resolved by image/workspace; build-registry credentials
		// are vended only for the exact configured build registry host.
		RegistryCredentials: registryCredentials,
	}, nil
}

// authorizeOriginCredentials limits untrusted workers (customer or provider
// hardware) to stubs of the workspace their token belongs to, and to a stub's
// own image. Provider machines belong to the platform's workspace without
// owning it, so they get credentials only for the stubs they run, as does an
// image export. Cluster workers and cache servers are trusted infrastructure
// that serves every workspace.
func (s *WorkerRepositoryService) authorizeOriginCredentials(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) error {
	worker, ok := untrustedWorker(ctx)
	if !ok {
		return nil
	}
	if !worker.InWorkspace(req.WorkspaceId) {
		return fmt.Errorf("worker token cannot request credentials for workspace %q", req.WorkspaceId)
	}
	if req.StubId == "" {
		return fmt.Errorf("worker token credential request requires a stub")
	}
	if req.ImageArchiveUploadId != "" || s.platformWorkspace(ctx, req.WorkspaceId) {
		if !worker.RunsStub(s.containerRepo, req.StubId) {
			return fmt.Errorf("stub %q is not running on this worker", req.StubId)
		}
	}
	if req.ImageId == "" {
		return nil
	}
	if s.backendRepo == nil {
		return fmt.Errorf("backend repository is unavailable")
	}
	config, err := s.workerRuntimeStubConfig(ctx, req.StubId, req.WorkspaceId)
	if err != nil {
		return err
	}
	if config == nil || config.Runtime.ImageId != req.ImageId {
		return fmt.Errorf("worker token cannot request credentials for image %q on stub %q", req.ImageId, req.StubId)
	}
	return nil
}

// platformWorkspace reports whether workspaceID is the platform's own
// workspace, or can't be ruled out as it.
func (s *WorkerRepositoryService) platformWorkspace(ctx context.Context, workspaceID string) bool {
	if s.backendRepo == nil {
		return true
	}
	admin, err := s.backendRepo.GetAdminWorkspace(ctx)
	return err != nil || admin == nil || admin.ExternalId == workspaceID
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
	pruneCutoff := s.staleCacheCheckpointPruneCutoff()
	checkpoints, err := s.backendRepo.ListStaleCheckpoints(ctx, activeKeys, pruneCutoff)
	if err != nil {
		return &pb.PruneStaleCacheCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	pruneIDs := make([]string, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if !cacheCheckpointPruneCandidate(checkpoint, pruneCutoff) {
			continue
		}
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

func (s *WorkerRepositoryService) staleCacheCheckpointPruneCutoff() time.Time {
	return time.Now().Add(-s.recentCacheStubTTL())
}

func cacheCheckpointPruneCandidate(checkpoint types.Checkpoint, pruneCutoff time.Time) bool {
	lastUsedAt := checkpoint.CreatedAt.Time
	if checkpoint.LastRestoredAt.Time.After(lastUsedAt) {
		lastUsedAt = checkpoint.LastRestoredAt.Time
	}
	if lastUsedAt.IsZero() {
		return false
	}
	return lastUsedAt.Before(pruneCutoff)
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
	if err != nil {
		return nil
	}
	return cacheWorkspaceStorageCredentials(full)
}

func cacheWorkspaceStorageCredentials(workspace *types.Workspace) *pb.CacheWorkspaceStorageCredentials {
	if workspace == nil || !workspace.StorageAvailable() {
		return nil
	}
	st := workspace.Storage
	return &pb.CacheWorkspaceStorageCredentials{
		EndpointUrl:    derefString(st.EndpointUrl),
		Region:         derefString(st.Region),
		BucketName:     derefString(st.BucketName),
		AccessKey:      derefString(st.AccessKey),
		SecretKey:      derefString(st.SecretKey),
		ForcePathStyle: true,
	}
}

const imageArchivePresignExpiry = 15 * time.Minute

// imageArchiveUploadURL presigns the upload of a filesystem snapshot of the
// request's stub, so no bucket credentials leave the cluster.
func (s *WorkerRepositoryService) imageArchiveUploadURL(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) (string, error) {
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return "", err
	}
	if !worker.InWorkspace(req.WorkspaceId) {
		return "", fmt.Errorf("worker token cannot access workspace %q", req.WorkspaceId)
	}
	if stubID, ok := types.SnapshotImageStub(req.ImageArchiveUploadId); !ok || req.StubId == "" || stubID != req.StubId {
		return "", fmt.Errorf("invalid filesystem export image ID")
	}
	if _, err := s.workerRuntimeStub(ctx, req.StubId, req.WorkspaceId); err != nil {
		return "", err
	}
	extension := reg.RemoteImageFileExtension
	if req.ImageArchiveUploadData {
		extension = reg.LocalImageFileExtension
	}
	objectKey := req.ImageArchiveUploadId + "." + extension
	store, err := s.imageArchivePresignStore(ctx, s.appConfig.ImageService.Registries.S3, objectKey)
	if err != nil {
		return "", err
	}
	return store.PresignPut(ctx, objectKey, imageArchivePresignExpiry)
}

// imageArchiveCredentials returns the image archive object key plus presigned
// GET URLs for the remote (.rclip) archive and the v1 data (.clip) archive, so
// private-pool workers can pull and reconcile image archives without holding
// S3 credentials.
func (s *WorkerRepositoryService) imageArchiveCredentials(ctx context.Context, imageID string) (string, string, string) {
	if s.appConfig.ImageService.RegistryStore != reg.S3ImageRegistryStore {
		return "", "", ""
	}

	st := s.appConfig.ImageService.Registries.S3
	if st.BucketName == "" || imageID == "" {
		return "", "", ""
	}

	objectKey := fmt.Sprintf("%s.%s", imageID, reg.RemoteImageFileExtension)
	dataObjectKey := fmt.Sprintf("%s.%s", imageID, reg.LocalImageFileExtension)

	store, err := s.imageArchivePresignStore(ctx, st, objectKey)
	if err != nil {
		log.Warn().Err(err).Str("image_id", imageID).Msg("failed to build image archive presign store")
		return objectKey, "", ""
	}

	url, err := store.PresignGet(ctx, objectKey, imageArchivePresignExpiry)
	if err != nil {
		log.Warn().Err(err).Str("image_id", imageID).Str("object_key", objectKey).Msg("failed to presign image archive")
		return objectKey, "", ""
	}
	dataURL, err := store.PresignGet(ctx, dataObjectKey, imageArchivePresignExpiry)
	if err != nil {
		log.Warn().Err(err).Str("image_id", imageID).Str("object_key", dataObjectKey).Msg("failed to presign image data archive")
		dataURL = ""
	}
	return objectKey, url, dataURL
}

// imageArchivePresignStore returns the S3 store used to presign image archive
// URLs. Presigning is a local operation that succeeds even when the signer has
// no bucket access, so ambient (role-based) credentials are only trusted after
// a probe request against the bucket; otherwise the configured static keys are
// used.
func (s *WorkerRepositoryService) imageArchivePresignStore(ctx context.Context, st types.S3ImageRegistryConfig, probeKey string) (*reg.S3Store, error) {
	if imageArchiveAmbientCredentialsAvailable() {
		store, err := reg.NewS3StoreWithOptions(st, reg.S3StoreOptions{UseAmbientCredentials: true})
		if err == nil {
			if _, probeErr := store.Exists(ctx, probeKey); probeErr == nil {
				return store, nil
			}
		}
	}
	return reg.NewS3Store(st)
}

func imageArchiveAmbientCredentialsAvailable() bool {
	return os.Getenv("AWS_ROLE_ARN") != "" || os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != ""
}

func (s *WorkerRepositoryService) buildRegistryCredentials(ctx context.Context, registry string) string {
	// Only vend build-registry credentials when the requested registry host is
	// exactly the configured build registry. Substring matching would leak
	// credentials to look-alike hosts (e.g. "<build-registry>.evil.com").
	if registry == "" || !registryHostsEqual(registry, s.appConfig.ImageService.EffectiveBuildRegistry()) {
		return ""
	}
	return BuildRegistryCredentials(ctx, s.appConfig.ImageService)
}

// BuildRegistryCredentials returns "user:password" credentials for the
// configured build registry, or "" when none are available.
func BuildRegistryCredentials(ctx context.Context, imageCfg types.ImageServiceConfig) string {
	buildRegistry := imageCfg.EffectiveBuildRegistry()
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
	// Build-registry credentials can push to every workspace's images.
	if _, ok := untrustedWorker(ctx); ok {
		return ""
	}
	return s.buildRegistryCredentials(ctx, registry)
}

// untrustedWorker returns the caller when it is a worker on customer or
// provider hardware. authorizeCacheRepositoryRequest has checked that a caller
// with a token is a worker.
func untrustedWorker(ctx context.Context) (*auth.Worker, bool) {
	worker, err := auth.WorkerFromContext(ctx)
	return worker, err == nil && !worker.Trusted
}

func (s *WorkerRepositoryService) imageRegistryCredentials(ctx context.Context, workspaceID, imageID string) string {
	if s.backendRepo == nil || workspaceID == "" || imageID == "" {
		return ""
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx, workspaceID)
	if err != nil {
		return ""
	}
	credentials, _ := s.backendRepo.GetImageCredentials(ctx, &workspace, imageID)
	return credentials
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
