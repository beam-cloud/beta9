package repository_services

import (
	"context"
	"errors"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const cacheWorkspaceObjectURLTTL = 5 * time.Minute

func (s *WorkerRepositoryService) cacheWorkspaceObjectURL(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) (string, error) {
	if req.Registry != "" || req.ImageId != "" || req.ImageArchiveUploadId != "" || req.Source == "" || req.SourceHash == "" {
		return "", errors.New("cache object request must identify one workspace object")
	}
	if err := s.authorizeRecentCacheStub(ctx, req); err != nil {
		return "", err
	}
	if s.eventRepo == nil || s.backendRepo == nil {
		return "", errors.New("cache origin metadata is unavailable")
	}
	if _, err := s.workerRuntimeStub(ctx, req.StubId, req.WorkspaceId); err != nil {
		return "", errors.New("cache origin stub is unavailable")
	}
	items, err := s.eventRepo.ReadStubCacheRequiredContent(ctx, req.WorkspaceId, req.StubId)
	if err != nil {
		return "", errors.New("cache required content is unavailable")
	}
	if !slices.ContainsFunc(items, func(item types.CacheRequiredContentItem) bool {
		return cacheWorkspaceOriginMatches(item, req)
	}) {
		return "", errors.New("workspace object is not recorded as required cache content")
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return "", errors.New("workspace storage is unavailable")
	}
	full, err := s.backendRepo.GetWorkspace(ctx, workspace.Id)
	if err != nil || full == nil || !full.StorageAvailable() {
		return "", errors.New("workspace storage is unavailable")
	}
	bucket := derefString(full.Storage.BucketName)
	if bucket == "" || (req.SourceBucket != "" && req.SourceBucket != bucket) {
		return "", errors.New("cache object is outside workspace storage")
	}
	client, err := clients.NewWorkspaceStorageClientWithDefaultPresignEndpoint(ctx, full.Name, full.Storage, s.appConfig.Storage.WorkspaceStorage)
	if err != nil {
		return "", errors.New("workspace storage is unavailable")
	}
	url, err := client.StorageClient.GeneratePresignedGetURL(ctx, req.Source, int64(cacheWorkspaceObjectURLTTL/time.Second), bucket)
	if err != nil {
		return "", errors.New("failed to sign cache object read")
	}
	return url, nil
}

func cacheWorkspaceOriginMatches(item types.CacheRequiredContentItem, req *pb.GetCacheOriginCredentialsRequest) bool {
	if item.Retired || item.Hash != req.SourceHash || item.Source != req.Source || item.SourceBucket != req.SourceBucket {
		return false
	}
	if path.Clean(item.Source) != item.Source {
		return false
	}
	root, _, ok := strings.Cut(item.Source, "/")
	if !ok {
		return false
	}
	switch item.Kind {
	case types.CacheContentKindVolume:
		return root == types.DefaultVolumesPrefix
	case types.CacheContentKindCheckpoint:
		return root == types.DefaultCheckpointsPrefix
	case types.CacheContentKindDiskSnapshot:
		return root == types.DefaultDurableDisksPrefix
	default:
		return false
	}
}

func (s *WorkerRepositoryService) authorizeCacheArchiveRead(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) error {
	if err := s.authorizeRecentCacheStub(ctx, req); err != nil {
		return err
	}
	return s.authorizeCacheArchiveImage(ctx, req)
}

func (s *WorkerRepositoryService) authorizeCacheArchiveImage(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) error {
	if s.backendRepo == nil {
		return errors.New("cache origin metadata is unavailable")
	}
	config, err := s.workerRuntimeStubConfig(ctx, req.StubId, req.WorkspaceId)
	if err != nil || config == nil || config.Runtime.ImageId != req.ImageId {
		return errors.New("image archive does not belong to the cache stub")
	}
	return nil
}

func (s *WorkerRepositoryService) authorizeRecentCacheStub(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) error {
	if s.cacheMetadata == nil || req.CacheLocality == "" || req.WorkspaceId == "" || req.StubId == "" {
		return errors.New("cache origin requires a recent stub in the cache locality")
	}
	worker, err := auth.WorkerFromContext(ctx)
	if (err != nil || worker.Trusted) && strings.HasPrefix(req.CacheLocality, req.WorkspaceId+cacheLocalityScopeSeparator) {
		return errors.New("cache origin is not in a shared cache locality")
	}
	stubs, err := s.cacheMetadata.ListRecentStubs(ctx, s.scopedCacheLocality(ctx, req.CacheLocality), s.recentCacheStubTTL(), 0)
	if err != nil {
		return errors.New("recent cache stubs are unavailable")
	}
	if !slices.ContainsFunc(stubs, func(stub cache.RecentStub) bool {
		return stub.WorkspaceID == req.WorkspaceId && stub.StubID == req.StubId
	}) {
		return errors.New("stub is not recent in the cache locality")
	}
	return nil
}

func (s *WorkerRepositoryService) workerHasCacheOriginAssignment(worker *auth.Worker, req *pb.GetCacheOriginCredentialsRequest) bool {
	if req.StubId == "" || req.WorkspaceId == "" {
		return false
	}
	return slices.ContainsFunc(worker.Containers(s.containerRepo, req.StubId), func(state types.ContainerState) bool {
		return state.WorkspaceId == req.WorkspaceId
	})
}
