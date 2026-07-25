package worker

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
)

type gatewayCacheMetadataStore struct {
	client pb.WorkerRepositoryServiceClient
}

func newGatewayCacheMetadataStore(client pb.WorkerRepositoryServiceClient) cache.CacheMetadataStore {
	return &gatewayCacheMetadataStore{client: client}
}

// clampInt32 safely narrows an int to int32, saturating instead of overflowing.
func clampInt32(v int) int32 {
	const maxInt32 = 1<<31 - 1
	switch {
	case v < 0:
		return 0
	case v > maxInt32:
		return maxInt32
	default:
		return int32(v)
	}
}

func (s *gatewayCacheMetadataStore) SetClientLock(ctx context.Context, hash string, host string) error {
	_, err := handleGRPCResponse(s.client.SetCacheClientLock(ctx, &pb.SetCacheClientLockRequest{Hash: hash, HostId: host}))
	return err
}

func (s *gatewayCacheMetadataStore) RemoveClientLock(ctx context.Context, hash string, host string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheClientLock(ctx, &pb.RemoveCacheClientLockRequest{Hash: hash, HostId: host}))
	return err
}

func (s *gatewayCacheMetadataStore) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	_, err := handleGRPCResponse(s.client.SetCacheStoreFromContentLock(ctx, &pb.SetCacheStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath}))
	return err
}

func (s *gatewayCacheMetadataStore) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheStoreFromContentLock(ctx, &pb.RemoveCacheStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath}))
	return err
}

func (s *gatewayCacheMetadataStore) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	_, err := handleGRPCResponse(s.client.RefreshCacheStoreFromContentLock(ctx, &pb.RefreshCacheStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath}))
	return err
}

func (s *gatewayCacheMetadataStore) SetFsNode(ctx context.Context, id string, metadata *cache.FSMetadata) error {
	_, err := handleGRPCResponse(s.client.SetCacheFsNode(ctx, &pb.SetCacheFsNodeRequest{Id: id, Metadata: metadata.ToWorkerCacheProto()}))
	return err
}

func (s *gatewayCacheMetadataStore) GetFsNode(ctx context.Context, id string) (*cache.FSMetadata, error) {
	resp, err := handleGRPCResponse(s.client.GetCacheFsNode(ctx, &pb.GetCacheFsNodeRequest{Id: id}))
	if err != nil {
		return nil, err
	}
	return cache.FSMetadataFromWorkerCacheProto(resp.Metadata), nil
}

func (s *gatewayCacheMetadataStore) RemoveFsNode(ctx context.Context, id string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheFsNode(ctx, &pb.RemoveCacheFsNodeRequest{Id: id}))
	return err
}

func (s *gatewayCacheMetadataStore) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheFsNodeChild(ctx, &pb.RemoveCacheFsNodeChildRequest{Pid: pid, Id: id}))
	return err
}

func (s *gatewayCacheMetadataStore) GetFsNodeChildren(ctx context.Context, id string) ([]*cache.FSMetadata, error) {
	resp, err := handleGRPCResponse(s.client.GetCacheFsNodeChildren(ctx, &pb.GetCacheFsNodeChildrenRequest{Id: id}))
	if err != nil {
		return nil, err
	}
	children := make([]*cache.FSMetadata, 0, len(resp.Children))
	for _, child := range resp.Children {
		children = append(children, cache.FSMetadataFromWorkerCacheProto(child))
	}
	return children, nil
}

func (s *gatewayCacheMetadataStore) AddFsNodeChild(ctx context.Context, pid, id string) error {
	_, err := handleGRPCResponse(s.client.AddCacheFsNodeChild(ctx, &pb.AddCacheFsNodeChildRequest{Pid: pid, Id: id}))
	return err
}

func (s *gatewayCacheMetadataStore) AddRecentStub(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	_, err := handleGRPCResponse(s.client.AddRecentCacheStub(ctx, &pb.AddRecentCacheStubRequest{
		Locality:    locality,
		WorkspaceId: workspaceID,
		StubId:      stubID,
		TtlSeconds:  int64(ttl / time.Second),
	}))
	return err
}

func (s *gatewayCacheMetadataStore) ListRecentStubs(ctx context.Context, locality string, ttl time.Duration, limit int) ([]cache.RecentStub, error) {
	resp, err := handleGRPCResponse(s.client.ListRecentCacheStubs(ctx, &pb.ListRecentCacheStubsRequest{
		Locality:   locality,
		TtlSeconds: int64(ttl / time.Second),
		Limit:      clampInt32(limit),
	}))
	if err != nil {
		return nil, err
	}
	stubs := make([]cache.RecentStub, 0, len(resp.Stubs))
	for _, stub := range resp.Stubs {
		stubs = append(stubs, cache.RecentStub{
			WorkspaceID: stub.WorkspaceId,
			StubID:      stub.StubId,
			LastSeen:    time.Unix(stub.LastSeenUnix, 0),
		})
	}
	return stubs, nil
}

func (s *gatewayCacheMetadataStore) MarkStubReported(ctx context.Context, locality, stubID string, ttl time.Duration) (bool, error) {
	resp, err := handleGRPCResponse(s.client.MarkCacheStubReported(ctx, &pb.MarkCacheStubReportedRequest{
		Locality:   locality,
		StubId:     stubID,
		TtlSeconds: int64(ttl / time.Second),
	}))
	if err != nil {
		return false, err
	}
	return resp.Claimed, nil
}

func (s *gatewayCacheMetadataStore) AcquireReconcileLock(ctx context.Context, locality, logicalHost, hash string, ttlSeconds int) (bool, error) {
	resp, err := handleGRPCResponse(s.client.AcquireCacheReconcileLock(ctx, &pb.AcquireCacheReconcileLockRequest{
		Locality:    locality,
		LogicalHost: logicalHost,
		Hash:        hash,
		TtlSeconds:  int64(ttlSeconds),
	}))
	if err != nil {
		return false, err
	}
	return resp.Acquired, nil
}

func (s *gatewayCacheMetadataStore) ReleaseReconcileLock(ctx context.Context, locality, logicalHost, hash string) error {
	_, err := handleGRPCResponse(s.client.ReleaseCacheReconcileLock(ctx, &pb.ReleaseCacheReconcileLockRequest{
		Locality:    locality,
		LogicalHost: logicalHost,
		Hash:        hash,
	}))
	return err
}
