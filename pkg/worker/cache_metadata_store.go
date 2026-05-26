package worker

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
)

type gatewayCacheMetadataStore struct {
	client pb.WorkerRepositoryServiceClient
}

func newGatewayCacheMetadataStore(client pb.WorkerRepositoryServiceClient) cache.CacheMetadataStore {
	return &gatewayCacheMetadataStore{client: client}
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
	_, err := handleGRPCResponse(s.client.SetCacheFsNode(ctx, &pb.SetCacheFsNodeRequest{Id: id, Metadata: workerCacheFSMetadataToProto(metadata)}))
	return err
}

func (s *gatewayCacheMetadataStore) GetFsNode(ctx context.Context, id string) (*cache.FSMetadata, error) {
	resp, err := handleGRPCResponse(s.client.GetCacheFsNode(ctx, &pb.GetCacheFsNodeRequest{Id: id}))
	if err != nil {
		return nil, err
	}
	return workerCacheFSMetadataFromProto(resp.Metadata), nil
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
		children = append(children, workerCacheFSMetadataFromProto(child))
	}
	return children, nil
}

func (s *gatewayCacheMetadataStore) AddFsNodeChild(ctx context.Context, pid, id string) error {
	_, err := handleGRPCResponse(s.client.AddCacheFsNodeChild(ctx, &pb.AddCacheFsNodeChildRequest{Pid: pid, Id: id}))
	return err
}

func workerCacheFSMetadataToProto(metadata *cache.FSMetadata) *pb.WorkerCacheFSMetadata {
	if metadata == nil {
		return nil
	}
	return &pb.WorkerCacheFSMetadata{
		Id:        metadata.ID,
		Pid:       metadata.PID,
		Name:      metadata.Name,
		Path:      metadata.Path,
		Hash:      metadata.Hash,
		Ino:       metadata.Ino,
		Size:      metadata.Size,
		Blocks:    metadata.Blocks,
		Atime:     metadata.Atime,
		Mtime:     metadata.Mtime,
		Ctime:     metadata.Ctime,
		Atimensec: metadata.Atimensec,
		Mtimensec: metadata.Mtimensec,
		Ctimensec: metadata.Ctimensec,
		Mode:      metadata.Mode,
		Nlink:     metadata.Nlink,
		Rdev:      metadata.Rdev,
		Blksize:   metadata.Blksize,
		Padding:   metadata.Padding,
		Uid:       metadata.Uid,
		Gid:       metadata.Gid,
		Gen:       metadata.Gen,
	}
}

func workerCacheFSMetadataFromProto(metadata *pb.WorkerCacheFSMetadata) *cache.FSMetadata {
	if metadata == nil {
		return nil
	}
	return &cache.FSMetadata{
		ID:        metadata.Id,
		PID:       metadata.Pid,
		Name:      metadata.Name,
		Path:      metadata.Path,
		Hash:      metadata.Hash,
		Ino:       metadata.Ino,
		Size:      metadata.Size,
		Blocks:    metadata.Blocks,
		Atime:     metadata.Atime,
		Mtime:     metadata.Mtime,
		Ctime:     metadata.Ctime,
		Atimensec: metadata.Atimensec,
		Mtimensec: metadata.Mtimensec,
		Ctimensec: metadata.Ctimensec,
		Mode:      metadata.Mode,
		Nlink:     metadata.Nlink,
		Rdev:      metadata.Rdev,
		Blksize:   metadata.Blksize,
		Padding:   metadata.Padding,
		Uid:       metadata.Uid,
		Gid:       metadata.Gid,
		Gen:       metadata.Gen,
	}
}
