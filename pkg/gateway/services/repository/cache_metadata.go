package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *WorkerRepositoryService) SetCacheClientLock(ctx context.Context, req *pb.SetCacheClientLockRequest) (*pb.SetCacheClientLockResponse, error) {
	if s.cacheMetadata == nil {
		return &pb.SetCacheClientLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.SetClientLock(ctx, req.Hash, req.HostId); err != nil {
		return &pb.SetCacheClientLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.SetCacheClientLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheClientLock(ctx context.Context, req *pb.RemoveCacheClientLockRequest) (*pb.RemoveCacheClientLockResponse, error) {
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheClientLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveClientLock(ctx, req.Hash, req.HostId); err != nil {
		return &pb.RemoveCacheClientLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheClientLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetCacheStoreFromContentLock(ctx context.Context, req *pb.SetCacheStoreFromContentLockRequest) (*pb.SetCacheStoreFromContentLockResponse, error) {
	if s.cacheMetadata == nil {
		return &pb.SetCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.SetStoreFromContentLock(ctx, req.Locality, req.SourcePath); err != nil {
		return &pb.SetCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.SetCacheStoreFromContentLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheStoreFromContentLock(ctx context.Context, req *pb.RemoveCacheStoreFromContentLockRequest) (*pb.RemoveCacheStoreFromContentLockResponse, error) {
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveStoreFromContentLock(ctx, req.Locality, req.SourcePath); err != nil {
		return &pb.RemoveCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheStoreFromContentLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RefreshCacheStoreFromContentLock(ctx context.Context, req *pb.RefreshCacheStoreFromContentLockRequest) (*pb.RefreshCacheStoreFromContentLockResponse, error) {
	if s.cacheMetadata == nil {
		return &pb.RefreshCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RefreshStoreFromContentLock(ctx, req.Locality, req.SourcePath); err != nil {
		return &pb.RefreshCacheStoreFromContentLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RefreshCacheStoreFromContentLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetCacheFsNode(ctx context.Context, req *pb.SetCacheFsNodeRequest) (*pb.SetCacheFsNodeResponse, error) {
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
	if s.cacheMetadata == nil {
		return &pb.AddCacheFsNodeChildResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.AddFsNodeChild(ctx, req.Pid, req.Id); err != nil {
		return &pb.AddCacheFsNodeChildResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.AddCacheFsNodeChildResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheFsNode(ctx context.Context, req *pb.RemoveCacheFsNodeRequest) (*pb.RemoveCacheFsNodeResponse, error) {
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheFsNodeResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveFsNode(ctx, req.Id); err != nil {
		return &pb.RemoveCacheFsNodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheFsNodeResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveCacheFsNodeChild(ctx context.Context, req *pb.RemoveCacheFsNodeChildRequest) (*pb.RemoveCacheFsNodeChildResponse, error) {
	if s.cacheMetadata == nil {
		return &pb.RemoveCacheFsNodeChildResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if err := s.cacheMetadata.RemoveFsNodeChild(ctx, req.Pid, req.Id); err != nil {
		return &pb.RemoveCacheFsNodeChildResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RemoveCacheFsNodeChildResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) GetCacheFsNodeChildren(ctx context.Context, req *pb.GetCacheFsNodeChildrenRequest) (*pb.GetCacheFsNodeChildrenResponse, error) {
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
