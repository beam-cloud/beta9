package cache

import (
	"context"

	proto "github.com/beam-cloud/beta9/proto"
)

func (cs *Server) GetAvailableHosts(ctx context.Context, req *proto.CacheGetAvailableHostsRequest) (*proto.CacheGetAvailableHostsResponse, error) {
	Logger.Debugf("GetAvailableHosts[ACK] - [%s]", req.Locality)

	hosts, err := cs.coordinator.GetAvailableHosts(ctx, req.Locality)
	if err != nil {
		return &proto.CacheGetAvailableHostsResponse{Hosts: nil, Ok: false}, nil
	}

	protoHosts := make([]*proto.CacheHost, 0)
	for _, host := range hosts {
		protoHosts = append(protoHosts, &proto.CacheHost{HostId: host.HostId, Addr: host.Addr, PrivateIpAddr: host.PrivateAddr})
	}

	Logger.Debugf("GetAvailableHosts[OK] - [%s]", protoHosts)
	return &proto.CacheGetAvailableHostsResponse{Hosts: protoHosts, Ok: true}, nil
}

func (cs *Server) GetRegionConfig(ctx context.Context, req *proto.CacheGetRegionConfigRequest) (*proto.CacheGetRegionConfigResponse, error) {
	Logger.Debugf("GetRegionConfig[ACK] - [%s]", req.Locality)

	config, ok := cs.serverConfig.Regions[req.Locality]
	if !ok {
		return &proto.CacheGetRegionConfigResponse{Ok: false, ServerConfig: nil}, nil
	}

	return &proto.CacheGetRegionConfigResponse{Ok: true, ServerConfig: config.ServerConfig.ToProto()}, nil
}

func (cs *Server) SetClientLock(ctx context.Context, req *proto.CacheSetClientLockRequest) (*proto.CacheSetClientLockResponse, error) {
	Logger.Debugf("SetClientLock[ACK] - [%s]", req.Hash)

	err := cs.coordinator.SetClientLock(ctx, req.Hash, req.HostId)
	if err != nil {
		return &proto.CacheSetClientLockResponse{Ok: false}, nil
	}

	return &proto.CacheSetClientLockResponse{Ok: true}, nil
}

func (cs *Server) RemoveClientLock(ctx context.Context, req *proto.CacheRemoveClientLockRequest) (*proto.CacheRemoveClientLockResponse, error) {
	Logger.Debugf("RemoveClientLock[ACK] - [%s]", req.Hash)

	err := cs.coordinator.RemoveClientLock(ctx, req.Hash, req.HostId)
	if err != nil {
		return &proto.CacheRemoveClientLockResponse{Ok: false}, nil
	}

	return &proto.CacheRemoveClientLockResponse{Ok: true}, nil
}

func (cs *Server) SetStoreFromContentLock(ctx context.Context, req *proto.CacheSetStoreFromContentLockRequest) (*proto.CacheSetStoreFromContentLockResponse, error) {
	Logger.Debugf("SetStoreFromContentLock[ACK] - [%s]", req.SourcePath)

	err := cs.coordinator.SetStoreFromContentLock(ctx, req.Locality, req.SourcePath)
	if err != nil {
		return &proto.CacheSetStoreFromContentLockResponse{Ok: false}, nil
	}

	return &proto.CacheSetStoreFromContentLockResponse{Ok: true}, nil
}

func (cs *Server) RemoveStoreFromContentLock(ctx context.Context, req *proto.CacheRemoveStoreFromContentLockRequest) (*proto.CacheRemoveStoreFromContentLockResponse, error) {
	Logger.Debugf("RemoveStoreFromContentLock[ACK] - [%s]", req.SourcePath)

	err := cs.coordinator.RemoveStoreFromContentLock(ctx, req.Locality, req.SourcePath)
	if err != nil {
		return &proto.CacheRemoveStoreFromContentLockResponse{Ok: false}, nil
	}

	return &proto.CacheRemoveStoreFromContentLockResponse{Ok: true}, nil
}

func (cs *Server) RefreshStoreFromContentLock(ctx context.Context, req *proto.CacheRefreshStoreFromContentLockRequest) (*proto.CacheRefreshStoreFromContentLockResponse, error) {
	Logger.Debugf("RefreshStoreFromContentLock[ACK] - [%s]", req.SourcePath)

	err := cs.coordinator.RefreshStoreFromContentLock(ctx, req.Locality, req.SourcePath)
	if err != nil {
		return &proto.CacheRefreshStoreFromContentLockResponse{Ok: false}, nil
	}

	return &proto.CacheRefreshStoreFromContentLockResponse{Ok: true}, nil
}

func (cs *Server) SetFsNode(ctx context.Context, req *proto.CacheSetFsNodeRequest) (*proto.CacheSetFsNodeResponse, error) {
	if req == nil || req.Metadata == nil {
		return &proto.CacheSetFsNodeResponse{Ok: false}, nil
	}

	Logger.Debugf("SetFsNode[ACK] - [%s]", req.Id)

	metadata := &FSMetadata{
		ID:        req.Metadata.Id,
		PID:       req.Metadata.Pid,
		Name:      req.Metadata.Name,
		Hash:      req.Metadata.Hash,
		Path:      req.Metadata.Path,
		Ino:       req.Metadata.Ino,
		Size:      req.Metadata.Size,
		Blocks:    req.Metadata.Blocks,
		Atime:     req.Metadata.Atime,
		Mtime:     req.Metadata.Mtime,
		Ctime:     req.Metadata.Ctime,
		Atimensec: req.Metadata.Atimensec,
		Mtimensec: req.Metadata.Mtimensec,
		Ctimensec: req.Metadata.Ctimensec,
		Mode:      req.Metadata.Mode,
		Nlink:     req.Metadata.Nlink,
		Rdev:      req.Metadata.Rdev,
		Blksize:   req.Metadata.Blksize,
		Padding:   req.Metadata.Padding,
		Uid:       req.Metadata.Uid,
		Gid:       req.Metadata.Gid,
		Gen:       req.Metadata.Gen,
	}

	err := cs.coordinator.SetFsNode(ctx, req.Id, metadata)
	if err != nil {
		return &proto.CacheSetFsNodeResponse{Ok: false}, nil
	}

	return &proto.CacheSetFsNodeResponse{Ok: true}, nil
}

func (cs *Server) GetFsNode(ctx context.Context, req *proto.CacheGetFsNodeRequest) (*proto.CacheGetFsNodeResponse, error) {
	Logger.Debugf("GetFsNode[ACK] - [%s]", req.Id)

	metadata, err := cs.coordinator.GetFsNode(ctx, req.Id)
	if err != nil {
		return &proto.CacheGetFsNodeResponse{Metadata: nil, Ok: false}, nil
	}

	return &proto.CacheGetFsNodeResponse{Metadata: metadata.ToProto(), Ok: true}, nil
}

func (cs *Server) RemoveFsNode(ctx context.Context, req *proto.CacheRemoveFsNodeRequest) (*proto.CacheRemoveFsNodeResponse, error) {
	Logger.Debugf("RemoveFsNode[ACK] - [%s]", req.Id)

	err := cs.coordinator.RemoveFsNode(ctx, req.Id)
	if err != nil {
		return &proto.CacheRemoveFsNodeResponse{Ok: false}, nil
	}

	return &proto.CacheRemoveFsNodeResponse{Ok: true}, nil
}

func (cs *Server) RemoveFsNodeChild(ctx context.Context, req *proto.CacheRemoveFsNodeChildRequest) (*proto.CacheRemoveFsNodeChildResponse, error) {
	Logger.Debugf("RemoveFsNodeChild[ACK] - [%s]", req.Id)

	err := cs.coordinator.RemoveFsNodeChild(ctx, req.Pid, req.Id)
	if err != nil {
		return &proto.CacheRemoveFsNodeChildResponse{Ok: false}, nil
	}

	return &proto.CacheRemoveFsNodeChildResponse{Ok: true}, nil
}

func (cs *Server) GetFsNodeChildren(ctx context.Context, req *proto.CacheGetFsNodeChildrenRequest) (*proto.CacheGetFsNodeChildrenResponse, error) {
	Logger.Debugf("GetFsNodeChildren[ACK] - [%s]", req.Id)

	children, err := cs.coordinator.GetFsNodeChildren(ctx, req.Id)
	if err != nil {
		return &proto.CacheGetFsNodeChildrenResponse{Children: nil, Ok: false}, nil
	}

	protoChildren := make([]*proto.CacheFSMetadata, 0)
	for _, child := range children {
		protoChildren = append(protoChildren, child.ToProto())
	}

	return &proto.CacheGetFsNodeChildrenResponse{Children: protoChildren, Ok: true}, nil
}

func (cs *Server) AddFsNodeChild(ctx context.Context, req *proto.CacheAddFsNodeChildRequest) (*proto.CacheAddFsNodeChildResponse, error) {
	Logger.Debugf("AddFsNodeChild[ACK] - [%s]", req.Id)

	err := cs.coordinator.AddFsNodeChild(ctx, req.Pid, req.Id)
	if err != nil {
		return &proto.CacheAddFsNodeChildResponse{Ok: false}, nil
	}

	return &proto.CacheAddFsNodeChildResponse{Ok: true}, nil
}

func (cs *Server) AddHostToIndex(ctx context.Context, req *proto.CacheAddHostToIndexRequest) (*proto.CacheAddHostToIndexResponse, error) {
	if req == nil || req.Host == nil {
		return &proto.CacheAddHostToIndexResponse{Ok: false}, nil
	}

	Logger.Debugf("AddHostToIndex[ACK] - [%s]", req.Locality)

	host := &Host{
		HostId:      req.Host.HostId,
		Addr:        req.Host.Addr,
		PrivateAddr: req.Host.PrivateIpAddr,
	}

	err := cs.coordinator.AddHostToIndex(ctx, req.Locality, host)
	if err != nil {
		return &proto.CacheAddHostToIndexResponse{Ok: false}, nil
	}

	return &proto.CacheAddHostToIndexResponse{Ok: true}, nil
}

func (cs *Server) SetHostKeepAlive(ctx context.Context, req *proto.CacheSetHostKeepAliveRequest) (*proto.CacheSetHostKeepAliveResponse, error) {
	if req == nil || req.Host == nil {
		return &proto.CacheSetHostKeepAliveResponse{Ok: false}, nil
	}

	Logger.Debugf("SetHostKeepAlive[ACK] - [%s]", req.Locality)

	host := &Host{
		HostId:      req.Host.HostId,
		Addr:        req.Host.Addr,
		PrivateAddr: req.Host.PrivateIpAddr,
	}

	err := cs.coordinator.SetHostKeepAlive(ctx, req.Locality, host)
	if err != nil {
		return &proto.CacheSetHostKeepAliveResponse{Ok: false}, nil
	}

	return &proto.CacheSetHostKeepAliveResponse{Ok: true}, nil
}
