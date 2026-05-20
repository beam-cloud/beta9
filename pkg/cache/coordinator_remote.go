package cache

import (
	"context"
	"crypto/tls"
	"errors"

	proto "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type RemoteRegistry struct {
	host   string
	cfg    GlobalConfig
	client proto.CacheClient
}

func NewRemoteRegistry(cfg GlobalConfig, token string) (Registry, error) {
	addr := cfg.CoordinatorHost

	transportCredentials := grpc.WithTransportCredentials(insecure.NewCredentials())
	if isTLSEnabled(addr) {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		transportCredentials = grpc.WithTransportCredentials(h2creds)
	}

	var dialOpts = []grpc.DialOption{
		transportCredentials,
		grpc.WithContextDialer(DialWithTimeout),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.GRPCMessageSizeBytes),
			grpc.MaxCallSendMsgSize(cfg.GRPCMessageSizeBytes),
		),
	}

	if token != "" {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(grpcAuthInterceptor(token)))
	}

	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return nil, err
	}

	return &RemoteRegistry{cfg: cfg, host: cfg.CoordinatorHost, client: proto.NewCacheClient(conn)}, nil
}

func (c *RemoteRegistry) AddHostToIndex(ctx context.Context, locality string, host *Host) error {
	r, err := c.client.AddHostToIndex(ctx, &proto.CacheAddHostToIndexRequest{Locality: locality, Host: &proto.CacheHost{HostId: host.HostId, Addr: host.Addr, PrivateIpAddr: host.PrivateAddr}})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to add host to index")
	}

	return nil
}

func (c *RemoteRegistry) SetHostKeepAlive(ctx context.Context, locality string, host *Host) error {
	r, err := c.client.SetHostKeepAlive(ctx, &proto.CacheSetHostKeepAliveRequest{Locality: locality, Host: &proto.CacheHost{HostId: host.HostId, Addr: host.Addr, PrivateIpAddr: host.PrivateAddr}})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to set host keep alive")
	}

	return nil
}

func (c *RemoteRegistry) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	r, err := c.client.SetStoreFromContentLock(ctx, &proto.CacheSetStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to set store from content lock")
	}

	return nil
}

func (c *RemoteRegistry) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	r, err := c.client.RemoveStoreFromContentLock(ctx, &proto.CacheRemoveStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to remove store from content lock")
	}

	return nil
}

func (c *RemoteRegistry) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	r, err := c.client.RefreshStoreFromContentLock(ctx, &proto.CacheRefreshStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to refresh store from content lock")
	}

	return nil
}

func (c *RemoteRegistry) GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error) {
	response, err := c.client.GetAvailableHosts(ctx, &proto.CacheGetAvailableHostsRequest{Locality: locality})
	if err != nil {
		return nil, err
	}

	hosts := make([]*Host, 0)
	for _, host := range response.Hosts {
		hosts = append(hosts, &Host{
			HostId:      host.HostId,
			Addr:        host.Addr,
			PrivateAddr: host.PrivateIpAddr,
		})
	}

	return hosts, nil
}

func (c *RemoteRegistry) GetRegionConfig(ctx context.Context, locality string) (ServerConfig, error) {
	response, err := c.client.GetRegionConfig(ctx, &proto.CacheGetRegionConfigRequest{Locality: locality})
	if err != nil {
		return ServerConfig{}, err
	}

	if !response.Ok {
		return ServerConfig{}, errors.New("failed to get region config")
	}

	cfg := ServerConfigFromProto(response.ServerConfig)
	return cfg, nil
}

func (c *RemoteRegistry) SetClientLock(ctx context.Context, hash string, hostId string) error {
	r, err := c.client.SetClientLock(ctx, &proto.CacheSetClientLockRequest{Hash: hash, HostId: hostId})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to set client lock")
	}

	return nil
}

func (c *RemoteRegistry) RemoveClientLock(ctx context.Context, hash string, hostId string) error {
	r, err := c.client.RemoveClientLock(ctx, &proto.CacheRemoveClientLockRequest{Hash: hash, HostId: hostId})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to remove client lock")
	}

	return nil
}

func (c *RemoteRegistry) SetFsNode(ctx context.Context, id string, metadata *FSMetadata) error {
	r, err := c.client.SetFsNode(ctx, &proto.CacheSetFsNodeRequest{
		Id: id,
		Metadata: &proto.CacheFSMetadata{
			Id:        metadata.ID,
			Pid:       metadata.PID,
			Name:      metadata.Name,
			Path:      metadata.Path,
			Hash:      metadata.Hash,
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
		},
	})

	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to set fs node")
	}

	return nil
}

func (c *RemoteRegistry) GetFsNode(ctx context.Context, id string) (*FSMetadata, error) {
	response, err := c.client.GetFsNode(ctx, &proto.CacheGetFsNodeRequest{Id: id})
	if err != nil {
		return nil, err
	}

	if !response.Ok {
		return nil, errors.New("failed to get fs node")
	}

	return &FSMetadata{
		ID:        response.Metadata.Id,
		PID:       response.Metadata.Pid,
		Name:      response.Metadata.Name,
		Path:      response.Metadata.Path,
		Hash:      response.Metadata.Hash,
		Size:      response.Metadata.Size,
		Blocks:    response.Metadata.Blocks,
		Atime:     response.Metadata.Atime,
		Mtime:     response.Metadata.Mtime,
		Ctime:     response.Metadata.Ctime,
		Atimensec: response.Metadata.Atimensec,
		Mtimensec: response.Metadata.Mtimensec,
		Ctimensec: response.Metadata.Ctimensec,
		Mode:      response.Metadata.Mode,
		Nlink:     response.Metadata.Nlink,
		Rdev:      response.Metadata.Rdev,
		Blksize:   response.Metadata.Blksize,
		Padding:   response.Metadata.Padding,
		Uid:       response.Metadata.Uid,
		Gid:       response.Metadata.Gid,
		Gen:       response.Metadata.Gen,
	}, nil
}

func (c *RemoteRegistry) AddFsNodeChild(ctx context.Context, pid, id string) error {
	r, err := c.client.AddFsNodeChild(ctx, &proto.CacheAddFsNodeChildRequest{Pid: pid, Id: id})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to add fs node child")
	}

	return nil
}

func (c *RemoteRegistry) RemoveFsNode(ctx context.Context, id string) error {
	r, err := c.client.RemoveFsNode(ctx, &proto.CacheRemoveFsNodeRequest{Id: id})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to remove fs node")
	}

	return nil
}

func (c *RemoteRegistry) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
	r, err := c.client.RemoveFsNodeChild(ctx, &proto.CacheRemoveFsNodeChildRequest{Pid: pid, Id: id})
	if err != nil {
		return err
	}

	if !r.Ok {
		return errors.New("failed to remove fs node child")
	}

	return nil
}

func (c *RemoteRegistry) GetFsNodeChildren(ctx context.Context, id string) ([]*FSMetadata, error) {
	response, err := c.client.GetFsNodeChildren(ctx, &proto.CacheGetFsNodeChildrenRequest{Id: id})
	if err != nil {
		return nil, err
	}

	if !response.Ok {
		return nil, errors.New("failed to get fs node children")
	}

	children := make([]*FSMetadata, 0)
	for _, child := range response.Children {
		children = append(children, &FSMetadata{
			ID:        child.Id,
			PID:       child.Pid,
			Name:      child.Name,
			Path:      child.Path,
			Hash:      child.Hash,
			Size:      child.Size,
			Blocks:    child.Blocks,
			Atime:     child.Atime,
			Mtime:     child.Mtime,
			Ctime:     child.Ctime,
			Atimensec: child.Atimensec,
			Mtimensec: child.Mtimensec,
			Ctimensec: child.Ctimensec,
			Mode:      child.Mode,
			Nlink:     child.Nlink,
			Rdev:      child.Rdev,
			Blksize:   child.Blksize,
			Padding:   child.Padding,
			Uid:       child.Uid,
			Gid:       child.Gid,
			Gen:       child.Gen,
		})
	}
	return children, nil
}
