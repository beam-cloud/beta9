package cache

import (
	"context"

	redis "github.com/redis/go-redis/v9"
)

type Registry interface {
	AddHostToIndex(ctx context.Context, locality string, host *Host) error
	SetHostKeepAlive(ctx context.Context, locality string, host *Host) error
	GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error)
	GetRegionConfig(ctx context.Context, locality string) (ServerConfig, error)
	SetClientLock(ctx context.Context, hash string, host string) error
	RemoveClientLock(ctx context.Context, hash string, host string) error
	SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error
	RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error
	RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error
	SetFsNode(ctx context.Context, id string, metadata *FSMetadata) error
	GetFsNode(ctx context.Context, id string) (*FSMetadata, error)
	RemoveFsNode(ctx context.Context, id string) error
	RemoveFsNodeChild(ctx context.Context, pid, id string) error
	GetFsNodeChildren(ctx context.Context, id string) ([]*FSMetadata, error)
	AddFsNodeChild(ctx context.Context, pid, id string) error
}

type RedisRegistry struct {
	host         string
	globalConfig GlobalConfig
	serverConfig ServerConfig
	metadata     *Metadata
}

func NewRedisRegistry(globalConfig GlobalConfig, serverConfig ServerConfig) (Registry, error) {
	metadata, err := NewMetadata(serverConfig.Metadata)
	if err != nil {
		return nil, err
	}

	return &RedisRegistry{globalConfig: globalConfig, serverConfig: serverConfig, metadata: metadata}, nil
}

func NewRedisRegistryWithClient(globalConfig GlobalConfig, serverConfig ServerConfig, client redis.UniversalClient) Registry {
	return &RedisRegistry{
		globalConfig: globalConfig,
		serverConfig: serverConfig,
		metadata:     NewMetadataWithRedisClient(client),
	}
}

func (c *RedisRegistry) GetRegionConfig(ctx context.Context, locality string) (ServerConfig, error) {
	return c.serverConfig, nil
}

func (c *RedisRegistry) AddHostToIndex(ctx context.Context, locality string, host *Host) error {
	return c.metadata.AddHostToIndex(ctx, locality, host)
}

func (c *RedisRegistry) SetHostKeepAlive(ctx context.Context, locality string, host *Host) error {
	return c.metadata.SetHostKeepAlive(ctx, locality, host)
}

func (c *RedisRegistry) RemoveHost(ctx context.Context, locality string, host *Host) error {
	if err := c.metadata.RemoveHostFromIndex(ctx, locality, host); err != nil {
		return err
	}
	return c.metadata.RemoveHostKeepAlive(ctx, locality, host)
}

func (c *RedisRegistry) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return c.metadata.SetStoreFromContentLock(ctx, locality, sourcePath)
}

func (c *RedisRegistry) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return c.metadata.RemoveStoreFromContentLock(ctx, locality, sourcePath)
}

func (c *RedisRegistry) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return c.metadata.RefreshStoreFromContentLock(ctx, locality, sourcePath)
}

func (c *RedisRegistry) GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error) {
	return c.metadata.GetAvailableHosts(ctx, locality, func(host *Host) {
		c.metadata.RemoveHostFromIndex(ctx, locality, host)
	})
}

func (c *RedisRegistry) SetClientLock(ctx context.Context, hash string, host string) error {
	return c.metadata.SetClientLock(ctx, host, hash)
}

func (c *RedisRegistry) RemoveClientLock(ctx context.Context, hash string, host string) error {
	return c.metadata.RemoveClientLock(ctx, host, hash)
}

func (c *RedisRegistry) SetFsNode(ctx context.Context, id string, metadata *FSMetadata) error {
	return c.metadata.SetFsNode(ctx, id, metadata)
}

func (c *RedisRegistry) GetFsNode(ctx context.Context, id string) (*FSMetadata, error) {
	return c.metadata.GetFsNode(ctx, id)
}

func (c *RedisRegistry) AddFsNodeChild(ctx context.Context, pid, id string) error {
	return c.metadata.AddFsNodeChild(ctx, pid, id)
}

func (c *RedisRegistry) RemoveFsNode(ctx context.Context, id string) error {
	return c.metadata.RemoveFsNode(ctx, id)
}

func (c *RedisRegistry) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
	return c.metadata.RemoveFsNodeChild(ctx, pid, id)
}

func (c *RedisRegistry) GetFsNodeChildren(ctx context.Context, id string) ([]*FSMetadata, error) {
	return c.metadata.GetFsNodeChildren(ctx, id)
}
