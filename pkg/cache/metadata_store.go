package cache

import (
	"context"

	redis "github.com/redis/go-redis/v9"
)

type CacheMetadataStore interface {
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

type RedisCacheMetadataStore struct {
	metadata *Metadata
}

func NewRedisCacheMetadataStore(_ GlobalConfig, serverConfig ServerConfig) (CacheMetadataStore, error) {
	metadata, err := NewMetadata(serverConfig.Metadata)
	if err != nil {
		return nil, err
	}

	return &RedisCacheMetadataStore{metadata: metadata}, nil
}

func NewRedisCacheMetadataStoreWithClient(_ GlobalConfig, _ ServerConfig, client redis.UniversalClient) CacheMetadataStore {
	return &RedisCacheMetadataStore{
		metadata: NewMetadataWithRedisClient(client),
	}
}

func (c *RedisCacheMetadataStore) AddHostToIndex(ctx context.Context, locality string, host *Host) error {
	return c.metadata.AddHostToIndex(ctx, locality, host)
}

func (c *RedisCacheMetadataStore) SetHostKeepAlive(ctx context.Context, locality string, host *Host) error {
	return c.metadata.SetHostKeepAlive(ctx, locality, host)
}

func (c *RedisCacheMetadataStore) RemoveHost(ctx context.Context, locality string, host *Host) error {
	if err := c.metadata.RemoveHostFromIndex(ctx, locality, host); err != nil {
		return err
	}
	return c.metadata.RemoveHostKeepAlive(ctx, locality, host)
}

func (c *RedisCacheMetadataStore) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return c.metadata.SetStoreFromContentLock(ctx, locality, sourcePath)
}

func (c *RedisCacheMetadataStore) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return c.metadata.RemoveStoreFromContentLock(ctx, locality, sourcePath)
}

func (c *RedisCacheMetadataStore) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return c.metadata.RefreshStoreFromContentLock(ctx, locality, sourcePath)
}

func (c *RedisCacheMetadataStore) GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error) {
	return c.metadata.GetAvailableHosts(ctx, locality, func(host *Host) {
		c.metadata.RemoveHostFromIndex(ctx, locality, host)
	})
}

func (c *RedisCacheMetadataStore) SetClientLock(ctx context.Context, hash string, host string) error {
	return c.metadata.SetClientLock(ctx, host, hash)
}

func (c *RedisCacheMetadataStore) RemoveClientLock(ctx context.Context, hash string, host string) error {
	return c.metadata.RemoveClientLock(ctx, host, hash)
}

func (c *RedisCacheMetadataStore) SetFsNode(ctx context.Context, id string, metadata *FSMetadata) error {
	return c.metadata.SetFsNode(ctx, id, metadata)
}

func (c *RedisCacheMetadataStore) GetFsNode(ctx context.Context, id string) (*FSMetadata, error) {
	return c.metadata.GetFsNode(ctx, id)
}

func (c *RedisCacheMetadataStore) AddFsNodeChild(ctx context.Context, pid, id string) error {
	return c.metadata.AddFsNodeChild(ctx, pid, id)
}

func (c *RedisCacheMetadataStore) RemoveFsNode(ctx context.Context, id string) error {
	return c.metadata.RemoveFsNode(ctx, id)
}

func (c *RedisCacheMetadataStore) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
	return c.metadata.RemoveFsNodeChild(ctx, pid, id)
}

func (c *RedisCacheMetadataStore) GetFsNodeChildren(ctx context.Context, id string) ([]*FSMetadata, error) {
	return c.metadata.GetFsNodeChildren(ctx, id)
}
