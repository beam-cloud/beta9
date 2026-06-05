package cache

import (
	"context"
	"time"

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

	// Required-content reconciliation state. On workers these are brokered to
	// the gateway/coordinator over gRPC; the gateway implements them against
	// its Redis.
	AddRecentStub(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error
	ListRecentStubs(ctx context.Context, locality string, ttl time.Duration, limit int) ([]RecentStub, error)
	MarkStubReported(ctx context.Context, locality, stubID string, ttl time.Duration) (bool, error)
	AcquireReconcileLock(ctx context.Context, locality, logicalHost, hash string, ttlSeconds int) (bool, error)
	ReleaseReconcileLock(ctx context.Context, locality, logicalHost, hash string) error
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

func (c *RedisCacheMetadataStore) AddRecentStub(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	return c.metadata.AddRecentStub(ctx, locality, workspaceID, stubID, ttl)
}

func (c *RedisCacheMetadataStore) ListRecentStubs(ctx context.Context, locality string, ttl time.Duration, limit int) ([]RecentStub, error) {
	return c.metadata.ListRecentStubs(ctx, locality, ttl, limit)
}

func (c *RedisCacheMetadataStore) ListRecentStubsAnyLocality(ctx context.Context, ttl time.Duration) ([]RecentStub, error) {
	return c.metadata.ListRecentStubsAnyLocality(ctx, ttl)
}

func (c *RedisCacheMetadataStore) MarkStubReported(ctx context.Context, locality, stubID string, ttl time.Duration) (bool, error) {
	return c.metadata.MarkStubReported(ctx, locality, stubID, ttl)
}

func (c *RedisCacheMetadataStore) AcquireReconcileLock(ctx context.Context, locality, logicalHost, hash string, ttlSeconds int) (bool, error) {
	return c.metadata.AcquireReconcileLock(ctx, locality, logicalHost, hash, ttlSeconds)
}

func (c *RedisCacheMetadataStore) ReleaseReconcileLock(_ context.Context, locality, logicalHost, hash string) error {
	return c.metadata.ReleaseReconcileLock(locality, logicalHost, hash)
}
