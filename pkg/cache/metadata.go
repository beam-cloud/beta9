package cache

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	redis "github.com/redis/go-redis/v9"
)

const (
	storeFromContentLockTtlS = 30
)

type Metadata struct {
	rdb  *RedisClient
	lock *RedisLock
}

func NewMetadata(cfg MetadataConfig) (*Metadata, error) {
	redisMode := RedisModeSingle
	if cfg.RedisMode != "" {
		redisMode = cfg.RedisMode
	}

	rdb, err := NewRedisClient(RedisConfig{
		Addrs:              []string{cfg.RedisAddr},
		Mode:               redisMode,
		Password:           cfg.RedisPasswd,
		SentinelPassword:   cfg.RedisPasswd,
		EnableTLS:          cfg.RedisTLSEnabled,
		MasterName:         cfg.RedisMasterName,
		InsecureSkipVerify: cfg.RedisInsecureSkipVerify,
	})
	if err != nil {
		return nil, err
	}

	lock := NewRedisLock(rdb)
	return &Metadata{
		rdb:  rdb,
		lock: lock,
	}, nil
}

func NewMetadataWithRedisClient(client redis.UniversalClient) *Metadata {
	rdb := &RedisClient{UniversalClient: client}
	return &Metadata{
		rdb:  rdb,
		lock: NewRedisLock(rdb),
	}
}

func (m *Metadata) SetClientLock(ctx context.Context, clientId, hash string) error {
	err := m.lock.Acquire(ctx, MetadataKeys.MetadataClientLock(clientId, hash), RedisLockOptions{TtlS: 300, Retries: 0})
	if err != nil {
		return err
	}

	return nil
}

func (m *Metadata) RemoveClientLock(ctx context.Context, clientId, hash string) error {
	return m.lock.Release(MetadataKeys.MetadataClientLock(clientId, hash))
}

func (m *Metadata) GetFsNode(ctx context.Context, id string) (*FSMetadata, error) {
	key := MetadataKeys.MetadataFsNode(id)

	res, err := m.rdb.HGetAll(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, &ErrNodeNotFound{Id: id}
	}

	metadata := &FSMetadata{}
	if err = ToStruct(res, metadata); err != nil {
		return nil, fmt.Errorf("failed to deserialize cachefs node metadata <%v>: %v", key, err)
	}

	return metadata, nil
}

func (m *Metadata) SetFsNode(ctx context.Context, id string, metadata *FSMetadata) error {
	key := MetadataKeys.MetadataFsNode(id)

	// If metadata exists, increment inode generation #
	res, err := m.rdb.HGetAll(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	if len(res) > 0 {
		existingMetadata := &FSMetadata{}
		if err = ToStruct(res, existingMetadata); err != nil {
			return err
		}

		metadata.Gen = existingMetadata.Gen + 1
	}

	err = m.rdb.HSet(ctx, key, ToSlice(metadata)).Err()
	if err != nil {
		return fmt.Errorf("failed to set cachefs node metadata <%v>: %w", key, err)
	}

	return nil
}

func (m *Metadata) GetFsNodeChildren(ctx context.Context, id string) ([]*FSMetadata, error) {
	nodeIds, err := m.rdb.SMembers(ctx, MetadataKeys.MetadataFsNodeChildren(id)).Result()
	if err != nil {
		return nil, err
	}

	entries := []*FSMetadata{}
	for _, nodeId := range nodeIds {
		node, err := m.GetFsNode(ctx, nodeId)
		if err != nil {
			continue
		}

		entries = append(entries, node)
	}

	return entries, nil
}

func (m *Metadata) GetAvailableHosts(ctx context.Context, locality string, removeHostCallback func(host *Host)) ([]*Host, error) {
	hostIds, err := m.rdb.SMembers(ctx, MetadataKeys.MetadataHostIndex(locality)).Result()
	if err != nil {
		return nil, err
	}

	hosts := []*Host{}
	for _, hostId := range hostIds {
		hostBytes, err := m.rdb.Get(ctx, MetadataKeys.MetadataHostKeepAlive(locality, hostId)).Bytes()
		if err != nil {

			// If the keepalive key doesn't exist, remove the host index key
			if err == redis.Nil {
				m.RemoveHostFromIndex(ctx, locality, &Host{HostId: hostId})
				removeHostCallback(&Host{HostId: hostId})
			}

			continue
		}

		host := &Host{}
		if err = json.Unmarshal(hostBytes, host); err != nil {
			continue
		}

		hosts = append(hosts, host)
	}

	if len(hosts) == 0 {
		return nil, errors.New("no available hosts")
	}

	return hosts, nil
}

func (m *Metadata) AddHostToIndex(ctx context.Context, locality string, host *Host) error {
	return m.rdb.SAdd(ctx, MetadataKeys.MetadataHostIndex(locality), host.HostId).Err()
}

func (m *Metadata) RemoveHostFromIndex(ctx context.Context, locality string, host *Host) error {
	return m.rdb.SRem(ctx, MetadataKeys.MetadataHostIndex(locality), host.HostId).Err()
}

func (m *Metadata) RemoveHostKeepAlive(ctx context.Context, locality string, host *Host) error {
	return m.rdb.Del(ctx, MetadataKeys.MetadataHostKeepAlive(locality, host.HostId)).Err()
}

func (m *Metadata) SetHostKeepAlive(ctx context.Context, locality string, host *Host) error {
	hostBytes, err := json.Marshal(host)
	if err != nil {
		return err
	}

	return m.rdb.Set(ctx, MetadataKeys.MetadataHostKeepAlive(locality, host.HostId), hostBytes, time.Duration(defaultHostKeepAliveTimeoutS)*time.Second).Err()
}

func (m *Metadata) GetHostIndex(ctx context.Context, locality string) ([]*Host, error) {
	hostIds, err := m.rdb.SMembers(ctx, MetadataKeys.MetadataHostIndex(locality)).Result()
	if err != nil {
		return nil, err
	}

	hosts := []*Host{}
	for _, hostId := range hostIds {
		hosts = append(hosts, &Host{HostId: hostId})
	}

	return hosts, nil
}

func (m *Metadata) AddFsNodeChild(ctx context.Context, pid, id string) error {
	err := m.rdb.SAdd(ctx, MetadataKeys.MetadataFsNodeChildren(pid), id).Err()
	if err != nil {
		return err
	}
	return nil
}

func (m *Metadata) RemoveFsNode(ctx context.Context, id string) error {
	return m.rdb.Del(ctx, MetadataKeys.MetadataFsNode(id)).Err()
}

func (m *Metadata) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
	return m.rdb.SRem(ctx, MetadataKeys.MetadataFsNodeChildren(pid), id).Err()
}

// RecentStub identifies a stub recently used in a locality, tracked so cache
// servers can discover required-content streams to reconcile.
type RecentStub struct {
	WorkspaceID string
	StubID      string
	LastSeen    time.Time
}

// AddRecentStub records a (workspace, stub) as recently used in a locality and
// prunes entries older than ttl. The index is a ZSET scored by last-seen time.
func (m *Metadata) AddRecentStub(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	if workspaceID == "" || stubID == "" {
		return nil
	}
	key := MetadataKeys.MetadataReconcileRecent(locality)
	allKey := MetadataKeys.MetadataReconcileRecentAll()
	now := time.Now()
	member := RecentStubKey(workspaceID, stubID)

	pipe := m.rdb.TxPipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now.UnixNano()), Member: member})
	pipe.ZAdd(ctx, allKey, redis.Z{Score: float64(now.UnixNano()), Member: member})
	if ttl > 0 {
		cutoff := now.Add(-ttl).UnixNano()
		pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", cutoff))
		pipe.ZRemRangeByScore(ctx, allKey, "-inf", fmt.Sprintf("%d", cutoff))
		pipe.Expire(ctx, key, ttl)
		pipe.Expire(ctx, allKey, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ListRecentStubs returns recently used stubs in a locality, newest first,
// excluding entries older than ttl.
func (m *Metadata) ListRecentStubs(ctx context.Context, locality string, ttl time.Duration, limit int) ([]RecentStub, error) {
	key := MetadataKeys.MetadataReconcileRecent(locality)
	return m.listRecentStubsFromKey(ctx, key, ttl, limit)
}

func (m *Metadata) listRecentStubsFromKey(ctx context.Context, key string, ttl time.Duration, limit int) ([]RecentStub, error) {
	min := "-inf"
	if ttl > 0 {
		min = fmt.Sprintf("%d", time.Now().Add(-ttl).UnixNano())
	}

	rangeBy := &redis.ZRangeBy{Min: min, Max: "+inf"}
	if limit > 0 {
		rangeBy.Count = int64(limit)
	}
	results, err := m.rdb.ZRevRangeByScoreWithScores(ctx, key, rangeBy).Result()
	if err != nil {
		return nil, err
	}

	stubs := make([]RecentStub, 0, len(results))
	for _, z := range results {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		workspaceID, stubID, ok := parseRecentStubMember(member)
		if !ok {
			continue
		}
		stubs = append(stubs, RecentStub{
			WorkspaceID: workspaceID,
			StubID:      stubID,
			LastSeen:    time.Unix(0, int64(z.Score)),
		})
	}
	return stubs, nil
}

// ListRecentStubsAnyLocality returns recently used stubs across all localities
// from the global recent-stub index, newest first.
func (m *Metadata) ListRecentStubsAnyLocality(ctx context.Context, ttl time.Duration) ([]RecentStub, error) {
	return m.listRecentStubsFromKey(ctx, MetadataKeys.MetadataReconcileRecentAll(), ttl, 0)
}

// MarkStubReported atomically claims the one-time required-content generation
// for a stub. It returns true only for the first caller (cluster-wide), so the
// expensive enumeration + S2 write happens once per stub rather than on every
// container start. The marker is set once with the given TTL and is not
// refreshed by later callers; once it expires the content may be regenerated,
// which is idempotent.
func (m *Metadata) MarkStubReported(ctx context.Context, locality, stubID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return m.rdb.SetNX(ctx, MetadataKeys.MetadataReconcileReported(locality, stubID), "1", ttl).Result()
}

// AcquireReconcileLock takes a short lifecycle lock so only one materialization
// runs for a (locality, logical host, hash) at a time. It returns whether the
// lock was acquired; contention is not an error.
func (m *Metadata) AcquireReconcileLock(ctx context.Context, locality, logicalHost, hash string, ttlS int) (bool, error) {
	err := m.lock.Acquire(ctx, MetadataKeys.MetadataReconcileLock(locality, logicalHost, hash), RedisLockOptions{TtlS: ttlS, Retries: 0})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, redislock.ErrNotObtained) {
		return false, nil
	}
	return false, err
}

func (m *Metadata) RefreshReconcileLock(locality, logicalHost, hash string, ttlS int) error {
	return m.lock.Refresh(MetadataKeys.MetadataReconcileLock(locality, logicalHost, hash), RedisLockOptions{TtlS: ttlS, Retries: 0})
}

func (m *Metadata) ReleaseReconcileLock(locality, logicalHost, hash string) error {
	return m.lock.Release(MetadataKeys.MetadataReconcileLock(locality, logicalHost, hash))
}

func RecentStubKey(workspaceID, stubID string) string {
	return workspaceID + "|" + stubID
}

func parseRecentStubMember(member string) (string, string, bool) {
	for i := 0; i < len(member); i++ {
		if member[i] == '|' {
			return member[:i], member[i+1:], true
		}
	}
	return "", "", false
}

func (m *Metadata) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return m.lock.Acquire(ctx, MetadataKeys.MetadataStoreFromContentLock(locality, sourcePath), RedisLockOptions{TtlS: storeFromContentLockTtlS, Retries: 0})
}

func (m *Metadata) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return m.lock.Refresh(MetadataKeys.MetadataStoreFromContentLock(locality, sourcePath), RedisLockOptions{TtlS: storeFromContentLockTtlS, Retries: 0})
}

func (m *Metadata) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	return m.lock.Release(MetadataKeys.MetadataStoreFromContentLock(locality, sourcePath))
}

// Metadata key storage format
var (
	metadataPrefix               string = "cache"
	metadataHostIndex            string = "cache:host_index:%s"
	metadataClientLock           string = "cache:client_lock:%s:%s"
	metadataLocation             string = "cache:location:%s"
	metadataFsNode               string = "cache:fs:node:%s"
	metadataFsNodeChildren       string = "cache:fs:node:%s:children"
	metadataHostKeepAlive        string = "cache:host:keepalive:%s:%s"
	metadataStoreFromContentLock string = "cache:store_from_content_lock:%s:%s"
	metadataReconcileRecent      string = "cache:reconcile:recent:%s"
	metadataReconcileRecentAll   string = "cache:reconcile:recent"
	metadataReconcileLock        string = "cache:reconcile:lock:%s:%s:%s"
	metadataReconcileReported    string = "cache:reconcile:reported:%s:%s"
)

// Metadata keys
func (k *metadataKeys) MetadataPrefix() string {
	return metadataPrefix
}

func (k *metadataKeys) MetadataHostIndex(locality string) string {
	return fmt.Sprintf(metadataHostIndex, locality)
}

func (k *metadataKeys) MetadataHostKeepAlive(locality, hostId string) string {
	return fmt.Sprintf(metadataHostKeepAlive, locality, hostId)
}

func (k *metadataKeys) MetadataLocation(hash string) string {
	return fmt.Sprintf(metadataLocation, hash)
}

func (k *metadataKeys) MetadataFsNode(id string) string {
	return fmt.Sprintf(metadataFsNode, id)
}

func (k *metadataKeys) MetadataFsNodeChildren(id string) string {
	return fmt.Sprintf(metadataFsNodeChildren, id)
}

func (k *metadataKeys) MetadataClientLock(hostId, hash string) string {
	return fmt.Sprintf(metadataClientLock, hostId, hash)
}

func (k *metadataKeys) MetadataStoreFromContentLock(locality, sourcePath string) string {
	encodedPath := base64.RawURLEncoding.EncodeToString([]byte(sourcePath))
	return fmt.Sprintf(metadataStoreFromContentLock, locality, encodedPath)
}

func (k *metadataKeys) MetadataReconcileRecent(locality string) string {
	return fmt.Sprintf(metadataReconcileRecent, locality)
}

func (k *metadataKeys) MetadataReconcileRecentAll() string {
	return metadataReconcileRecentAll
}

func (k *metadataKeys) MetadataReconcileLock(locality, logicalHost, hash string) string {
	return fmt.Sprintf(metadataReconcileLock, locality, logicalHost, hash)
}

func (k *metadataKeys) MetadataReconcileReported(locality, stubID string) string {
	return fmt.Sprintf(metadataReconcileReported, locality, stubID)
}

var MetadataKeys = &metadataKeys{}

type metadataKeys struct{}
