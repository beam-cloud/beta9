package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/redis/go-redis/v9"
)

const cacheCoordinatorKeyPrefix = "cache:coordinator"

type CacheRedisRepository struct {
	rdb *common.RedisClient
}

type cacheCoordinatorHostRecord struct {
	LogicalHostID    string  `json:"logical_host_id"`
	RegistrationID   string  `json:"registration_id"`
	PoolName         string  `json:"pool_name"`
	Locality         string  `json:"locality"`
	NodeID           string  `json:"node_id"`
	CachePathID      string  `json:"cache_path_id"`
	Addr             string  `json:"addr"`
	PrivateAddr      string  `json:"private_addr"`
	CapacityUsagePct float64 `json:"capacity_usage_pct"`
}

func NewCacheRedisRepository(rdb *common.RedisClient) cache.CoordinatorRepository {
	return &CacheRedisRepository{rdb: rdb}
}

func (r *CacheRedisRepository) SetCacheRegistration(ctx context.Context, host cache.CoordinatorHost, ttl time.Duration) error {
	payload, err := json.Marshal(cacheCoordinatorHostRecord(host))
	if err != nil {
		return err
	}
	logicalPayload, err := json.Marshal(cacheCoordinatorHostRecord(host.LogicalOnly()))
	if err != nil {
		return err
	}

	if err := r.rdb.SAdd(ctx, cacheCoordinatorIndexKey(host.PoolName, host.Locality), host.LogicalHostID).Err(); err != nil {
		return err
	}
	if err := r.rdb.SAdd(ctx, cacheCoordinatorLocalityIndexKey(host.Locality), host.LogicalHostID).Err(); err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, cacheCoordinatorLogicalHostKey(host.LogicalHostID), logicalPayload, cacheCoordinatorLogicalHostTTL(ttl)).Err(); err != nil {
		return err
	}
	if err := r.rdb.SAdd(ctx, cacheCoordinatorRegistrationSetKey(host.LogicalHostID), host.RegistrationID).Err(); err != nil {
		return err
	}
	return r.rdb.Set(ctx, cacheCoordinatorRegistrationKey(host.LogicalHostID, host.RegistrationID), payload, ttl).Err()
}

func (r *CacheRedisRepository) GetActiveCacheRegistration(ctx context.Context, logicalHostID string) (string, bool, error) {
	registrationID, err := r.rdb.Get(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return registrationID, registrationID != "", nil
}

func (r *CacheRedisRepository) SetActiveCacheRegistration(ctx context.Context, logicalHostID, registrationID string, ttl time.Duration) error {
	return r.rdb.Set(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID), registrationID, ttl).Err()
}

func (r *CacheRedisRepository) ListCacheLogicalHosts(ctx context.Context, poolName, locality string) ([]string, error) {
	if poolName == "" {
		return r.listCacheLogicalHostsFromIndex(ctx, cacheCoordinatorLocalityIndexKey(locality))
	}
	return r.listCacheLogicalHostsFromIndex(ctx, cacheCoordinatorIndexKey(poolName, locality))
}

func (r *CacheRedisRepository) listCacheLogicalHostsFromIndex(ctx context.Context, key string) ([]string, error) {
	ids, err := r.rdb.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	filtered := ids[:0]
	for _, id := range ids {
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	sort.Strings(filtered)
	return filtered, nil
}

func (r *CacheRedisRepository) ListCacheRegistrations(ctx context.Context, logicalHostID string) ([]string, error) {
	return r.rdb.SMembers(ctx, cacheCoordinatorRegistrationSetKey(logicalHostID)).Result()
}

func (r *CacheRedisRepository) GetCacheRegistration(ctx context.Context, logicalHostID, registrationID string) (cache.CoordinatorHost, bool, error) {
	payload, err := r.rdb.Get(ctx, cacheCoordinatorRegistrationKey(logicalHostID, registrationID)).Bytes()
	if err == redis.Nil {
		return cache.CoordinatorHost{}, false, nil
	}
	if err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	record := cacheCoordinatorHostRecord{}
	if err := json.Unmarshal(payload, &record); err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	return cache.CoordinatorHost(record), true, nil
}

func (r *CacheRedisRepository) GetCacheLogicalHost(ctx context.Context, logicalHostID string) (cache.CoordinatorHost, bool, error) {
	payload, err := r.rdb.Get(ctx, cacheCoordinatorLogicalHostKey(logicalHostID)).Bytes()
	if err == redis.Nil {
		return cache.CoordinatorHost{}, false, nil
	}
	if err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	record := cacheCoordinatorHostRecord{}
	if err := json.Unmarshal(payload, &record); err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	return cache.CoordinatorHost(record).LogicalOnly(), true, nil
}

func (r *CacheRedisRepository) RemoveCacheRegistration(ctx context.Context, logicalHostID, registrationID string) error {
	if err := r.rdb.Del(ctx, cacheCoordinatorRegistrationKey(logicalHostID, registrationID)).Err(); err != nil {
		return err
	}
	if err := r.rdb.SRem(ctx, cacheCoordinatorRegistrationSetKey(logicalHostID), registrationID).Err(); err != nil {
		return err
	}

	activeRegistrationID, found, err := r.GetActiveCacheRegistration(ctx, logicalHostID)
	if err != nil || !found || activeRegistrationID != registrationID {
		return err
	}
	return r.rdb.Del(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID)).Err()
}

func (r *CacheRedisRepository) CountCacheRegistrations(ctx context.Context, logicalHostID string) (int64, error) {
	return r.rdb.SCard(ctx, cacheCoordinatorRegistrationSetKey(logicalHostID)).Result()
}

func (r *CacheRedisRepository) RemoveCacheLogicalHost(ctx context.Context, poolName, locality, logicalHostID string) error {
	if poolName != "" {
		if err := r.rdb.SRem(ctx, cacheCoordinatorIndexKey(poolName, locality), logicalHostID).Err(); err != nil {
			return err
		}
	}
	if err := r.rdb.SRem(ctx, cacheCoordinatorLocalityIndexKey(locality), logicalHostID).Err(); err != nil {
		return err
	}
	return r.rdb.Del(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID), cacheCoordinatorRegistrationSetKey(logicalHostID), cacheCoordinatorLogicalHostKey(logicalHostID)).Err()
}

func cacheCoordinatorIndexKey(poolName, locality string) string {
	return fmt.Sprintf("%s:host_index:%s:%s", cacheCoordinatorKeyPrefix, poolName, locality)
}

func cacheCoordinatorLocalityIndexKey(locality string) string {
	return fmt.Sprintf("%s:host_index_by_locality:%s", cacheCoordinatorKeyPrefix, locality)
}

func cacheCoordinatorRegistrationSetKey(logicalHostID string) string {
	return fmt.Sprintf("%s:host:%s:registrations", cacheCoordinatorKeyPrefix, logicalHostID)
}

func cacheCoordinatorLogicalHostKey(logicalHostID string) string {
	return fmt.Sprintf("%s:host:%s:logical", cacheCoordinatorKeyPrefix, logicalHostID)
}

func cacheCoordinatorRegistrationKey(logicalHostID, registrationID string) string {
	return fmt.Sprintf("%s:host:%s:registration:%s", cacheCoordinatorKeyPrefix, logicalHostID, registrationID)
}

func cacheCoordinatorActiveRegistrationKey(logicalHostID string) string {
	return fmt.Sprintf("%s:host:%s:active_registration", cacheCoordinatorKeyPrefix, logicalHostID)
}

func cacheCoordinatorLogicalHostTTL(registrationTTL time.Duration) time.Duration {
	ttl := registrationTTL * 4
	if ttl < 2*time.Minute {
		return 2 * time.Minute
	}
	return ttl
}
